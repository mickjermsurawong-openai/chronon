"""
Authentication commands for the Zipline CLI.

Provides `zipline auth login`, `zipline auth logout`, and `zipline auth status`
using device authorization flow (RFC 8628).
"""

import json
import sys
import time
import webbrowser
from pathlib import Path

import click
import requests

from ai.chronon.cli.theme import (
    print_error,
    print_info,
    print_key_value,
    print_success,
    print_warning,
)

AUTH_DIR = Path.home() / ".zipline"
AUTH_FILE = AUTH_DIR / "auth.json"


def get_frontend_url_from_teams():
    """Walk up from CWD to find teams.py and extract FRONTEND_URL from the default team."""
    from ai.chronon.cli.compile.parse_teams import import_module_from_file

    current = Path.cwd()
    for directory in [current, *current.parents]:
        teams_file = directory / "teams.py"
        if teams_file.exists():
            try:
                from gen_thrift.api.ttypes import Team

                module = import_module_from_file(str(teams_file))
                default_team = getattr(module, "default", None)
                if isinstance(default_team, Team) and default_team.env and default_team.env.common:
                    url = default_team.env.common.get("FRONTEND_URL")
                    if url:
                        return url
            except Exception:
                pass
            break
    return None


def _load_auth_data():
    """Load the raw auth data from disk, migrating old format if needed."""
    if not AUTH_FILE.exists():
        return None
    with open(AUTH_FILE) as f:
        data = json.load(f)
    # Migrate old flat format (single account) to new multi-account format
    if "accounts" not in data and "url" in data:
        account_url = data["url"]
        migrated = {"accounts": {account_url: data}, "default": account_url}
        _save_auth_data(migrated)
        return migrated
    return data


def _save_auth_data(data: dict):
    """Write the full auth data structure to disk."""
    AUTH_DIR.mkdir(parents=True, exist_ok=True)
    with open(AUTH_FILE, "w") as f:
        json.dump(data, f, indent=2)
    AUTH_FILE.chmod(0o600)


def get_auth_config(url=None):
    """Load persisted auth config for a specific URL, or the default.

    Returns the account dict for the given url, or the default account if
    url is None. Returns None if no matching account exists.
    """
    data = _load_auth_data()
    if not data or not data.get("accounts"):
        return None
    accounts = data["accounts"]
    if url:
        # Normalize trailing slash for lookup
        key = url.rstrip("/")
        return accounts.get(key)
    # Fall back to default when no specific URL requested
    default_url = data.get("default")
    if default_url and default_url in accounts:
        return accounts[default_url]
    # If no default set, return the single account if there's only one
    if len(accounts) == 1:
        return next(iter(accounts.values()))
    return None


def save_auth_config(config: dict):
    """Persist auth config for a hub. The url in config is used as the key."""
    account_url = config["url"]
    data = _load_auth_data() or {"accounts": {}}
    data["accounts"][account_url] = config
    data["default"] = account_url
    _save_auth_data(data)


def clear_auth_config(url=None):
    """Remove persisted auth config. If url is given, remove only that account."""
    if url:
        data = _load_auth_data()
        if data and data.get("accounts"):
            key = url.rstrip("/")
            data["accounts"].pop(key, None)
            if data.get("default") == key:
                # Set default to another account if available, else clear
                remaining = list(data["accounts"].keys())
                data["default"] = remaining[0] if remaining else None
            if data["accounts"]:
                _save_auth_data(data)
            elif AUTH_FILE.exists():
                AUTH_FILE.unlink()
    else:
        if AUTH_FILE.exists():
            AUTH_FILE.unlink()


def get_user_email(url=None) -> str:
    """Get the user email, preferring hub auth over git config.

    Priority: hub auth email > git config email > USER env var.
    """
    config = get_auth_config(url=url)
    if config and config.get("email"):
        return config["email"]

    from ai.chronon.cli.git_utils import get_git_user_email

    return get_git_user_email()


@click.group(help="Manage authentication for Zipline.")
def auth():
    pass


@auth.command()
@click.option("--url", default=None, help="Zipline Auth URL (e.g. https://zipline.example.com). Defaults to FRONTEND_URL from teams.py.")
@click.option("--no-browser", is_flag=True, help="Don't automatically open the browser")
def login(url, no_browser):
    """Authenticate with Zipline via browser-based device flow."""
    if url is None:
        default_url = get_frontend_url_from_teams()
        url = click.prompt("Zipline Auth URL", default=default_url or None)
    base_url = url.rstrip("/")

    # Step 1: Request device code
    print_info("Requesting device authorization...")
    try:
        resp = requests.post(
            f"{base_url}/api/auth/device/code",
            json={"client_id": "zipline-cli"},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print_error(f"Failed to request device code: {e}")
        sys.exit(1)

    device_code = data["device_code"]
    user_code = data["user_code"]
    verification_uri = data.get("verification_uri", f"{base_url}/auth/device")
    verification_uri_complete = data.get("verification_uri_complete")
    interval = data.get("interval", 5)
    expires_in = data.get("expires_in", 900)

    # Step 2: Display code and open browser
    click.echo()
    click.echo(f"  Your one-time code is: {click.style(user_code, bold=True)}")
    click.echo(f"  Visit: {verification_uri}")
    click.echo()

    url_to_open = verification_uri_complete or f"{verification_uri}?user_code={user_code}"
    if not no_browser:
        print_info("Opening browser...")
        webbrowser.open(url_to_open)

    # Step 3: Poll for token
    print_info(f"Waiting for authorization (expires in {expires_in}s)...")
    deadline = time.time() + expires_in
    poll_interval = interval

    while time.time() < deadline:
        time.sleep(poll_interval)
        try:
            token_resp = requests.post(
                f"{base_url}/api/auth/device/token",
                json={
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                    "device_code": device_code,
                    "client_id": "zipline-cli",
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            token_data = token_resp.json()
        except requests.RequestException:
            continue

        if "access_token" in token_data:
            # Success — save token and user info
            auth_config = {
                "url": base_url,
                "access_token": token_data["access_token"],
                "token_type": token_data.get("token_type", "Bearer"),
            }

            # Fetch user info to persist and display
            try:
                session_resp = requests.get(
                    f"{base_url}/api/auth/get-session",
                    headers={"Authorization": f"Bearer {token_data['access_token']}"},
                    timeout=10,
                )
                if session_resp.ok:
                    session_data = session_resp.json()
                    user = session_data.get("user", {})
                    name = user.get("name", "Unknown")
                    email = user.get("email", "")
                    auth_config["name"] = name
                    auth_config["email"] = email
            except requests.RequestException:
                pass

            save_auth_config(auth_config)
            print_success("Authentication successful!")
            if auth_config.get("email"):
                print_key_value("Logged in as", f"{auth_config['name']} ({auth_config['email']})")
            return

        error = token_data.get("error", "")
        if error == "authorization_pending":
            continue
        elif error == "slow_down":
            poll_interval += 5
        elif error == "access_denied":
            print_error("Authorization was denied.")
            sys.exit(1)
        elif error == "expired_token":
            print_error("Device code expired. Please try again.")
            sys.exit(1)
        else:
            print_error(f"Unexpected error: {token_data.get('error_description', error)}")
            sys.exit(1)

    print_error("Authorization timed out. Please try again.")
    sys.exit(1)


@auth.command()
@click.option("--url", default=None, help="Zipline Auth URL to log out of. If omitted, logs out of all accounts.")
def logout(url):
    """Remove stored Zipline credentials."""
    if url:
        clear_auth_config(url=url)
        print_success(f"Logged out of {url}.")
    else:
        clear_auth_config()
        print_success("Logged out of all accounts.")


@auth.command(name="get-access-token")
@click.option("--url", default=None, help="Zipline Auth URL to get token for. Defaults to the most recently authenticated account.")
def get_access_token(url):
    """Print a JWT access token to stdout (for use in scripts and curl)."""
    config = get_auth_config(url=url)
    if not config or not config.get("access_token"):
        print_error("Not authenticated. Run 'zipline auth login' first.")
        sys.exit(1)

    base_url = config["url"]
    session_token = config["access_token"]
    try:
        resp = requests.get(
            f"{base_url}/api/auth/token",
            headers={"Authorization": f"Bearer {session_token}"},
            timeout=10,
        )
        if resp.ok:
            token = resp.json().get("token")
            if token:
                click.echo(token)
                return
        print_error("Session expired or invalid. Run 'zipline auth login' to re-authenticate.")
        sys.exit(1)
    except requests.RequestException as e:
        print_error(f"Failed to fetch access token: {e}")
        sys.exit(1)


@auth.command()
@click.option("--url", default=None, help="Zipline Auth URL to check status for. If omitted, shows all accounts.")
def status(url):
    """Show current authentication status."""
    data = _load_auth_data()
    if not data or not data.get("accounts"):
        click.echo("Not authenticated. Run 'zipline auth login' to authenticate.")
        return

    accounts = data["accounts"]
    default_url = data.get("default")

    if url:
        key = url.rstrip("/")
        config = accounts.get(key)
        if not config:
            click.echo(f"No account found for {url}. Run 'zipline auth login --url {url}' to authenticate.")
            return
        _print_account_status(config, is_default=(key == default_url))
    else:
        account_list = list(accounts.items())
        for i, (account_url, config) in enumerate(account_list):
            _print_account_status(config, is_default=(account_url == default_url))
            if i < len(account_list) - 1:
                click.echo()


def _print_account_status(config, is_default=False):
    """Print status for a single account."""
    base_url = config.get("url", "unknown")
    label = f"{base_url} (default)" if is_default else base_url
    print_key_value("URL", label)
    print_key_value("Token", "present" if config.get("access_token") else "missing")

    token = config.get("access_token")
    if base_url and token:
        try:
            resp = requests.get(
                f"{base_url}/api/auth/get-session",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            if resp.ok:
                user = resp.json().get("user", {})
                name = user.get("name", "Unknown")
                email = user.get("email", "")
                print_key_value("User", f"{name} ({email})")
                print_key_value("Status", "authenticated")
            else:
                print_warning("Token expired or invalid. Run 'zipline auth login' to re-authenticate.")
        except requests.RequestException:
            print_warning("Unable to verify token (network error).")
