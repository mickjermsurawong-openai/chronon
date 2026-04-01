import json
import os
from unittest.mock import Mock, patch

import pytest
import requests
from click.testing import CliRunner

from ai.chronon.repo.auth import (
    _load_auth_data,
    auth,
    clear_auth_config,
    get_auth_config,
    get_frontend_url_from_teams,
    get_user_email,
    save_auth_config,
)


@pytest.fixture
def auth_dir(tmp_path):
    """Use a temporary directory for auth config."""
    auth_file = tmp_path / "auth.json"
    with (
        patch("ai.chronon.repo.auth.AUTH_DIR", tmp_path),
        patch("ai.chronon.repo.auth.AUTH_FILE", auth_file),
    ):
        yield tmp_path, auth_file


@pytest.fixture
def sample_config():
    return {
        "url": "https://hub.example.com",
        "access_token": "test-session-token",
        "token_type": "Bearer",
        "name": "Test User",
        "email": "test@example.com",
    }


class TestAuthConfig:
    """Tests for auth config persistence (save/load/clear)."""

    def test_get_auth_config_returns_none_when_missing(self, auth_dir):
        assert get_auth_config() is None

    def test_save_and_load_config(self, auth_dir, sample_config):
        _, auth_file = auth_dir
        save_auth_config(sample_config)

        loaded = get_auth_config()
        assert loaded == sample_config
        # Verify file permissions are restrictive
        assert oct(auth_file.stat().st_mode & 0o777) == "0o600"

    def test_clear_auth_config(self, auth_dir, sample_config):
        _, auth_file = auth_dir
        save_auth_config(sample_config)
        assert auth_file.exists()

        clear_auth_config()
        assert not auth_file.exists()

    def test_clear_auth_config_noop_when_missing(self, auth_dir):
        # Should not raise
        clear_auth_config()

    def test_save_multiple_accounts(self, auth_dir):
        """Saving configs for different urls creates separate accounts."""
        _, auth_file = auth_dir
        config_a = {"url": "https://hub-a.example.com", "access_token": "tok-a"}
        config_b = {"url": "https://hub-b.example.com", "access_token": "tok-b"}

        save_auth_config(config_a)
        save_auth_config(config_b)

        data = _load_auth_data()
        assert len(data["accounts"]) == 2
        assert data["accounts"]["https://hub-a.example.com"]["access_token"] == "tok-a"
        assert data["accounts"]["https://hub-b.example.com"]["access_token"] == "tok-b"
        # Default is the most recently saved
        assert data["default"] == "https://hub-b.example.com"

    def test_get_auth_config_by_url(self, auth_dir):
        """get_auth_config with a specific url returns that account."""
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        config = get_auth_config(url="https://hub-a.example.com")
        assert config["access_token"] == "tok-a"

    def test_get_auth_config_returns_default(self, auth_dir):
        """get_auth_config without url returns the default account."""
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        config = get_auth_config()
        assert config["access_token"] == "tok-b"

    def test_get_auth_config_returns_none_for_unknown_url(self, auth_dir, sample_config):
        """When the specific URL doesn't match, return None (strict matching)."""
        save_auth_config(sample_config)
        assert get_auth_config(url="https://unknown.example.com") is None

    def test_get_auth_config_returns_none_for_unknown_url_with_multiple_accounts(self, auth_dir):
        """With multiple accounts, unknown URL returns None (no fallback to default)."""
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        assert get_auth_config(url="https://unknown.example.com") is None

    def test_get_auth_config_exact_match_over_default(self, auth_dir):
        """Exact URL match takes priority over the default account."""
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})
        # Default is hub-b, but we ask for hub-a
        config = get_auth_config(url="https://hub-a.example.com")
        assert config["access_token"] == "tok-a"

    def test_get_auth_config_normalizes_trailing_slash(self, auth_dir, sample_config):
        """Trailing slash on the lookup URL is ignored."""
        save_auth_config(sample_config)
        config = get_auth_config(url="https://hub.example.com/")
        assert config == sample_config

    def test_save_auth_config_overwrites_existing(self, auth_dir):
        """Re-login to the same URL updates the token."""
        save_auth_config({"url": "https://hub.example.com", "access_token": "old-tok"})
        save_auth_config({"url": "https://hub.example.com", "access_token": "new-tok"})

        data = _load_auth_data()
        assert len(data["accounts"]) == 1
        assert data["accounts"]["https://hub.example.com"]["access_token"] == "new-tok"

    def test_clear_specific_account(self, auth_dir):
        """clear_auth_config with a url removes only that account."""
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        clear_auth_config(url="https://hub-a.example.com")

        data = _load_auth_data()
        assert "https://hub-a.example.com" not in data["accounts"]
        assert "https://hub-b.example.com" in data["accounts"]

    def test_clear_default_account_updates_default(self, auth_dir):
        """Clearing the default account sets default to another account."""
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        clear_auth_config(url="https://hub-b.example.com")

        data = _load_auth_data()
        assert data["default"] == "https://hub-a.example.com"

    def test_clear_last_account_removes_file(self, auth_dir, sample_config):
        """Clearing the only account removes the file entirely."""
        _, auth_file = auth_dir
        save_auth_config(sample_config)
        clear_auth_config(url=sample_config["url"])
        assert not auth_file.exists()

    def test_migrates_old_flat_format(self, auth_dir):
        """Old flat auth.json format is migrated on read."""
        _, auth_file = auth_dir
        old_config = {
            "url": "https://old.example.com",
            "access_token": "old-token",
            "token_type": "Bearer",
        }
        auth_file.parent.mkdir(parents=True, exist_ok=True)
        auth_file.write_text(json.dumps(old_config))

        config = get_auth_config()
        assert config["url"] == "https://old.example.com"
        assert config["access_token"] == "old-token"

        # File should now be in new format
        data = json.loads(auth_file.read_text())
        assert "accounts" in data
        assert data["default"] == "https://old.example.com"


class TestGetUserEmail:
    """Tests for get_user_email() priority: hub auth > git > USER env."""

    def test_returns_hub_auth_email(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        assert get_user_email() == "test@example.com"

    def test_falls_back_to_git_email(self, auth_dir):
        with patch("ai.chronon.cli.git_utils.subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.stdout = "git@example.com\n"
            mock_run.return_value = mock_result
            assert get_user_email() == "git@example.com"

    def test_falls_back_to_env_user(self, auth_dir):
        with (
            patch("ai.chronon.cli.git_utils.subprocess.run", side_effect=Exception("no git")),
            patch.dict(os.environ, {"USER": "envuser"}),
        ):
            assert get_user_email() == "envuser"

    def test_skips_hub_config_without_email(self, auth_dir):
        save_auth_config({"url": "https://hub.example.com", "access_token": "tok"})
        with patch("ai.chronon.cli.git_utils.subprocess.run") as mock_run:
            mock_result = Mock()
            mock_result.stdout = "git@example.com\n"
            mock_run.return_value = mock_result
            assert get_user_email() == "git@example.com"

    def test_returns_email_for_specific_url(self, auth_dir):
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "t", "email": "a@example.com"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "t", "email": "b@example.com"})
        assert get_user_email(url="https://hub-a.example.com") == "a@example.com"


class TestLoginCommand:
    """Tests for `zipline auth login`."""

    def test_successful_login_saves_config(self, auth_dir):
        _, auth_file = auth_dir
        runner = CliRunner()

        device_code_resp = Mock()
        device_code_resp.raise_for_status = Mock()
        device_code_resp.json.return_value = {
            "device_code": "dev123",
            "user_code": "ABCD-1234",
            "verification_uri": "https://hub.example.com/auth/device",
            "interval": 0,
            "expires_in": 10,
        }

        token_resp = Mock()
        token_resp.json.return_value = {
            "access_token": "session-tok-123",
            "token_type": "Bearer",
        }

        session_resp = Mock()
        session_resp.ok = True
        session_resp.json.return_value = {
            "user": {"name": "Jane Doe", "email": "jane@example.com"}
        }

        with (
            patch("ai.chronon.repo.auth.requests.post", return_value=device_code_resp) as mock_post,
            patch("ai.chronon.repo.auth.requests.get", return_value=session_resp),
            patch("ai.chronon.repo.auth.webbrowser.open"),
            patch("ai.chronon.repo.auth.time.sleep"),
            patch("ai.chronon.repo.auth.time.time", side_effect=[0, 1]),
        ):
            # After device code request, switch post to return token
            mock_post.side_effect = [device_code_resp, token_resp]

            result = runner.invoke(auth, ["login", "--url", "https://hub.example.com", "--no-browser"])

        assert result.exit_code == 0
        assert "Authentication successful" in result.output

        data = json.loads(auth_file.read_text())
        saved = data["accounts"]["https://hub.example.com"]
        assert saved["url"] == "https://hub.example.com"
        assert saved["access_token"] == "session-tok-123"
        assert saved["email"] == "jane@example.com"
        assert saved["name"] == "Jane Doe"
        assert data["default"] == "https://hub.example.com"

    def test_login_device_code_request_failure(self, auth_dir):
        runner = CliRunner()

        with patch(
            "ai.chronon.repo.auth.requests.post",
            side_effect=requests.ConnectionError("connection refused"),
        ):
            result = runner.invoke(auth, ["login", "--url", "https://hub.example.com"])

        assert result.exit_code != 0
        assert "Failed to request device code" in result.output

    def test_login_access_denied(self, auth_dir):
        runner = CliRunner()

        device_code_resp = Mock()
        device_code_resp.raise_for_status = Mock()
        device_code_resp.json.return_value = {
            "device_code": "dev123",
            "user_code": "ABCD-1234",
            "interval": 0,
            "expires_in": 10,
        }

        denied_resp = Mock()
        denied_resp.json.return_value = {"error": "access_denied"}

        with (
            patch("ai.chronon.repo.auth.requests.post", side_effect=[device_code_resp, denied_resp]),
            patch("ai.chronon.repo.auth.webbrowser.open"),
            patch("ai.chronon.repo.auth.time.sleep"),
            patch("ai.chronon.repo.auth.time.time", side_effect=[0, 1]),
        ):
            result = runner.invoke(auth, ["login", "--url", "https://hub.example.com", "--no-browser"])

        assert result.exit_code != 0
        assert "denied" in result.output

    def test_login_second_hub_preserves_first(self, auth_dir):
        """Logging into a second hub doesn't remove the first."""
        _, auth_file = auth_dir
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})

        runner = CliRunner()

        device_code_resp = Mock()
        device_code_resp.raise_for_status = Mock()
        device_code_resp.json.return_value = {
            "device_code": "dev123",
            "user_code": "ABCD-1234",
            "interval": 0,
            "expires_in": 10,
        }

        token_resp = Mock()
        token_resp.json.return_value = {"access_token": "tok-b", "token_type": "Bearer"}

        session_resp = Mock()
        session_resp.ok = True
        session_resp.json.return_value = {"user": {"name": "User B", "email": "b@example.com"}}

        with (
            patch("ai.chronon.repo.auth.requests.post", side_effect=[device_code_resp, token_resp]),
            patch("ai.chronon.repo.auth.requests.get", return_value=session_resp),
            patch("ai.chronon.repo.auth.webbrowser.open"),
            patch("ai.chronon.repo.auth.time.sleep"),
            patch("ai.chronon.repo.auth.time.time", side_effect=[0, 1]),
        ):
            result = runner.invoke(auth, ["login", "--url", "https://hub-b.example.com", "--no-browser"])

        assert result.exit_code == 0
        data = json.loads(auth_file.read_text())
        assert len(data["accounts"]) == 2
        assert "https://hub-a.example.com" in data["accounts"]
        assert "https://hub-b.example.com" in data["accounts"]
        assert data["default"] == "https://hub-b.example.com"


class TestLogoutCommand:
    """Tests for `zipline auth logout`."""

    def test_logout_clears_all_config(self, auth_dir, sample_config):
        _, auth_file = auth_dir
        save_auth_config(sample_config)
        assert auth_file.exists()

        runner = CliRunner()
        result = runner.invoke(auth, ["logout"])

        assert result.exit_code == 0
        assert "Logged out of all accounts" in result.output
        assert not auth_file.exists()

    def test_logout_when_not_authenticated(self, auth_dir):
        runner = CliRunner()
        result = runner.invoke(auth, ["logout"])
        assert result.exit_code == 0

    def test_logout_specific_hub(self, auth_dir):
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        runner = CliRunner()
        result = runner.invoke(auth, ["logout", "--url", "https://hub-a.example.com"])

        assert result.exit_code == 0
        assert "hub-a.example.com" in result.output
        # hub-b should still exist
        config = get_auth_config(url="https://hub-b.example.com")
        assert config is not None


class TestStatusCommand:
    """Tests for `zipline auth status`."""

    def test_status_not_authenticated(self, auth_dir):
        runner = CliRunner()
        result = runner.invoke(auth, ["status"])

        assert result.exit_code == 0
        assert "Not authenticated" in result.output

    def test_status_authenticated(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        runner = CliRunner()

        session_resp = Mock()
        session_resp.ok = True
        session_resp.json.return_value = {
            "user": {"name": "Test User", "email": "test@example.com"}
        }

        with patch("ai.chronon.repo.auth.requests.get", return_value=session_resp):
            result = runner.invoke(auth, ["status"])

        assert result.exit_code == 0
        assert "hub.example.com" in result.output
        assert "authenticated" in result.output

    def test_status_expired_token(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        runner = CliRunner()

        expired_resp = Mock()
        expired_resp.ok = False
        expired_resp.status_code = 401

        with patch("ai.chronon.repo.auth.requests.get", return_value=expired_resp):
            result = runner.invoke(auth, ["status"])

        assert result.exit_code == 0
        assert "expired or invalid" in result.output

    def test_status_network_error(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        runner = CliRunner()

        with patch(
            "ai.chronon.repo.auth.requests.get",
            side_effect=requests.ConnectionError("no network"),
        ):
            result = runner.invoke(auth, ["status"])

        assert result.exit_code == 0
        assert "network error" in result.output

    def test_status_shows_default_label(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        runner = CliRunner()

        session_resp = Mock()
        session_resp.ok = True
        session_resp.json.return_value = {"user": {"name": "Test", "email": "t@e.com"}}

        with patch("ai.chronon.repo.auth.requests.get", return_value=session_resp):
            result = runner.invoke(auth, ["status"])

        assert "(default)" in result.output

    def test_status_specific_hub(self, auth_dir):
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        runner = CliRunner()

        session_resp = Mock()
        session_resp.ok = True
        session_resp.json.return_value = {"user": {"name": "User A", "email": "a@e.com"}}

        with patch("ai.chronon.repo.auth.requests.get", return_value=session_resp):
            result = runner.invoke(auth, ["status", "--url", "https://hub-a.example.com"])

        assert result.exit_code == 0
        assert "hub-a.example.com" in result.output

    def test_status_multiple_accounts(self, auth_dir):
        """Shows all accounts with separator between them, default labeled."""
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        runner = CliRunner()

        session_resp = Mock()
        session_resp.ok = True
        session_resp.json.return_value = {"user": {"name": "User", "email": "u@e.com"}}

        with patch("ai.chronon.repo.auth.requests.get", return_value=session_resp):
            result = runner.invoke(auth, ["status"])

        assert result.exit_code == 0
        assert "hub-a.example.com" in result.output
        assert "hub-b.example.com" in result.output
        # hub-b is default (last saved), hub-a is not
        assert "hub-b.example.com (default)" in result.output
        assert "hub-a.example.com (default)" not in result.output

    def test_status_unknown_hub(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        runner = CliRunner()

        result = runner.invoke(auth, ["status", "--url", "https://unknown.example.com"])
        assert "No account found" in result.output


class TestGetAccessTokenCommand:
    """Tests for `zipline auth get-access-token`."""

    def test_prints_jwt(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        runner = CliRunner()

        token_resp = Mock()
        token_resp.ok = True
        token_resp.json.return_value = {"token": "eyJhbGciOi.payload.signature"}

        with patch("ai.chronon.repo.auth.requests.get", return_value=token_resp):
            result = runner.invoke(auth, ["get-access-token"])

        assert result.exit_code == 0
        assert result.output.strip() == "eyJhbGciOi.payload.signature"

    def test_not_authenticated(self, auth_dir):
        runner = CliRunner()
        result = runner.invoke(auth, ["get-access-token"])

        assert result.exit_code != 0
        assert "Not authenticated" in result.output

    def test_expired_session(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        runner = CliRunner()

        expired_resp = Mock()
        expired_resp.ok = False
        expired_resp.status_code = 401

        with patch("ai.chronon.repo.auth.requests.get", return_value=expired_resp):
            result = runner.invoke(auth, ["get-access-token"])

        assert result.exit_code != 0
        assert "expired or invalid" in result.output

    def test_network_error(self, auth_dir, sample_config):
        save_auth_config(sample_config)
        runner = CliRunner()

        with patch(
            "ai.chronon.repo.auth.requests.get",
            side_effect=requests.ConnectionError("connection refused"),
        ):
            result = runner.invoke(auth, ["get-access-token"])

        assert result.exit_code != 0
        assert "Failed to fetch access token" in result.output

    def test_get_access_token_for_specific_url(self, auth_dir):
        save_auth_config({"url": "https://hub-a.example.com", "access_token": "tok-a"})
        save_auth_config({"url": "https://hub-b.example.com", "access_token": "tok-b"})

        runner = CliRunner()

        token_resp = Mock()
        token_resp.ok = True
        token_resp.json.return_value = {"token": "jwt-for-a"}

        with patch("ai.chronon.repo.auth.requests.get", return_value=token_resp) as mock_get:
            result = runner.invoke(auth, ["get-access-token", "--url", "https://hub-a.example.com"])

        assert result.exit_code == 0
        assert result.output.strip() == "jwt-for-a"
        # Verify it called hub-a, not hub-b
        mock_get.assert_called_once()
        call_url = mock_get.call_args[0][0]
        assert "hub-a.example.com" in call_url


class TestGetFrontendUrlFromTeams:
    """Tests for get_frontend_url_from_teams()."""

    def test_finds_frontend_url_in_cwd(self, tmp_path):
        teams_py = tmp_path / "teams.py"
        teams_py.write_text(
            "from gen_thrift.api.ttypes import Team\n"
            "from ai.chronon.types import EnvironmentVariables\n"
            'default = Team(env=EnvironmentVariables(common={"FRONTEND_URL": "https://zipline.example.com"}))\n'
        )
        with patch("ai.chronon.repo.auth.Path.cwd", return_value=tmp_path):
            assert get_frontend_url_from_teams() == "https://zipline.example.com"

    def test_finds_frontend_url_in_parent(self, tmp_path):
        teams_py = tmp_path / "teams.py"
        teams_py.write_text(
            "from gen_thrift.api.ttypes import Team\n"
            "from ai.chronon.types import EnvironmentVariables\n"
            'default = Team(env=EnvironmentVariables(common={"FRONTEND_URL": "https://parent.example.com"}))\n'
        )
        child = tmp_path / "subdir"
        child.mkdir()
        with patch("ai.chronon.repo.auth.Path.cwd", return_value=child):
            assert get_frontend_url_from_teams() == "https://parent.example.com"

    def test_returns_none_when_no_teams_py(self, tmp_path):
        with patch("ai.chronon.repo.auth.Path.cwd", return_value=tmp_path):
            assert get_frontend_url_from_teams() is None

    def test_returns_none_when_no_frontend_url(self, tmp_path):
        teams_py = tmp_path / "teams.py"
        teams_py.write_text(
            "from gen_thrift.api.ttypes import Team\n"
            "from ai.chronon.types import EnvironmentVariables\n"
            'default = Team(env=EnvironmentVariables(common={"HUB_URL": "https://hub.example.com"}))\n'
        )
        with patch("ai.chronon.repo.auth.Path.cwd", return_value=tmp_path):
            assert get_frontend_url_from_teams() is None


class TestLoginDefaultUrl:
    """Tests for login --url defaulting to FRONTEND_URL from teams.py."""

    def test_login_uses_frontend_url_as_prompt_default(self, auth_dir):
        """When --url is not set, the prompt default comes from teams.py FRONTEND_URL."""
        _, auth_file = auth_dir
        runner = CliRunner()

        device_code_resp = Mock()
        device_code_resp.raise_for_status = Mock()
        device_code_resp.json.return_value = {
            "device_code": "dev123",
            "user_code": "ABCD-1234",
            "verification_uri": "https://zipline.example.com/auth/device",
            "interval": 0,
            "expires_in": 10,
        }

        token_resp = Mock()
        token_resp.json.return_value = {
            "access_token": "session-tok-123",
            "token_type": "Bearer",
        }

        session_resp = Mock()
        session_resp.ok = True
        session_resp.json.return_value = {
            "user": {"name": "Jane Doe", "email": "jane@example.com"}
        }

        with (
            patch("ai.chronon.repo.auth.get_frontend_url_from_teams", return_value="https://zipline.example.com"),
            patch("ai.chronon.repo.auth.requests.post", side_effect=[device_code_resp, token_resp]),
            patch("ai.chronon.repo.auth.requests.get", return_value=session_resp),
            patch("ai.chronon.repo.auth.webbrowser.open"),
            patch("ai.chronon.repo.auth.time.sleep"),
            patch("ai.chronon.repo.auth.time.time", side_effect=[0, 1]),
        ):
            # Press enter to accept the default
            result = runner.invoke(auth, ["login", "--no-browser"], input="\n")

        assert result.exit_code == 0
        assert "zipline.example.com" in result.output  # default shown in prompt
        assert "Authentication successful" in result.output
        data = json.loads(auth_file.read_text())
        assert data["accounts"]["https://zipline.example.com"]["url"] == "https://zipline.example.com"

    def test_login_prompts_without_default_when_no_teams(self, auth_dir):
        """When no teams.py FRONTEND_URL is found, prompt has no default."""
        runner = CliRunner()

        with patch("ai.chronon.repo.auth.get_frontend_url_from_teams", return_value=None):
            result = runner.invoke(auth, ["login", "--no-browser"], input="https://prompted.example.com\n")

        assert "Zipline Auth URL" in result.output
