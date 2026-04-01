import pytest

from ai.chronon.repo.utils import resolve_conf


def test_resolve_conf_exact_match(tmp_path):
    """Return conf unchanged when the exact file exists."""
    compiled = tmp_path / "compiled" / "joins" / "team"
    compiled.mkdir(parents=True)
    (compiled / "my_join.v1__1").write_text("{}")

    conf = "compiled/joins/team/my_join.v1__1"
    assert resolve_conf(str(tmp_path), conf) == conf


def test_resolve_conf_single_versioned_match(tmp_path):
    """Resolve to the single versioned file when exact path is missing."""
    compiled = tmp_path / "compiled" / "joins" / "team"
    compiled.mkdir(parents=True)
    (compiled / "my_join.v1__3").write_text("{}")

    conf = "compiled/joins/team/my_join.v1"
    resolved = resolve_conf(str(tmp_path), conf)
    assert resolved == "compiled/joins/team/my_join.v1__3"


def test_resolve_conf_ambiguous_raises(tmp_path):
    """Raise ValueError when multiple versioned files match."""
    compiled = tmp_path / "compiled" / "joins" / "team"
    compiled.mkdir(parents=True)
    (compiled / "my_join.v1__1").write_text("{}")
    (compiled / "my_join.v1__2").write_text("{}")

    with pytest.raises(ValueError, match="Ambiguous conf"):
        resolve_conf(str(tmp_path), "compiled/joins/team/my_join.v1")


def test_resolve_conf_no_match_returns_original(tmp_path):
    """Return conf unchanged when no versioned files match (let downstream error)."""
    compiled = tmp_path / "compiled" / "joins" / "team"
    compiled.mkdir(parents=True)
    (compiled / "other_join.v1__1").write_text("{}")

    conf = "compiled/joins/team/my_join.v1"
    assert resolve_conf(str(tmp_path), conf) == conf


def test_resolve_conf_no_directory_returns_original(tmp_path):
    """Return conf unchanged when the parent directory doesn't exist."""
    conf = "compiled/joins/team/my_join.v1"
    assert resolve_conf(str(tmp_path), conf) == conf


def test_resolve_conf_does_not_match_different_prefix(tmp_path):
    """Ensure prefix matching is exact — don't match a different config name."""
    compiled = tmp_path / "compiled" / "joins" / "team"
    compiled.mkdir(parents=True)
    (compiled / "my_join.v1_extra__1").write_text("{}")

    conf = "compiled/joins/team/my_join.v1"
    assert resolve_conf(str(tmp_path), conf) == conf


def test_resolve_conf_ignores_non_numeric_suffix(tmp_path):
    """Only match __<digits> suffixes, not arbitrary strings."""
    compiled = tmp_path / "compiled" / "joins" / "team"
    compiled.mkdir(parents=True)
    (compiled / "my_join.v1__backup").write_text("{}")
    (compiled / "my_join.v1__old").write_text("{}")

    conf = "compiled/joins/team/my_join.v1"
    assert resolve_conf(str(tmp_path), conf) == conf
