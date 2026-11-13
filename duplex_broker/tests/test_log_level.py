import pytest

from duplex_broker.__main__ import log_level


@pytest.mark.parametrize(
    ("value", "expected"),
    [("", "info"), ("  ", "info"), ("bogus", "info"), ("DEBUG", "debug"), ("warning", "warning")],
)
def test_an_empty_or_unknown_log_level_option_means_info(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: str
) -> None:
    monkeypatch.setenv("DUPLEX_LOG_LEVEL", value)
    assert log_level() == expected


def test_no_log_level_option_means_info(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DUPLEX_LOG_LEVEL", raising=False)
    assert log_level() == "info"
