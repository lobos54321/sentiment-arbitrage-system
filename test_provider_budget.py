import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from provider_budget import (  # noqa: E402
    clear_provider_budget_state,
    provider_request_allowed,
    provider_budget_snapshot,
    record_provider_result,
)


def test_provider_budget_exhausts_window_and_reports_remaining(monkeypatch):
    clear_provider_budget_state()
    monkeypatch.setenv("PROVIDER_BUDGET_DEXSCREENER_PER_MIN", "2")

    first = provider_request_allowed("dexscreener", now=1000.0)
    second = provider_request_allowed("dexscreener", now=1001.0)
    third = provider_request_allowed("dexscreener", now=1002.0)

    assert first["pass"] is True
    assert second["pass"] is True
    assert third["pass"] is False
    assert third["reason"] == "provider_budget_exhausted"


def test_provider_budget_rate_limit_opens_cooldown():
    clear_provider_budget_state()

    allowed = provider_request_allowed("helius", now=2000.0)
    snapshot = record_provider_result("helius", success=False, rate_limited=True, now=2000.0)
    blocked = provider_request_allowed("helius", now=2001.0)

    assert allowed["pass"] is True
    assert snapshot["providers"]["helius"]["rate_limited_n"] == 1
    assert blocked["pass"] is False
    assert blocked["reason"] == "provider_budget_cooldown"
    assert provider_budget_snapshot("helius", now=2001.0)["providers"]["helius"]["cooldown_remaining_sec"] > 0
