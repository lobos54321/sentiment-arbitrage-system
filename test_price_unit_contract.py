import os
from pathlib import Path
import re
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from paper_trade_monitor import (  # noqa: E402
    AMOUNT_UNIT_SOL,
    AMOUNT_UNIT_TOKEN,
    PNL_UNIT_RATIO_DECIMAL,
    PRICE_UNIT_SOL_PER_TOKEN,
    SOL_MINT,
    build_execution_audit,
    price_unit_contract_payload,
    sanitize_monitor_state,
)


def test_buy_execution_audit_labels_sol_per_token_units():
    audit = build_execution_audit({
        "side": "buy",
        "success": True,
        "inputMint": SOL_MINT,
        "outputMint": "TokenMint",
        "tokenCA": "TokenMint",
        "inputAmount": 0.05,
        "quotedOutAmount": 1000.0,
        "effectivePrice": 0.00005,
    })

    assert audit["effectivePriceUnit"] == PRICE_UNIT_SOL_PER_TOKEN
    assert audit["inputAmountUnit"] == AMOUNT_UNIT_SOL
    assert audit["quotedOutAmountUnit"] == AMOUNT_UNIT_TOKEN
    assert audit["accountingUnit"] == AMOUNT_UNIT_SOL
    assert audit["pnlUnit"] == PNL_UNIT_RATIO_DECIMAL


def test_sell_execution_audit_labels_sol_per_token_units():
    audit = build_execution_audit({
        "side": "sell",
        "success": True,
        "inputMint": "TokenMint",
        "outputMint": SOL_MINT,
        "tokenCA": "TokenMint",
        "inputAmount": 1000.0,
        "quotedOutAmount": 0.06,
        "effectivePrice": 0.00006,
    })

    assert audit["effectivePriceUnit"] == PRICE_UNIT_SOL_PER_TOKEN
    assert audit["inputAmountUnit"] == AMOUNT_UNIT_TOKEN
    assert audit["quotedOutAmountUnit"] == AMOUNT_UNIT_SOL


def test_monitor_state_carries_entry_unit_contract():
    state = sanitize_monitor_state(
        {},
        token_ca="TokenMint",
        symbol="DOG",
        entry_price=0.00005,
        entry_ts=1000,
        position_size_sol=0.05,
        token_amount_raw=1000000000,
        token_decimals=6,
    )

    assert state["entryPriceUnit"] == PRICE_UNIT_SOL_PER_TOKEN
    assert state["entryQuotePriceUnit"] == PRICE_UNIT_SOL_PER_TOKEN
    assert state["entryTriggerPriceUnit"] == PRICE_UNIT_SOL_PER_TOKEN
    assert state["accountingUnit"] == AMOUNT_UNIT_SOL
    assert state["pnlUnit"] == PNL_UNIT_RATIO_DECIMAL


def test_price_unit_contract_payload_can_mark_synthetic_close():
    payload = price_unit_contract_payload(exitPriceUnit="SYNTHETIC_CLOSE_NO_PRICE")

    assert payload["entryPriceUnit"] == PRICE_UNIT_SOL_PER_TOKEN
    assert payload["exitPriceUnit"] == "SYNTHETIC_CLOSE_NO_PRICE"
    assert payload["stopUnit"] == PNL_UNIT_RATIO_DECIMAL
    assert payload["trailUnit"] == PNL_UNIT_RATIO_DECIMAL


def test_live_paper_trade_insert_shape_matches_columns():
    source = Path(__file__).resolve().parent / "scripts" / "paper_trade_monitor.py"
    text = source.read_text()
    insert_blocks = re.finditer(
        r"INSERT INTO paper_trades\s*\((.*?)\)\s*VALUES\s*\((.*?)\)",
        text,
        re.S,
    )

    for block in insert_blocks:
        columns_sql = block.group(1)
        values_sql = block.group(2)
        if "entry_execution_json" not in columns_sql:
            continue

        columns = [part.strip() for part in columns_sql.split(",")]
        values = [part.strip() for part in values_sql.split(",")]
        assert len(values) == len(columns)
        return

    raise AssertionError("live paper_trades insert block not found")
