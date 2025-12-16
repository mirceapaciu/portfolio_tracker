#!/usr/bin/env python3
"""Convert comdirect open positions export to a normalized CSV file."""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.parse import parse_german_decimal

ENTRY_HEADER = "Wert:"
DATE_IN_LINE_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")
BROKER_NAME = "comdirect"
SHARE_LINE_RE = re.compile(r"^\s*([\d\.\s]+)\s+(.+?)\s+<", re.IGNORECASE)
CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
PRICE_TOKEN_RE = re.compile(r"^[\d\.,]+%?$")
FIELDNAMES = ["broker", "security_name", "shares", "share_price", "amount", "date"]


@dataclass
class PositionState:
    """Holds intermediate parsing state for a single position."""

    security_name: Optional[str] = None
    shares_decimal: Optional[Decimal] = None
    share_price_decimal: Optional[Decimal] = None
    share_price_decimals: Optional[int] = None
    amount_decimal: Optional[Decimal] = None
    amount_decimals: Optional[int] = None
    date: Optional[str] = None


def count_decimal_places(value: str) -> int:
    cleaned = value.strip().rstrip("%")
    if "," in cleaned:
        return len(cleaned.split(",", maxsplit=1)[1])
    return 0


def decimal_places_from_decimal(value: Decimal) -> int:
    return max(-value.as_tuple().exponent, 0)


def decimal_to_german(value: Decimal, decimals: int) -> str:
    decimals = max(decimals, 0)
    quant = Decimal("1") if decimals == 0 else Decimal(f"1.{'0' * decimals}")
    normalized = value.quantize(quant, rounding=ROUND_HALF_UP)
    return format(normalized, "f").replace(".", ",")


def format_shares(value: Decimal) -> str:
    integral_candidate = value.to_integral_value()
    if value == integral_candidate:
        return str(int(integral_candidate))
    decimals = decimal_places_from_decimal(value)
    return decimal_to_german(value, decimals)


def extract_share_line(line: str) -> Optional[tuple[str, str]]:
    match = SHARE_LINE_RE.match(line)
    if not match:
        return None
    shares_raw = re.sub(r"\s+", "", match.group(1))
    security_name = match.group(2).strip()
    return shares_raw, " ".join(security_name.split())


def extract_price(line: str) -> Optional[tuple[Decimal, int]]:
    collapsed = " ".join(line.replace("\t", " ").split())
    if not collapsed:
        return None
    tokens = collapsed.split(" ")
    if len(tokens) < 5:
        return None
    if not (CURRENCY_RE.fullmatch(tokens[0]) and CURRENCY_RE.fullmatch(tokens[-2])):
        return None
    candidate = tokens[-1]
    if not PRICE_TOKEN_RE.fullmatch(candidate):
        return None
    numeric = candidate.rstrip("%")
    decimals = count_decimal_places(numeric)
    value = parse_german_decimal(numeric)
    if value is None:
        return None
    return value, decimals


def finalize_state(state: PositionState) -> Optional[dict[str, str]]:
    if not state.security_name or not state.date:
        return None
    if state.shares_decimal is None or state.shares_decimal == 0:
        return None
    if state.amount_decimal is None:
        return None
    share_price_decimal = state.share_price_decimal
    share_price_decimals = state.share_price_decimals
    if share_price_decimal is None:
        share_price_decimal = (state.amount_decimal / state.shares_decimal).quantize(
            Decimal("0.0001"), rounding=ROUND_HALF_UP
        )
        share_price_decimals = max(decimal_places_from_decimal(share_price_decimal), 4)
    amount_decimals = state.amount_decimals if state.amount_decimals is not None else 2
    share_price_decimals = share_price_decimals if share_price_decimals is not None else 2
    return {
        "broker": BROKER_NAME,
        "security_name": state.security_name,
        "shares": format_shares(state.shares_decimal),
        "share_price": decimal_to_german(share_price_decimal, share_price_decimals),
        "amount": decimal_to_german(state.amount_decimal, amount_decimals),
        "date": state.date,
    }


def parse_open_positions(lines: Iterable[str]) -> List[dict[str, str]]:
    entries: List[dict[str, str]] = []
    state: Optional[PositionState] = None
    expect_amount = False
    awaiting_price = False

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        if line == ENTRY_HEADER:
            if state:
                finalized = finalize_state(state)
                if finalized:
                    entries.append(finalized)
            state = PositionState()
            expect_amount = True
            awaiting_price = False
            continue

        if expect_amount and state:
            amount_decimal = parse_german_decimal(line)
            if amount_decimal is not None:
                state.amount_decimal = amount_decimal
                state.amount_decimals = count_decimal_places(line)
            expect_amount = False
            continue

        if state is None:
            continue

        if state.shares_decimal is None and "<http" in raw_line.lower():
            extracted = extract_share_line(raw_line)
            if extracted:
                shares_str, security_name = extracted
                shares_decimal = parse_german_decimal(shares_str)
                if shares_decimal is not None:
                    state.shares_decimal = shares_decimal
                    state.security_name = security_name
                    awaiting_price = True
                continue

        if awaiting_price and state.share_price_decimal is None:
            price_data = extract_price(line)
            if price_data:
                state.share_price_decimal, state.share_price_decimals = price_data
                awaiting_price = False
                continue

        if state.date is None:
            date_match = DATE_IN_LINE_RE.search(line)
            if date_match:
                state.date = date_match.group(1)
                finalized = finalize_state(state)
                if finalized:
                    entries.append(finalized)
                state = None
                expect_amount = False
                awaiting_price = False
                continue

    if state:
        finalized = finalize_state(state)
        if finalized:
            entries.append(finalized)

    return entries


def write_csv(entries: List[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES, delimiter=";")
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="data/input/open_positions.txt",
        help="Path to the raw open positions TXT export",
    )
    parser.add_argument(
        "--output",
        default="data/loaded/open_positions.csv",
        help="Destination path for the normalized CSV",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    lines = input_path.read_text(encoding="utf-8").splitlines()
    entries = parse_open_positions(lines)
    if not entries:
        print("No positions detected; nothing to write.")
        return
    output_path = Path(args.output)
    write_csv(entries, output_path)
    print(f"Wrote {len(entries)} positions to {output_path}")


if __name__ == "__main__":
    main()
