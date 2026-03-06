from __future__ import annotations

import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import requests
from dotenv import load_dotenv
from tqdm import tqdm


ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
MONTH_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclass(frozen=True)
class ParsedReleaseDate:
    text: str
    year: int
    month: int
    day: int | None

    def sort_key(self) -> tuple[int, int, int]:
        return (self.year, self.month, self.day or 0)

    def is_post_aug_2025(self) -> bool:
        if self.year > 2025:
            return True
        if self.year < 2025:
            return False
        return self.month >= 8


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read CSV with a Product column, fetch product release dates with Claude "
            "web search, and write an augmented CSV."
        )
    )
    parser.add_argument("filepath", type=Path, help="Path to input CSV file")
    parser.add_argument(
        "--model",
        default=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
        help="Anthropic model to use (default from ANTHROPIC_MODEL env var)",
    )
    parser.add_argument(
        "--max-searches-per-product",
        type=int,
        default=3,
        help="Maximum Claude web searches allowed per product lookup",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=120,
        help="HTTP timeout for Anthropic API calls",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Maximum parallel Claude requests for unique products",
    )
    parser.add_argument(
        "--max-attempts-per-product",
        type=int,
        default=3,
        help="Total Claude attempts per product before giving up",
    )
    parser.add_argument(
        "--retry-base-delay-seconds",
        type=float,
        default=1.0,
        help="Base delay for retry backoff (seconds)",
    )
    parser.add_argument(
        "--retry-backoff-factor",
        type=float,
        default=2.0,
        help="Exponential backoff multiplier between attempts",
    )
    return parser.parse_args()


def split_products(raw_value: Any) -> list[str]:
    if raw_value is None:
        return []
    text = str(raw_value).strip()
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def parse_release_date(text: str) -> ParsedReleaseDate | None:
    cleaned = text.strip().replace(",", "")
    if not cleaned:
        return None

    day_match = re.fullmatch(r"([A-Za-z]+)\s+([0-9]{1,2})\s+([0-9]{4})", cleaned)
    if day_match:
        month_name, day_str, year_str = day_match.groups()
        month = MONTH_TO_NUMBER.get(month_name.lower())
        if month is None:
            return None
        day = int(day_str)
        if day < 1 or day > 31:
            return None
        return ParsedReleaseDate(
            text=f"{month_name} {day} {year_str}",
            year=int(year_str),
            month=month,
            day=day,
        )

    month_match = re.fullmatch(r"([A-Za-z]+)\s+([0-9]{4})", cleaned)
    if month_match:
        month_name, year_str = month_match.groups()
        month = MONTH_TO_NUMBER.get(month_name.lower())
        if month is None:
            return None
        return ParsedReleaseDate(
            text=f"{month_name} {year_str}",
            year=int(year_str),
            month=month,
            day=None,
        )

    return None


def extract_json_from_text(text: str) -> dict[str, Any] | None:
    text = text.strip()
    if not text:
        return None

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def call_claude_for_release_date(
    *,
    api_key: str,
    model: str,
    product_name: str,
    max_searches_per_product: int,
    timeout_seconds: int,
    max_attempts: int,
    retry_base_delay_seconds: float,
    retry_backoff_factor: float,
) -> ParsedReleaseDate | None:
    headers = {
        "x-api-key": api_key,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }
    anthropic_beta = os.getenv("ANTHROPIC_BETA")
    if anthropic_beta:
        headers["anthropic-beta"] = anthropic_beta

    max_attempts = max(1, max_attempts)
    retry_base_delay_seconds = max(0.0, retry_base_delay_seconds)
    retry_backoff_factor = max(1.0, retry_backoff_factor)

    for attempt in range(1, max_attempts + 1):
        retry_guidance = ""
        if attempt > 1:
            retry_guidance = (
                "\nAdditional guidance for retry:\n"
                "- Try alternate product naming (publisher/brand/edition abbreviations).\n"
                "- Cross-check two independent sources if possible.\n"
                "- If only month+year is available, return Month Year."
            )

        prompt = (
            "Find the latest release date for this product using web search.\n"
            f"Product: {product_name}\n\n"
            "Return ONLY valid JSON with this shape:\n"
            '{"release_date": "Month Day Year" | "Month Year" | "UNKNOWN"}\n'
            "Rules:\n"
            "- Prefer official vendor or publisher sources.\n"
            "- If day is unavailable, return Month Year.\n"
            "- No extra keys, no markdown, no prose."
            f"{retry_guidance}"
        )

        payload = {
            "model": model,
            "max_tokens": 300,
            "tools": [
                {
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "max_uses": max_searches_per_product,
                }
            ],
            "messages": [{"role": "user", "content": prompt}],
        }

        try:
            response = requests.post(
                ANTHROPIC_API_URL,
                headers=headers,
                json=payload,
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            if attempt == max_attempts:
                tqdm.write(
                    f'  Failed product "{product_name}" after {attempt} attempts due to request errors'
                )
                return None

            delay = retry_base_delay_seconds * (retry_backoff_factor ** (attempt - 1))
            tqdm.write(
                f'  Request error for "{product_name}" on attempt {attempt}/{max_attempts}; '
                f"retrying in {delay:.1f}s"
            )
            if delay > 0:
                time.sleep(delay)
            continue

        text_chunks: list[str] = []
        for block in data.get("content", []):
            if isinstance(block, dict) and block.get("type") == "text":
                block_text = block.get("text")
                if isinstance(block_text, str):
                    text_chunks.append(block_text)

        joined_text = "\n".join(text_chunks).strip()
        parsed = extract_json_from_text(joined_text)
        if parsed is not None:
            release_text = parsed.get("release_date")
            if isinstance(release_text, str) and release_text.strip().upper() != "UNKNOWN":
                parsed_date = parse_release_date(release_text)
                if parsed_date is not None:
                    return parsed_date
        else:
            parsed_date = parse_release_date(joined_text)
            if parsed_date is not None:
                return parsed_date

        if attempt == max_attempts:
            tqdm.write(f'  No parseable release date found for "{product_name}" after {attempt} attempts')
            return None

        delay = retry_base_delay_seconds * (retry_backoff_factor ** (attempt - 1))
        tqdm.write(
            f'  No valid release date for "{product_name}" on attempt {attempt}/{max_attempts}; '
            f"retrying in {delay:.1f}s"
        )
        if delay > 0:
            time.sleep(delay)

    return None


def output_path_for(input_path: Path) -> Path:
    if input_path.suffix:
        return input_path.with_name(f"{input_path.stem}-with-release-dates{input_path.suffix}")
    return input_path.with_name(f"{input_path.name}-with-release-dates.csv")


def main() -> None:
    load_dotenv()
    args = parse_args()

    input_path = args.filepath.expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("Missing ANTHROPIC_API_KEY environment variable.")

    df = pl.read_csv(input_path)
    if "Product" not in df.columns:
        raise ValueError('CSV must include a "Product" column.')

    cache: dict[str, ParsedReleaseDate | None] = {}
    release_date_values: list[str] = []
    post_aug_values: list[str] = []

    product_rows = df["Product"].to_list()
    split_product_rows = [split_products(raw_products) for raw_products in product_rows]
    unique_products: list[str] = []
    seen_products: set[str] = set()
    for products in split_product_rows:
        for product in products:
            if product not in seen_products:
                seen_products.add(product)
                unique_products.append(product)

    tqdm.write(f"Unique products to fetch: {len(unique_products)}")

    def fetch_product(product_name: str) -> tuple[str, ParsedReleaseDate | None]:
        return (
            product_name,
            call_claude_for_release_date(
                api_key=api_key,
                model=args.model,
                product_name=product_name,
                max_searches_per_product=args.max_searches_per_product,
                timeout_seconds=args.request_timeout_seconds,
                max_attempts=args.max_attempts_per_product,
                retry_base_delay_seconds=args.retry_base_delay_seconds,
                retry_backoff_factor=args.retry_backoff_factor,
            ),
        )

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        futures = [executor.submit(fetch_product, product) for product in unique_products]
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Fetching product release dates",
            unit="product",
        ):
            product_name, release = future.result()
            cache[product_name] = release

    for row_idx, products in enumerate(
        tqdm(split_product_rows, desc="Processing rows", unit="row"), start=1
    ):
        row_dates: list[ParsedReleaseDate] = []
        tqdm.write(f"[Row {row_idx}] products to process: {len(products)}")

        for product in products:
            release = cache.get(product)
            if release is not None:
                row_dates.append(release)

        if not row_dates:
            release_date_values.append("")
            post_aug_values.append("False")
            tqdm.write(f"[Row {row_idx}] no release date found")
            continue

        latest = max(row_dates, key=lambda item: item.sort_key())
        release_date_values.append(latest.text)
        post_aug_values.append("TRUE" if latest.is_post_aug_2025() else "False")
        tqdm.write(
            f"[Row {row_idx}] latest release date: {latest.text}; "
            f"Post-Aug-2025={post_aug_values[-1]}"
        )

    output_df = df.with_columns(
        pl.Series("Release date", release_date_values),
        pl.Series("Post-Aug-2025", post_aug_values),
    )

    output_path = output_path_for(input_path)
    output_df.write_csv(output_path)
    print(f"Wrote output CSV: {output_path}")


if __name__ == "__main__":
    main()
