"""Clean scenario CSV rows before upload.

Usage:
    uv run python src/processing.py --csv path/to/scenarios.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

REQUIRED_FIELDS = ["Prompt", "Use Case", "Category", "Rubric Notes/URLs"]
ROW_INDEX_COL = "__row_idx"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read scenarios.csv, remove duplicate rows, and remove rows with "
            "missing required fields."
        )
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("scenarios.csv"),
        help="Path to scenarios.csv (default: scenarios.csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path (default: overwrite --csv path)",
    )
    return parser.parse_args()


def _validate_required_columns(df: pl.DataFrame) -> None:
    missing_cols = [col for col in REQUIRED_FIELDS if col not in df.columns]
    if missing_cols:
        missing_str = ", ".join(missing_cols)
        raise ValueError(f"Missing required column(s): {missing_str}")


def _print_removed_indices(label: str, indices: list[int]) -> None:
    indices_str = ", ".join(str(idx) for idx in indices) if indices else "none"
    print(f"{label} ({len(indices)}): {indices_str}")


def clean_scenarios(csv_path: Path, output_path: Path) -> None:
    df = pl.read_csv(csv_path).with_row_index(ROW_INDEX_COL)
    _validate_required_columns(df)

    duplicate_indices = (
        df.group_by("Prompt", maintain_order=True)
        .agg(pl.col(ROW_INDEX_COL))
        .select(pl.col(ROW_INDEX_COL).list.slice(1).alias("removed"))
        .explode("removed")
        .drop_nulls()
        .get_column("removed")
        .to_list()
    )

    deduped = df.unique(subset=["Prompt"], maintain_order=True)
    missing_expr = pl.any_horizontal(
        [
            pl.col(col).is_null()
            | pl.col(col).cast(pl.Utf8, strict=False).str.strip_chars().eq("")
            for col in REQUIRED_FIELDS
        ]
    )
    missing_required_indices = (
        deduped.filter(missing_expr).get_column(ROW_INDEX_COL).to_list()
    )

    cleaned = deduped.filter(~missing_expr).drop(ROW_INDEX_COL)
    cleaned.write_csv(output_path)

    _print_removed_indices("Removed duplicate row indices", duplicate_indices)
    _print_removed_indices(
        "Removed rows with missing required fields", missing_required_indices
    )
    print(f"Wrote cleaned CSV: {output_path}")


def main() -> None:
    args = parse_args()
    csv_path = args.csv
    output_path = args.output or csv_path
    clean_scenarios(csv_path=csv_path, output_path=output_path)


if __name__ == "__main__":
    main()
