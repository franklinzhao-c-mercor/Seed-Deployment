from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from pathlib import Path
from typing import Any

import polars as pl
import requests
from dotenv import load_dotenv
from tqdm import tqdm


AIRTABLE_API_ROOT = "https://api.airtable.com/v0"
AIRTABLE_CREATE_BATCH_SIZE = 10
REQUIRED_COLUMNS = [
    "Post-Aug-2025",
    "Release date",
    "Use case",
    "Category",
    "Prompt",
    "Rubric Notes & URLs",
    "Notes",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload CSV rows with Post-Aug-2025 == TRUE to Airtable and write an "
            "output CSV with Task ID values."
        )
    )
    parser.add_argument("filepath", type=Path, help="Path to input CSV file")
    parser.add_argument(
        "--team",
        type=int,
        required=True,
        choices=[1, 2, 3, 4],
        help="Team number used to resolve EMPORIUM_BASE_<team> from environment",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=60,
        help="HTTP timeout for Airtable requests",
    )
    parser.add_argument(
        "--validation-workers",
        type=int,
        default=8,
        help="Maximum parallel Airtable requests during validation",
    )
    return parser.parse_args()


def output_path_for(input_path: Path) -> Path:
    if input_path.suffix:
        return input_path.with_name(f"{input_path.stem}-uploaded{input_path.suffix}")
    return input_path.with_name(f"{input_path.name}-uploaded.csv")


def to_string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def chunked(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def record_signature(fields: dict[str, Any], field_names: list[str]) -> tuple[str, ...]:
    return tuple(to_string(fields.get(field_name)).strip() for field_name in field_names)


def validate_csv_columns(df: pl.DataFrame) -> None:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(
            "CSV schema validation failed. Missing required columns: "
            + ", ".join(missing_columns)
        )
    print("CSV schema validation passed.")


def airtable_request(
    *,
    method: str,
    url: str,
    token: str,
    timeout_seconds: int,
    params: dict[str, Any] | None = None,
    json_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.request(
        method=method,
        url=url,
        headers=headers,
        params=params,
        json=json_payload,
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected Airtable response type: {type(data)}")
    return data


def list_records_by_task_id(
    *,
    base_id: str,
    table_id: str,
    token: str,
    timeout_seconds: int,
    task_id: str,
) -> list[dict[str, Any]]:
    url = f"{AIRTABLE_API_ROOT}/{base_id}/{table_id}"
    escaped_task_id = task_id.replace("'", "\\'")
    formula = f"{{Task ID}}='{escaped_task_id}'"
    data = airtable_request(
        method="GET",
        url=url,
        token=token,
        timeout_seconds=timeout_seconds,
        params={"pageSize": 100, "filterByFormula": formula},
    )
    records = data.get("records", [])
    if not isinstance(records, list):
        raise RuntimeError("Airtable list response missing records array")
    return records


def main() -> None:
    load_dotenv()
    args = parse_args()

    input_path = args.filepath.expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {input_path}")

    base_id = to_string(os.getenv(f"EMPORIUM_BASE_{args.team}")).strip()
    table_id = to_string(os.getenv("EMPORIUM_TASKS_TABLE_ID")).strip()
    table_token = to_string(os.getenv("EMPORIUM_SEEDING_TOKEN")).strip()
    missing_env: list[str] = []
    if not base_id:
        missing_env.append(f"EMPORIUM_BASE_{args.team}")
    if not table_id:
        missing_env.append("EMPORIUM_TASKS_TABLE_ID")
    if not table_token:
        missing_env.append("EMPORIUM_SEEDING_TOKEN")
    if missing_env:
        raise RuntimeError(
            f"Missing required environment variable(s): {', '.join(missing_env)}"
        )

    df = pl.read_csv(input_path)
    validate_csv_columns(df)

    df_with_index = df.with_row_index("__row_idx")
    post_aug_col = pl.col("Post-Aug-2025").cast(pl.Utf8).fill_null("").str.strip_chars().str.to_uppercase()
    release_date_col = pl.col("Release date").cast(pl.Utf8).fill_null("").str.strip_chars()
    excluded_rows = (post_aug_col == "FALSE") & (release_date_col != "")
    filtered_df = df_with_index.filter(~excluded_rows)

    rows_to_upload = filtered_df.iter_rows(named=True)
    rows_to_upload_list = list(rows_to_upload)
    print(f"Rows selected for upload: {len(rows_to_upload_list)}")

    task_ids: list[str] = [""] * df.height
    expected_by_task_id: dict[str, dict[str, str]] = {}
    upload_warnings: list[str] = []
    created_task_ids_in_order: list[str] = []

    if rows_to_upload_list:
        url = f"{AIRTABLE_API_ROOT}/{base_id}/{table_id}"
        records_payload: list[dict[str, Any]] = []
        row_indexes: list[int] = []
        for row in rows_to_upload_list:
            fields = {
                "Task Status": "Unclaimed",
                "Modality": "Text-based",
                "Turns": "Single-turn",
                "Seed Prompt": True,
                "Use Case": to_string(row.get("Use case")),
                "Category": to_string(row.get("Category")),
                "Prompt 1 (Current)": to_string(row.get("Prompt")),
                "Rubric Notes (Current)": to_string(row.get("Rubric Notes & URLs")),
                "Notes": to_string(row.get("Notes")),
            }
            records_payload.append({"fields": fields})
            row_indexes.append(int(row["__row_idx"]))

        uploaded_count = 0
        for chunk_idx, payload_chunk in enumerate(
            tqdm(chunked(records_payload, AIRTABLE_CREATE_BATCH_SIZE), desc="Uploading chunks", unit="chunk"),
            start=1,
        ):
            start = (chunk_idx - 1) * AIRTABLE_CREATE_BATCH_SIZE
            end = start + len(payload_chunk)
            index_chunk = row_indexes[start:end]
            print(f"Uploading chunk {chunk_idx}: {len(payload_chunk)} records")

            response_data = airtable_request(
                method="POST",
                url=url,
                token=table_token,
                timeout_seconds=args.timeout_seconds,
                json_payload={"records": payload_chunk, "typecast": True},
            )
            created_records = response_data.get("records", [])
            if not isinstance(created_records, list):
                raise RuntimeError("Airtable create response missing records array")
            if len(created_records) != len(payload_chunk):
                raise RuntimeError(
                    f"Chunk {chunk_idx} returned {len(created_records)} records for {len(payload_chunk)} uploads"
                )

            for local_i, created_record in enumerate(created_records):
                fields = created_record.get("fields", {})
                if not isinstance(fields, dict):
                    fields = {}

                task_id_value = to_string(fields.get("Task ID")).strip()
                if not task_id_value:
                    task_id_value = to_string(created_record.get("id")).strip()
                    upload_warnings.append(
                        "Airtable 'Task ID' was blank for one record; used record id fallback"
                    )
                if not task_id_value:
                    raise RuntimeError("Airtable record missing both Task ID and record id")
                if task_id_value in expected_by_task_id:
                    raise RuntimeError(f"Duplicate Task ID returned by Airtable create API: {task_id_value}")

                original_row_idx = index_chunk[local_i]
                task_ids[original_row_idx] = task_id_value
                expected_by_task_id[task_id_value] = payload_chunk[local_i]["fields"]
                created_task_ids_in_order.append(task_id_value)
                uploaded_count += 1

        print(f"Uploaded records: {uploaded_count}")
    else:
        print("No rows matched Post-Aug-2025 == TRUE. No Airtable records created.")

    output_df = df.with_columns(pl.Series("Task ID", task_ids))
    output_path = output_path_for(input_path)
    output_df.write_csv(output_path)
    print(f"Wrote uploaded CSV: {output_path}")

    print("Starting validation against Airtable...")
    if upload_warnings:
        for warning in upload_warnings:
            print(f"Warning: {warning}")
    if not expected_by_task_id:
        print("Validation skipped: no uploaded rows.")
        return

    records_by_task_id: dict[str, dict[str, Any]] = {}
    task_id_counts: Counter[str] = Counter()
    task_ids_for_validation = list(expected_by_task_id.keys())

    def fetch_task_records(task_id: str) -> tuple[str, list[dict[str, Any]]]:
        return (
            task_id,
            list_records_by_task_id(
                base_id=base_id,
                table_id=table_id,
                token=table_token,
                timeout_seconds=args.timeout_seconds,
                task_id=task_id,
            ),
        )

    with ThreadPoolExecutor(max_workers=max(1, args.validation_workers)) as executor:
        futures = [executor.submit(fetch_task_records, task_id) for task_id in task_ids_for_validation]
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="Validating Airtable records",
            unit="task",
        ):
            task_id, matched_records = future.result()
            task_id_counts[task_id] = len(matched_records)
            if matched_records:
                fields = matched_records[0].get("fields", {})
                if isinstance(fields, dict):
                    records_by_task_id[task_id] = fields

    validation_errors: list[str] = []
    expected_field_names = [
        "Task Status",
        "Modality",
        "Turns",
        "Seed Prompt",
        "Use Case",
        "Category",
        "Prompt 1 (Current)",
        "Rubric Notes (Current)",
        "Notes",
    ]
    nonblank_content_field_names = [
        "Use Case",
        "Category",
        "Prompt 1 (Current)",
        "Rubric Notes (Current)",
        "Notes",
    ]
    expected_signature_counts: Counter[tuple[str, ...]] = Counter()
    actual_signature_counts: Counter[tuple[str, ...]] = Counter()

    for task_id, expected_fields in expected_by_task_id.items():
        if task_id_counts.get(task_id, 0) > 1:
            validation_errors.append(
                f"Duplicate Task ID in Airtable table: {task_id} (count={task_id_counts[task_id]})"
            )

        actual_fields = records_by_task_id.get(task_id)
        if actual_fields is None:
            validation_errors.append(f"Missing Airtable record for Task ID: {task_id}")
            continue

        if all(to_string(actual_fields.get(field_name)).strip() == "" for field_name in nonblank_content_field_names):
            validation_errors.append(f"Task ID {task_id}: uploaded record appears blank in mapped content fields")

        for field_name in expected_field_names:
            expected_value = to_string(expected_fields.get(field_name)).strip()
            actual_value = to_string(actual_fields.get(field_name)).strip()
            if expected_value != actual_value:
                validation_errors.append(
                    f'Task ID {task_id}: field "{field_name}" mismatch '
                    f'(expected="{expected_value}", actual="{actual_value}")'
                )
        expected_signature_counts[record_signature(expected_fields, expected_field_names)] += 1
        actual_signature_counts[record_signature(actual_fields, expected_field_names)] += 1

    duplicate_signatures = [
        (signature, count) for signature, count in actual_signature_counts.items() if count > expected_signature_counts[signature]
    ]
    for signature, count in duplicate_signatures:
        expected_count = expected_signature_counts[signature]
        validation_errors.append(
            "Duplicate uploaded content detected in Airtable "
            f"(actual={count}, expected={expected_count}, signature={signature})"
        )

    if validation_errors:
        print("Validation failed:")
        for error in validation_errors:
            print(f"- {error}")
        raise RuntimeError(f"Validation failed with {len(validation_errors)} mismatches")

    print("Validation passed: Airtable records match uploaded CSV rows.")
    if created_task_ids_in_order:
        first_created = created_task_ids_in_order[0]
        last_created = created_task_ids_in_order[-1]
        lexicographic_min = min(created_task_ids_in_order)
        lexicographic_max = max(created_task_ids_in_order)
        print(f"Created Task ID range (creation order): {first_created} -> {last_created}")
        print(f"Created Task ID range (lexicographic): {lexicographic_min} -> {lexicographic_max}")


if __name__ == "__main__":
    main()
