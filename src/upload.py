"""Upload scenario-generator outputs to Airtable Tasks as seed prompts.

This script reads scenario rows from `scenarios.csv` and creates one Airtable
Task per scenario. It defaults to the same base/table constants and token env
used by Emporium analytics tooling.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

DEFAULT_BASE_ID = "appx7Mv1XuWdbs6fq"
DEFAULT_TASKS_TABLE_ID = "tblfACDlEtpUmLlMj"
DEFAULT_TOKEN_ENV = "EMPORIUM_TOKEN"
AIRTABLE_API_ROOT = "https://api.airtable.com/v0"
URL_REGEX = re.compile(r"https?://[^\s<>\]\)\},;\"']+")


@dataclass
class UploadConfig:
    csv_path: Path
    batch_name: str
    batch_source: str
    api_token: str
    base_id: str
    table_id: str
    timeout_seconds: int
    dry_run: bool
    update_existing: bool
    limit: int | None
    typecast: bool
    continue_on_error: bool
    prompt_field: str
    category_field: str
    use_case_field: str
    rubric_notes_field: str
    seed_prompt_field: str
    modality_field: str
    modality_value: str
    turns_field: str
    turns_value: str
    status_field: str
    status_value: str
    extra_fields: dict[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload generated scenarios (scenarios.csv) into Airtable Tasks "
            "with Seed Prompt checked."
        )
    )
    parser.add_argument(
        "--batch",
        type=str,
        help=("Batch folder name under scenario_runs " "(e.g. batch_20260228_002459)."),
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Exact path to scenarios.csv (overrides --batch and auto-latest).",
    )
    parser.add_argument(
        "--token-env",
        default=DEFAULT_TOKEN_ENV,
        help=f"Environment variable holding Airtable PAT (default: {DEFAULT_TOKEN_ENV})",
    )
    parser.add_argument(
        "--base-id",
        default=DEFAULT_BASE_ID,
        help=f"Airtable base id (default: {DEFAULT_BASE_ID})",
    )
    parser.add_argument(
        "--table-id",
        default=DEFAULT_TASKS_TABLE_ID,
        help=f"Airtable table id or name (default: {DEFAULT_TASKS_TABLE_ID})",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print payload preview without creating Airtable records.",
    )
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help=(
            "Update existing Airtable records by prompt text and fill missing "
            "Category/Use Case instead of creating new records."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Upload only first N scenario rows.",
    )
    parser.add_argument(
        "--no-typecast",
        action="store_true",
        help="Disable Airtable typecast (enabled by default).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue uploading next rows when a record fails.",
    )
    parser.add_argument(
        "--prompt-field",
        default="Prompt 1 (Current)",
        help='Airtable field for prompt text (default: "Prompt 1 (Current)")',
    )
    parser.add_argument(
        "--category-field",
        default="Category",
        help='Airtable field for scenario category (default: "Category")',
    )
    parser.add_argument(
        "--use-case-field",
        default="Use Case",
        help='Airtable field for scenario use case (default: "Use Case")',
    )
    parser.add_argument(
        "--rubric-notes-field",
        default="Rubric Notes (Current)",
        help='Airtable field for browsing path text (default: "Rubric Notes (Current)")',
    )
    parser.add_argument(
        "--seed-prompt-field",
        default="Seed Prompt",
        help='Checkbox field that must be set true (default: "Seed Prompt")',
    )
    parser.add_argument(
        "--modality-field",
        default="Modality",
        help='Field for modality value (default: "Modality")',
    )
    parser.add_argument(
        "--modality",
        default="Text-based",
        help='Modality field value (default: "Text-based")',
    )
    parser.add_argument(
        "--turns-field",
        default="Turns",
        help='Field for turns value (default: "Turns")',
    )
    parser.add_argument(
        "--turns",
        default="Single-turn",
        help='Turns field value (default: "Single-turn")',
    )
    parser.add_argument(
        "--status-field",
        default="Task Status",
        help='Field for task status value (default: "Task Status")',
    )
    parser.add_argument(
        "--status",
        default="Unclaimed",
        help='Task status value for created rows (default: "Unclaimed")',
    )
    parser.add_argument(
        "--extra-field",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Add a static Airtable field to every record. Repeatable.",
    )
    return parser.parse_args()


def resolve_csv_path(
    csv_path: Path | None = None,
    batch: str | None = None,
) -> Path:
    if csv_path:
        if not csv_path.is_file():
            raise FileNotFoundError(f"--csv is not a file: {csv_path}")
        return csv_path

    if batch:
        batch_dir = Path("scenario_runs") / batch
        if not batch_dir.is_dir():
            raise FileNotFoundError(f"Batch directory not found: {batch_dir}")
        csv_path = batch_dir / "scenarios.csv"
        if not csv_path.exists():
            raise FileNotFoundError(f"No scenarios.csv found for batch: {batch}")
        return csv_path

    default_root = Path("scenario_runs")
    candidates = sorted(
        default_root.glob("**/scenarios.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("No scenarios.csv found under scenario_runs/")
    return candidates[0]


def parse_extra_fields(entries: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in entries:
        if "=" not in item:
            raise ValueError(f"Invalid --extra-field (expected KEY=VALUE): {item}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Invalid --extra-field (empty key): {item}")
        out[key] = value
    return out


def load_config(args: argparse.Namespace) -> UploadConfig:
    # Load local .env so EMPORIUM_TOKEN can be read without manual export.
    load_dotenv()

    csv_path = resolve_csv_path(args.csv, args.batch)
    token = os.environ.get(args.token_env, "").strip()
    if not token and not args.dry_run:
        raise RuntimeError(
            f"Missing Airtable token. Set env var {args.token_env} or use --dry-run."
        )
    batch_name = csv_path.parent.name
    if args.csv:
        batch_source = "explicit (--csv)"
    elif args.batch:
        batch_source = "explicit (--batch)"
    else:
        batch_source = "latest (auto)"
    return UploadConfig(
        csv_path=csv_path,
        batch_name=batch_name,
        batch_source=batch_source,
        api_token=token,
        base_id=args.base_id,
        table_id=args.table_id,
        timeout_seconds=args.timeout_seconds,
        dry_run=args.dry_run,
        update_existing=args.update_existing,
        limit=args.limit,
        typecast=not args.no_typecast,
        continue_on_error=args.continue_on_error,
        prompt_field=args.prompt_field,
        category_field=args.category_field,
        use_case_field=args.use_case_field,
        rubric_notes_field=args.rubric_notes_field,
        seed_prompt_field=args.seed_prompt_field,
        modality_field=args.modality_field,
        modality_value=args.modality,
        turns_field=args.turns_field,
        turns_value=args.turns,
        status_field=args.status_field,
        status_value=args.status,
        extra_fields=parse_extra_fields(args.extra_field),
    )


def load_scenario_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def get_row_value(row: dict[str, str], *candidates: str) -> str:
    for key in candidates:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def row_to_airtable_fields(row: dict[str, str], config: UploadConfig) -> dict[str, Any]:
    prompt_value = sanitize_text(get_row_value(row, "prompt", "Prompt", "Final Prompt"))
    fields: dict[str, Any] = {
        config.prompt_field: prompt_value,
        config.seed_prompt_field: True,
    }
    if config.category_field:
        category = get_row_value(row, "category", "Category")
        if category:
            fields[config.category_field] = category
    if config.use_case_field:
        use_case = get_row_value(row, "use_case", "Use case", "Use Case")
        if use_case:
            fields[config.use_case_field] = use_case
    if config.rubric_notes_field:
        browsing_path = get_row_value(row, "browsing_path", "URLs", "urls")
        if browsing_path:
            fields[config.rubric_notes_field] = sanitize_urls_field(browsing_path)
    if config.modality_field and config.modality_value:
        fields[config.modality_field] = config.modality_value
    if config.turns_field and config.turns_value:
        fields[config.turns_field] = config.turns_value
    if config.status_field and config.status_value:
        fields[config.status_field] = config.status_value
    fields.update(config.extra_fields)
    for key, value in list(fields.items()):
        if isinstance(value, str):
            fields[key] = sanitize_text(value)
    return fields


def is_placeholder_value(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return True
    lowered = text.lower()
    return lowered.startswith("skipped") or lowered.startswith("error:")


def sanitize_text(value: str) -> str:
    return re.sub(r"meta api", "Client API", value, flags=re.IGNORECASE)


def sanitize_urls_field(browsing_path: str) -> str:
    """Return only final URLs, stripping repair-note context and descriptions."""
    urls: list[str] = []
    for raw_line in browsing_path.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        matches = URL_REGEX.findall(line)
        if not matches:
            continue
        # If a repair note contains old -> new URLs, keep only the final URL.
        if "->" in line:
            urls.append(matches[-1])
        else:
            urls.append(matches[0])

    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)

    if not deduped:
        return sanitize_text(browsing_path)

    return "\n".join(f"{idx}. {url}" for idx, url in enumerate(deduped, start=1))


def airtable_create_records(
    records: list[dict[str, Any]],
    config: UploadConfig,
) -> list[dict[str, Any]]:
    url = f"{AIRTABLE_API_ROOT}/{config.base_id}/{config.table_id}"
    headers = {
        "Authorization": f"Bearer {config.api_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "records": [{"fields": r} for r in records],
        "typecast": config.typecast,
    }
    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=config.timeout_seconds,
    )
    if response.status_code >= 400:
        message = response.text
        raise RuntimeError(
            f"Airtable create failed ({response.status_code}). Response: {message}"
        )
    data = response.json()
    return data.get("records", [])


def airtable_list_records(
    config: UploadConfig, fields: list[str]
) -> list[dict[str, Any]]:
    url = f"{AIRTABLE_API_ROOT}/{config.base_id}/{config.table_id}"
    headers = {
        "Authorization": f"Bearer {config.api_token}",
    }

    out: list[dict[str, Any]] = []
    offset: str | None = None
    while True:
        params: dict[str, Any] = {"pageSize": 100, "fields[]": fields}
        if offset:
            params["offset"] = offset
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=config.timeout_seconds,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Airtable list failed ({response.status_code}). Response: {response.text}"
            )
        payload = response.json()
        out.extend(payload.get("records", []))
        offset = payload.get("offset")
        if not offset:
            break
    return out


def airtable_update_records(
    records: list[dict[str, Any]],
    config: UploadConfig,
) -> list[dict[str, Any]]:
    url = f"{AIRTABLE_API_ROOT}/{config.base_id}/{config.table_id}"
    headers = {
        "Authorization": f"Bearer {config.api_token}",
        "Content-Type": "application/json",
    }
    payload = {"records": records, "typecast": config.typecast}
    response = requests.patch(
        url,
        headers=headers,
        json=payload,
        timeout=config.timeout_seconds,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            f"Airtable update failed ({response.status_code}). Response: {response.text}"
        )
    data = response.json()
    return data.get("records", [])


def chunked(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def summarize_created_ids(records: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    record_ids: list[str] = []
    task_ids: list[str] = []
    for record in records:
        rec_id = str(record.get("id") or "").strip()
        if rec_id:
            record_ids.append(rec_id)
        fields = record.get("fields", {}) or {}
        task_id = fields.get("Task ID")
        if task_id is not None and str(task_id).strip():
            task_ids.append(str(task_id).strip())
    return record_ids, task_ids


def run(config: UploadConfig) -> int:
    rows = load_scenario_rows(config.csv_path)
    if config.limit is not None:
        rows = rows[: config.limit]

    print(f"Batch: {config.batch_name} [{config.batch_source}]")
    print(f"CSV: {config.csv_path}")

    if not rows:
        print(f"No rows found in {config.csv_path}")
        return 0

    prepared: list[tuple[int, dict[str, Any]]] = []
    for idx, row in enumerate(rows, start=1):
        prompt_value = get_row_value(row, "prompt", "Prompt", "Final Prompt")
        browsing_path_value = get_row_value(row, "browsing_path", "URLs", "urls")
        if is_placeholder_value(prompt_value):
            print(f"Skipping row {idx}: placeholder or empty prompt")
            continue
        if is_placeholder_value(browsing_path_value):
            print(f"Skipping row {idx}: placeholder or empty browsing_path")
            continue

        fields = row_to_airtable_fields(row, config)
        if not str(fields.get(config.prompt_field, "")).strip():
            print(f"Skipping row {idx}: empty prompt")
            continue
        prepared.append((idx, fields))

    if not prepared:
        print("No valid rows to upload after filtering empty prompts.")
        return 0

    print(f"Rows loaded: {len(rows)}")
    print(f"Rows prepared: {len(prepared)}")
    print(f"Airtable base/table: {config.base_id}/{config.table_id}")
    print(
        "Mode: update existing missing Category/Use Case"
        if config.update_existing
        else "Mode: create new records"
    )

    if config.dry_run:
        sample = prepared[0][1]
        if config.update_existing:
            print(
                "Dry run enabled in update mode. "
                "Will match existing records by prompt and patch missing "
                "Category/Use Case."
            )
            print("Sample source payload fields:")
        else:
            print("Dry run enabled. Sample payload fields:")
        print(json.dumps(sample, indent=2, ensure_ascii=False))
        return 0

    if config.update_existing:
        prompt_to_fields: dict[str, dict[str, Any]] = {}
        for _, fields in prepared:
            prompt = str(fields.get(config.prompt_field) or "").strip()
            if prompt and prompt not in prompt_to_fields:
                prompt_to_fields[prompt] = fields

        existing = airtable_list_records(
            config,
            [
                config.prompt_field,
                config.category_field,
                config.use_case_field,
                "Task ID",
            ],
        )
        print(f"Fetched {len(existing)} existing Airtable task records.")

        def _is_missing(value: Any) -> bool:
            if value is None:
                return True
            if isinstance(value, str):
                return not value.strip()
            if isinstance(value, list):
                return len(value) == 0
            return False

        updates: list[dict[str, Any]] = []
        for record in existing:
            record_id = str(record.get("id") or "").strip()
            if not record_id:
                continue
            existing_fields = record.get("fields", {}) or {}
            prompt = str(existing_fields.get(config.prompt_field) or "").strip()
            if not prompt:
                continue
            source_fields = prompt_to_fields.get(prompt)
            if not source_fields:
                continue

            patch_fields: dict[str, Any] = {}
            if _is_missing(existing_fields.get(config.category_field)):
                value = source_fields.get(config.category_field)
                if isinstance(value, str) and value.strip():
                    patch_fields[config.category_field] = value
            if _is_missing(existing_fields.get(config.use_case_field)):
                value = source_fields.get(config.use_case_field)
                if isinstance(value, str) and value.strip():
                    patch_fields[config.use_case_field] = value

            if patch_fields:
                updates.append({"id": record_id, "fields": patch_fields})

        if not updates:
            print("No matching existing records required Category/Use Case updates.")
            return 0

        updated_count = 0
        failed_count = 0
        updated_record_ids: list[str] = []
        update_batches = chunked(updates, 10)
        for batch_index, batch in enumerate(update_batches, start=1):
            print(
                f"Updating batch {batch_index}/{len(update_batches)} "
                f"({len(batch)} record(s))..."
            )
            try:
                updated = airtable_update_records(batch, config)
                updated_count += len(updated)
                batch_ids = [
                    str(r.get("id") or "").strip() for r in updated if r.get("id")
                ]
                updated_record_ids.extend(batch_ids)
                if batch_ids:
                    print(f"  Updated Airtable record IDs: {', '.join(batch_ids)}")
            except Exception as exc:
                print(f"Update batch failed: {exc}")
                if not config.continue_on_error:
                    return 1
                failed_count += len(batch)

        print(f"Done. Updated: {updated_count}, Failed: {failed_count}")
        if updated_record_ids:
            print(f"All updated Airtable record IDs: {', '.join(updated_record_ids)}")
        return 0 if failed_count == 0 else 1

    created_count = 0
    failed_count = 0
    all_record_ids: list[str] = []
    all_task_ids: list[str] = []
    batches = chunked(prepared, 10)
    total_batches = len(batches)
    for batch_index, batch in enumerate(batches, start=1):
        batch_rows = [r for _, r in batch]
        batch_ids = [idx for idx, _ in batch]
        print(
            f"Uploading batch {batch_index}/{total_batches} "
            f"(CSV rows {batch_ids[0]}-{batch_ids[-1]})..."
        )
        try:
            created = airtable_create_records(batch_rows, config)
            created_count += len(created)
            batch_record_ids, batch_task_ids = summarize_created_ids(created)
            all_record_ids.extend(batch_record_ids)
            all_task_ids.extend(batch_task_ids)
            print(
                f"Created {len(created)} records for CSV rows {batch_ids[0]}-{batch_ids[-1]}"
            )
            if batch_record_ids:
                print(f"  Airtable record IDs: {', '.join(batch_record_ids)}")
            if batch_task_ids:
                print(f"  Task IDs: {', '.join(batch_task_ids)}")
        except Exception as exc:
            print(f"Batch failed for rows {batch_ids[0]}-{batch_ids[-1]}: {exc}")
            if not config.continue_on_error:
                return 1
            for idx, row_fields in batch:
                try:
                    print(f"Retrying row {idx} as single-record upload...")
                    created = airtable_create_records([row_fields], config)
                    created_count += len(created)
                    row_record_ids, row_task_ids = summarize_created_ids(created)
                    all_record_ids.extend(row_record_ids)
                    all_task_ids.extend(row_task_ids)
                    print(f"Created row {idx}")
                    if row_record_ids:
                        print(f"  Airtable record IDs: {', '.join(row_record_ids)}")
                    if row_task_ids:
                        print(f"  Task IDs: {', '.join(row_task_ids)}")
                except Exception as row_exc:
                    failed_count += 1
                    print(f"Failed row {idx}: {row_exc}")

    print(f"Done. Created: {created_count}, Failed: {failed_count}")
    if all_record_ids:
        print(f"All Airtable record IDs: {', '.join(all_record_ids)}")
    if all_task_ids:
        print(f"All Task IDs: {', '.join(all_task_ids)}")
    return 0 if failed_count == 0 else 1


def main() -> int:
    args = parse_args()
    try:
        config = load_config(args)
        return run(config)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
