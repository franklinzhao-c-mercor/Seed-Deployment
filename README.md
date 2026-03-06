# Seed Deployment Tools

This repo has two CLI tools:
- `src/label_product_release.py`
- `src/upload.py`

## Setup

1. Install dependencies:
```bash
uv sync
```

2. Create a `.env` file (or export vars in shell).

---

## Tool 1: Label Product Release Dates

Script: `src/label_product_release.py`

### What it does
- Reads a CSV with a required `Product` column.
- Splits each row's `Product` value by comma into individual products.
- Uses Claude web search to find release dates.
- Adds:
  - `Release date` (`Month Day Year` or `Month Year`)
  - `Post-Aug-2025` (`TRUE` when date is in/after August 2025, otherwise `False`)
- Writes a new CSV next to input with `-with-release-dates` suffix.

### Required env vars
- `ANTHROPIC_API_KEY`

Optional:
- `ANTHROPIC_MODEL` (default model is used if unset)
- `ANTHROPIC_BETA` (only if your API config needs it)

### Usage
```bash
uv run python src/label_product_release.py <input_csv>
```

Example:
```bash
uv run python src/label_product_release.py data/scenarios.csv
```

### Useful flags
- `--model`
- `--max-workers` (parallel product lookups)
- `--max-searches-per-product`
- `--max-attempts-per-product`
- `--retry-base-delay-seconds`
- `--retry-backoff-factor`
- `--request-timeout-seconds`

---

## Tool 2: Upload Rows to Airtable

Script: `src/upload.py`

### What it does
- Validates required CSV columns first.
- Uploads rows to Airtable, excluding only rows where:
  - `Release date` is non-blank, and
  - `Post-Aug-2025` is `False`
- Sets Airtable fields:
  - `Task Status = Unclaimed`
  - `Modality = Text-based`
  - `Turns = Single-turn`
  - `Seed Prompt = True` (checkbox checked)
  - `Use Case <- Use case`
  - `Category <- Category`
  - `Prompt 1 (Current) <- Prompt`
  - `Rubric Notes (Current) <- Rubric Notes & URLs`
  - `Notes <- Notes`
- Writes a new CSV with `Task ID` column and `-uploaded` suffix.
- Validates uploaded records against Airtable (parallelized with `tqdm`).

### Required CSV columns
- `Post-Aug-2025`
- `Release date`
- `Use case`
- `Category`
- `Prompt`
- `Rubric Notes & URLs`
- `Notes`

### Required env vars
- `EMPORIUM_BASE_1` / `EMPORIUM_BASE_2` / `EMPORIUM_BASE_3` / `EMPORIUM_BASE_4`
- `EMPORIUM_TASKS_TABLE_ID`
- `EMPORIUM_SEEDING_TOKEN`

### Usage
```bash
uv run python src/upload.py <input_csv> --team <1|2|3|4>
```

Example:
```bash
uv run python src/upload.py data/scenarios-with-release-dates.csv --team 1
```

### Useful flags
- `--timeout-seconds`
- `--validation-workers`

---

## Output Files

- Release labeling output:
  - `<name>-with-release-dates.csv`
- Upload output:
  - `<name>-uploaded.csv`
