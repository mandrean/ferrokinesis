#!/usr/bin/env python3

import argparse
import json
import re
from collections import Counter
from pathlib import Path

VALID_OUTCOMES = {"success", "failure", "cancelled", "skipped"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workflow", type=Path)
    parser.add_argument("--ref", default="")
    return parser.parse_args()


def load_expected_slugs(workflow_path: Path) -> list[str]:
    workflow = workflow_path.read_text(encoding="utf-8")
    matches = re.findall(r"^\s+slug:\s*([A-Za-z0-9][A-Za-z0-9._-]*)\s*$", workflow, re.MULTILINE)
    if not matches:
        raise SystemExit(f"no compatibility suite slugs found in workflow: {workflow_path}")
    return matches


def load_rows(result_paths: list[Path]) -> list[dict[str, object]]:
    if not result_paths:
        raise SystemExit("no compatibility result files found")

    rows = [json.loads(path.read_text(encoding="utf-8")) for path in result_paths]
    duplicate_slugs = sorted(
        slug for slug, count in Counter(str(row["slug"]) for row in rows).items() if count > 1
    )
    if duplicate_slugs:
        raise SystemExit(
            "duplicate compatibility result slugs: " + ", ".join(duplicate_slugs)
        )
    return rows


def validate_rows(rows: list[dict[str, object]], expected_slugs: list[str]) -> None:
    actual_slugs = [str(row["slug"]) for row in rows]
    missing = sorted(set(expected_slugs) - set(actual_slugs))
    unexpected = sorted(set(actual_slugs) - set(expected_slugs))

    if missing or unexpected or len(actual_slugs) != len(expected_slugs):
        details: list[str] = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        if len(actual_slugs) != len(expected_slugs):
            details.append(
                f"expected {len(expected_slugs)} suites but found {len(actual_slugs)} result files"
            )
        raise SystemExit("incomplete compatibility result set; " + "; ".join(details))

    for row in rows:
        outcome = row.get("outcome")
        if not isinstance(outcome, str) or outcome not in VALID_OUTCOMES:
            raise SystemExit(
                "invalid compatibility outcome for "
                f"{row['slug']}: {outcome!r}"
            )


def main() -> int:
    args = parse_args()

    template = args.template.read_text(encoding="utf-8").rstrip()
    result_paths = sorted(args.results_dir.glob("*.json"))
    rows = load_rows(result_paths)
    expected_slugs = load_expected_slugs(args.workflow) if args.workflow else []
    if expected_slugs:
        validate_rows(rows, expected_slugs)
        sort_order = {slug: index for index, slug in enumerate(expected_slugs)}
        rows.sort(key=lambda row: sort_order[str(row["slug"])])
    else:
        rows.sort(key=lambda row: str(row["suite"]))

    outcome_counts = Counter(str(row["outcome"]) for row in rows)
    status_labels = {
        "success": "PASS",
        "failure": "FAIL",
        "cancelled": "CANCELLED",
        "skipped": "SKIPPED",
    }

    lines = [template, "", "## Latest Run"]
    if args.ref:
        lines.append(f"- Ref: `{args.ref}`")
    lines.append(f"- Suites: {len(rows)}")
    lines.append(f"- Passed: {outcome_counts.get('success', 0)}")
    lines.append(f"- Failed: {outcome_counts.get('failure', 0)}")
    lines.append(f"- Cancelled: {outcome_counts.get('cancelled', 0)}")
    lines.append(f"- Skipped: {outcome_counts.get('skipped', 0)}")
    lines.append("")
    lines.append("| Suite | Language | Result | DynamoDB Local |")
    lines.append("| --- | --- | --- | --- |")

    for row in rows:
        outcome = str(row["outcome"])
        label = status_labels[outcome]
        needs_dynamodb = "yes" if row["needs_dynamodb"] else "no"
        lines.append(
            f"| {row['suite']} | `{row['language']}` | {label} | {needs_dynamodb} |"
        )

    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
