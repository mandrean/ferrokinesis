#!/usr/bin/env python3

import argparse
import json
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ref", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    template = args.template.read_text(encoding="utf-8").rstrip()
    result_paths = sorted(args.results_dir.glob("*.json"))
    if not result_paths:
        raise SystemExit("no compatibility result files found")

    rows = [json.loads(path.read_text(encoding="utf-8")) for path in result_paths]
    rows.sort(key=lambda row: row["suite"])

    outcome_counts = Counter(row["outcome"] for row in rows)
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
        outcome = row["outcome"]
        label = status_labels.get(outcome, outcome.upper())
        needs_dynamodb = "yes" if row["needs_dynamodb"] else "no"
        lines.append(
            f"| {row['suite']} | `{row['language']}` | {label} | {needs_dynamodb} |"
        )

    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
