#!/usr/bin/env python3
import json
import sys
from pathlib import Path

MIB = 1024 * 1024


def load_summary(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def main() -> int:
    if len(sys.argv) != 4:
        print(
            "usage: scripts/ci/compare-memory-smoke.py <base-summary.json> <head-summary.json> <output-report.json>",
            file=sys.stderr,
        )
        return 2

    base_path = Path(sys.argv[1])
    head_path = Path(sys.argv[2])
    out_path = Path(sys.argv[3])

    base = load_summary(base_path)
    head = load_summary(head_path)

    base_hwm = int(base.get("vm_hwm_bytes", 0))
    head_hwm = int(head.get("vm_hwm_bytes", 0))
    base_median = int(base.get("post_cleanup_rss_median_bytes", base.get("rss_median_bytes", 0)))
    head_median = int(head.get("post_cleanup_rss_median_bytes", head.get("rss_median_bytes", 0)))
    base_cleanup_samples = int(base.get("post_cleanup_rss_samples_count", 0))
    head_cleanup_samples = int(head.get("post_cleanup_rss_samples_count", 0))

    hwm_limit = int(base_hwm * 1.15 + 32 * MIB)
    median_limit = int(base_median * 1.20 + 16 * MIB)

    checks = {
        "base_post_cleanup_samples_present": base_cleanup_samples > 0,
        "head_post_cleanup_samples_present": head_cleanup_samples > 0,
        "head_readiness_failures_is_zero": int(head.get("readiness_failures", 0)) == 0,
        "head_rejected_writes_is_zero": int(head.get("rejected_writes_total", 0)) == 0,
        "head_vm_hwm_within_threshold": head_hwm <= hwm_limit,
        "head_post_cleanup_median_within_threshold": head_median <= median_limit,
    }

    report = {
        "base_summary_path": str(base_path),
        "head_summary_path": str(head_path),
        "thresholds": {
            "vm_hwm_limit_bytes": hwm_limit,
            "post_cleanup_rss_median_limit_bytes": median_limit,
        },
        "base": {
            "vm_hwm_bytes": base_hwm,
            "post_cleanup_rss_median_bytes": base_median,
            "post_cleanup_rss_samples_count": base_cleanup_samples,
        },
        "head": {
            "vm_hwm_bytes": head_hwm,
            "post_cleanup_rss_median_bytes": head_median,
            "post_cleanup_rss_samples_count": head_cleanup_samples,
            "readiness_failures": int(head.get("readiness_failures", 0)),
            "rejected_writes_total": int(head.get("rejected_writes_total", 0)),
        },
        "checks": checks,
        "ok": all(checks.values()),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)
        fh.write("\n")

    print("Memory smoke comparison report")
    print(f"  base vm_hwm_bytes: {base_hwm}")
    print(f"  head vm_hwm_bytes: {head_hwm}")
    print(f"  vm_hwm_limit_bytes: {hwm_limit}")
    print(f"  base post_cleanup_rss_median_bytes: {base_median}")
    print(f"  head post_cleanup_rss_median_bytes: {head_median}")
    print(f"  base post_cleanup_rss_samples_count: {base_cleanup_samples}")
    print(f"  head post_cleanup_rss_samples_count: {head_cleanup_samples}")
    print(f"  post_cleanup_rss_median_limit_bytes: {median_limit}")
    print("  checks:")
    for name, value in checks.items():
        print(f"    - {name}: {'PASS' if value else 'FAIL'}")

    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
