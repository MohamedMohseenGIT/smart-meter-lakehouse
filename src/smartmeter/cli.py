"""Command-line entry point packaged in the wheel (python_wheel_task `generate_sources`)."""

from __future__ import annotations

import argparse
import json
from datetime import date

from smartmeter.generator import GeneratorConfig, run


def generate_main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Land synthetic upstream files into the raw volume.")
    p.add_argument("--catalog")
    p.add_argument("--landing_schema")
    p.add_argument("--volume", default="raw")
    p.add_argument("--output_root", help="Override the target directory (local runs and tests).")
    p.add_argument("--customers", type=int, default=GeneratorConfig.n_customers)
    p.add_argument("--backfill_days", type=int, default=GeneratorConfig.backfill_days)
    p.add_argument("--until", help="Last day to generate (YYYY-MM-DD). Default: yesterday.")
    args, _unknown = p.parse_known_args(argv)

    root = args.output_root or f"/Volumes/{args.catalog}/{args.landing_schema}/{args.volume}"
    cfg = GeneratorConfig(n_customers=args.customers, backfill_days=args.backfill_days)
    result = run(root, cfg, date.fromisoformat(args.until) if args.until else None)
    print(json.dumps({"root": root, **result}))


if __name__ == "__main__":
    generate_main()
