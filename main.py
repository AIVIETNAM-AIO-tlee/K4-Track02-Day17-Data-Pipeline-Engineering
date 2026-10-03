"""Run the Day 17 pipeline. Zero-key, DuckDB-only.

    python main.py                         # fresh build: reset Silver/Gold, backfill 08-10..08-16
    python main.py --date 2026-08-12       # one daily run on the existing warehouse
    python main.py --backfill 2026-08-10 2026-08-16
    python main.py --land-only             # only land Bronze Parquet (used by the dbt track)
    python main.py --lateness              # measure event lateness from Bronze
    python main.py --reset                 # drop Silver + Gold (Bronze stays)
"""
from __future__ import annotations

import argparse
import sys

from pipeline import config
from pipeline.bronze import land_day
from pipeline.checksum import gold_checksums
from pipeline.gold import lateness_profile
from pipeline.run import backfill, connect, reset_warehouse, run_day, summarize


def print_checksums(con) -> dict:
    cs = gold_checksums(con)
    print("\nGold checksums:")
    for t in config.GOLD_TABLES:
        print(f"  {t:<20} {cs[t]}")
    print(f"  {'gold (combined)':<20} {cs['gold']}")
    return cs


def fresh_build(quiet: bool = False) -> dict:
    reset_warehouse()
    con = connect()
    try:
        if not quiet:
            print(f"=== Day 17 pipeline: fresh build {config.FIRST_DAY} .. {config.LAST_DAY} ===")
        backfill(con, config.FIRST_DAY, config.LAST_DAY, quiet=quiet)
        return gold_checksums(con) if quiet else print_checksums(con)
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", help="run one ingest day (YYYY-MM-DD) on the existing warehouse")
    ap.add_argument("--backfill", nargs=2, metavar=("START", "END"))
    ap.add_argument("--land-only", action="store_true")
    ap.add_argument("--lateness", action="store_true")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args(argv)

    if args.reset:
        reset_warehouse()
        print("warehouse reset (Bronze untouched)")
        return 0
    if not (args.date or args.backfill or args.land_only or args.lateness):
        fresh_build()
        return 0

    con = connect()
    try:
        if args.land_only:
            for day in config.days_between(config.FIRST_DAY, config.LAST_DAY):
                res = land_day(con, day)
                print(f"  {day}  " + "  ".join(f"{s}:{v['status']}({v['rows']})" for s, v in res.items()))
        if args.lateness:
            lp = lateness_profile(con)
            print(f"event lateness over {lp['n']} Bronze records (calendar days): "
                  f"p50={lp['p50']:.2f} p95={lp['p95']:.2f} p99={lp['p99']:.2f} max={lp['max']}")
            print(f"-> lookback must be >= ceil(p99) = {lp['p99_days']} day(s); "
                  f"config.LOOKBACK_DAYS = {config.LOOKBACK_DAYS}")
        if args.backfill:
            backfill(con, *args.backfill)
            print_checksums(con)
        if args.date:
            print(summarize(args.date, run_day(con, args.date)))
            print_checksums(con)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
