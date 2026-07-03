from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.generate_arbitrage_config import (  # noqa: E402
    build_pairs,
    extract_pair_defaults,
    load_config,
    read_json,
    read_stockinfo,
    read_table,
    rebase_fubon_cert_paths,
    remove_replay_date,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run daily.ipynb to build target futures, then generate arbitrage_config.json."
    )
    parser.add_argument(
        "--notebook",
        default="daily.ipynb",
        help="Notebook that exports the target futures file.",
    )
    parser.add_argument(
        "--executed-notebook",
        default=r"output\daily_debug_executed.ipynb",
        help="Path for the executed notebook output.",
    )
    parser.add_argument(
        "--skip-notebook",
        action="store_true",
        help="Skip notebook execution and reuse the existing targets file.",
    )
    parser.add_argument(
        "--targets",
        default=r"output\target_futures.csv",
        help="Target futures CSV/Parquet path produced by the notebook.",
    )
    parser.add_argument(
        "--base-config",
        default="arbitrage_unified_config.example.json",
        help="Shared config containing pair_defaults.",
    )
    parser.add_argument(
        "--stockinfo",
        default="stockinfo.csv",
        help="Stock future mapping csv.",
    )
    parser.add_argument(
        "--output",
        default="arbitrage_config.json",
        help="Generated arbitrage config output path.",
    )
    parser.add_argument(
        "--name-template",
        default="{spot_symbol}_{future_symbol}",
        help="Pair name template using {spot_symbol}, {future_symbol}, {future_prefix}.",
    )
    parser.add_argument(
        "--target-encoding",
        default="utf-8-sig",
        help="Target CSV encoding.",
    )
    parser.add_argument(
        "--notebook-timeout",
        type=int,
        default=300,
        help="Notebook execution timeout in seconds.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.skip_notebook:
        execute_notebook(
            notebook=Path(args.notebook),
            output=Path(args.executed_notebook),
            timeout=args.notebook_timeout,
        )

    base = read_json(Path(args.base_config))
    pair_defaults = extract_pair_defaults(base, Path(args.base_config))
    targets = read_table(Path(args.targets), encoding=args.target_encoding)
    mapping = read_stockinfo(Path(args.stockinfo))
    pairs = build_pairs(
        targets=targets,
        mapping=mapping,
        pair_defaults=pair_defaults,
        name_template=args.name_template,
    )

    output_config = {key: value for key, value in base.items() if key not in {"pair_defaults", "pairs"}}
    remove_replay_date(output_config)
    rebase_fubon_cert_paths(output_config, Path(args.base_config).parent)
    output_config["pairs"] = pairs
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(output_config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    load_config(output)
    logging.info("wrote %s pairs to %s", len(pairs), output)


def execute_notebook(notebook: Path, output: Path, timeout: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "jupyter",
        "nbconvert",
        "--to",
        "notebook",
        "--execute",
        str(notebook),
        "--output",
        str(output),
        f"--ExecutePreprocessor.timeout={timeout}",
    ]
    logging.info("executing notebook: %s", notebook)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)
    logging.info("executed notebook written to %s", output)


if __name__ == "__main__":
    main()
