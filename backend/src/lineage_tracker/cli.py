"""CLI entry point for sql-lineage-tracker."""

import argparse
import sys
import webbrowser
from pathlib import Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sql-lineage-tracker",
        description="BigQuery column-level SQL lineage tracker",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser(
        "serve",
        help="Start the lineage tracker server",
    )
    serve_parser.add_argument(
        "--project",
        required=True,
        help="GCP project ID",
    )
    serve_parser.add_argument(
        "--target",
        default=None,
        help="Target table/view (dataset.table) to trace lineage backward from",
    )
    serve_parser.add_argument(
        "--dataset",
        action="append",
        default=None,
        dest="datasets",
        help="Limit scan to specific dataset (repeatable)",
    )
    serve_parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Max depth in dataset hops (default: unlimited)",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=8050,
        help="Server port (default: 8050)",
    )
    serve_parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path.home() / ".sql-lineage-tracker",
        help="Persistence directory (default: ~/.sql-lineage-tracker)",
    )
    serve_parser.add_argument(
        "--no-scan",
        action="store_true",
        default=False,
        help="Skip BigQuery connection, use cached graph",
    )

    return parser.parse_args(argv)


def cmd_serve(args: argparse.Namespace) -> None:
    """Execute the serve command."""
    import uvicorn

    from lineage_tracker.server import create_app

    has_scan_flags = args.target is not None or args.datasets is not None

    scan_config = None
    if has_scan_flags and not args.no_scan:
        scan_config = {
            "target": args.target,
            "datasets": args.datasets or [],
            "depth": args.depth,
        }

    app = create_app(
        project_id=args.project,
        data_dir=args.data_dir,
        no_scan=args.no_scan,
        initial_scan_config=scan_config,
    )

    print(f"Starting sql-lineage-tracker server on http://localhost:{args.port}")
    print(f"Project: {args.project}")
    print(f"Data dir: {args.data_dir}")

    if args.no_scan:
        print("Mode: offline (no BigQuery connection)")
    elif scan_config:
        print(f"Scan config: {scan_config}")
    else:
        print("Mode: no scan flags provided")

    webbrowser.open(f"http://localhost:{args.port}")

    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="info")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    if args.command == "serve":
        cmd_serve(args)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)
