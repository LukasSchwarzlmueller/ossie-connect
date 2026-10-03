"""Command line wrapper around the Fabric and Databricks connections.

    ossie-connect check    fabric
    ossie-connect upload   fabric     model.yaml
    ossie-connect download fabric     sales_demo -o model.yaml
    ossie-connect upload   databricks model.yaml
    ossie-connect download databricks sales_demo -o model.yaml

Connection settings come from the environment (and a .env file beside you, if there is
one); every one of them can be overridden with a flag. --dry-run prints what would be
sent and connects to nothing.
"""

import argparse
import os
import sys
from pathlib import Path

from .connection import Connection, SupportsDownload
from .console import plain_warnings
from .env import load_env


def _build_parser():
    parser = argparse.ArgumentParser(
        prog="ossie-connect",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="store_true", help="print the version and exit")
    sub = parser.add_subparsers(dest="command")

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("target", choices=["fabric", "databricks", "snowflake"])
    common.add_argument("--warnings", action="store_true", help="show conversion warnings")
    common.add_argument("--workspace", help="Fabric workspace id")
    common.add_argument("--lakehouse", help="Fabric lakehouse item id")
    common.add_argument("--schema", help="Fabric schema, or Databricks schema")
    common.add_argument("--catalog", help="Databricks catalog")
    common.add_argument("--warehouse-id", help="Databricks SQL warehouse id")
    common.add_argument("--database", help="Snowflake database")

    up = sub.add_parser("upload", parents=[common], help="Ossie file -> platform")
    up.add_argument("model", help="Ossie YAML file, or a folder of them")
    up.add_argument("--name", help="remote name (default: the model's own name)")
    up.add_argument(
        "--dry-run", action="store_true", help="print what would be sent, connect to nothing"
    )

    sub.add_parser(
        "check", parents=[common],
        help="verify the settings describe something real; writes nothing",
    )

    down = sub.add_parser("download", parents=[common], help="platform -> Ossie file")
    down.add_argument("name", help="name of the model to download")
    down.add_argument("-o", "--output", help="write the Ossie YAML here (default: stdout)")
    return parser


def _models(model_arg):
    """The models to upload: one file, or every model in a folder.

    Returns (name, path) pairs; `name` is None for a single file so the connection
    falls back to the model's own name or an explicit --name.
    """
    path = Path(model_arg)
    if not path.is_dir():
        return [(None, path)]
    from .models import Models

    found = Models(path)
    if not found:
        raise ValueError(f"no Ossie models found in {path}")
    return list(found.items())


def _connect(args) -> "Connection":
    if args.target == "fabric":
        from .fabric import Fabric

        return Fabric.from_env(
            workspace=args.workspace, lakehouse=args.lakehouse, schema=args.schema
        )
    if args.target == "snowflake":
        from .snowflake import Snowflake

        return Snowflake.from_env(database=args.database, schema=args.schema)
    from .databricks import Databricks

    return Databricks.from_env(
        catalog=args.catalog, schema=args.schema, warehouse_id=args.warehouse_id
    )


def _upload(connection, targets, args) -> int:
    """Upload each target, reporting per model.

    A failure does not stop the rest: uploads are idempotent, so seeing every problem at
    once and re-running after fixing them is better than one failure per run.
    """
    failed = []
    for index, (_name, path) in enumerate(targets):
        if args.dry_run:
            if index:
                print()
            if len(targets) > 1:
                print(f"--- {path.name} ---")
            print(connection.preview(path, name=args.name, warn=args.warnings))
            continue
        try:
            where = connection.upload(path, name=args.name, warn=args.warnings)
            print(f"Uploaded {path} -> {where}")
        except (ValueError, TypeError, OSError, RuntimeError) as exc:
            print(f"Error: {path}: {exc}", file=sys.stderr)
            failed.append(path)

    if failed:
        print(f"{len(failed)} of {len(targets)} model(s) failed", file=sys.stderr)
        return 1
    return 0


def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.version:
        from . import __version__

        print(__version__)
        return 0
    if not args.command:
        parser.print_help()
        return 1

    load_env()
    plain_warnings()
    try:
        connection = _connect(args)

        if args.command == "check":
            findings = connection.check()
            for finding in findings:
                print(f"{finding.level}: {finding.message}", file=sys.stderr)
            if not findings:
                print(f"{connection.platform} {connection.target}: ready")
            return 1 if any(f.fatal for f in findings) else 0

        if args.command == "upload":
            targets = _models(args.model)
            if args.name and len(targets) > 1:
                print("Error: --name cannot be used with a folder of models",
                      file=sys.stderr)
                return 1
            return _upload(connection, targets, args)

        if not isinstance(connection, SupportsDownload):
            print(
                f"Error: {args.target} cannot be downloaded from - "
                "apache-ossie-snowflake ships no Snowflake-to-Ossie converter",
                file=sys.stderr,
            )
            return 1
        ossie_yaml = connection.download(args.name, args.output, warn=args.warnings)
        if args.output:
            print(f"Downloaded {args.name} -> {args.output}")
        else:
            sys.stdout.write(ossie_yaml)
        return 0
    except (ValueError, TypeError, OSError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
