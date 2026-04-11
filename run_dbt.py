#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run dbt with a selectable engine target (normal or fusion)."
    )
    parser.add_argument(
        "--engine",
        choices=["normal", "fusion"],
        default="normal",
        help="Select the dbt execution engine target.",
    )
    parser.add_argument(
        "--profiles-dir",
        default="coffee_analytics",
        help="The dbt profiles directory to use.",
    )
    parser.add_argument(
        "--project-dir",
        default="coffee_analytics",
        help="The dbt project directory to use.",
    )
    parser.add_argument(
        "dbt_args",
        nargs=argparse.REMAINDER,
        help="The dbt command and arguments to execute.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.engine == "normal":
        target = "dev"
    elif args.engine == "fusion":
        target = "fusion"

    if not args.dbt_args:
        dbt_args = ["run"]
    else:
        dbt_args = args.dbt_args[:]
        if dbt_args[0] == "--":
            dbt_args = dbt_args[1:]

    if any(arg.startswith("--target") for arg in dbt_args):
        print(
            "ERROR: Do not pass --target manually. Use --engine normal|fusion instead."
        )
        sys.exit(1)

    dbt_executable = os.path.join(os.path.dirname(sys.executable), "dbt")
    cmd = [
        dbt_executable,
        *dbt_args,
        "--profiles-dir",
        args.profiles_dir,
        "--project-dir",
        args.project_dir,
        "--target",
        target,
    ]
    print("Running dbt command:", " ".join(cmd))
    return_code = subprocess.call(cmd)
    sys.exit(return_code)


if __name__ == "__main__":
    main()
