from __future__ import annotations

import sys

from trading_platform.cli.parser import build_parser, rewrite_legacy_cli_args


def main() -> None:
    argv, deprecation_note = rewrite_legacy_cli_args(sys.argv[1:])
    parser = build_parser()
    args = parser.parse_args(argv)
    args._cli_argv = argv

    if not hasattr(args, "func"):
        parser.print_help()
        raise SystemExit(2)

    if deprecation_note:
        print(f"[DEPRECATED] {deprecation_note}")
    args.func(args)


if __name__ == "__main__":
    main()
