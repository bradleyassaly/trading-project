from __future__ import annotations

from trading_platform.cli.parser import build_parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        raise SystemExit(2)

    args.func(args)


if __name__ == "__main__":
    main()