from __future__ import annotations

from trading_platform.cli.grouped_parser import build_parser, rewrite_legacy_cli_args


def test_grouped_data_ingest_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["data", "ingest", "--symbols", "AAPL"])

    assert args.command_family == "data"
    assert args.data_command == "ingest"
    assert args.symbols == ["AAPL"]


def test_grouped_research_alpha_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["research", "alpha", "--symbols", "AAPL", "--lookbacks", "5"])

    assert args.command_family == "research"
    assert args.research_command == "alpha"
    assert args.symbols == ["AAPL"]
    assert args.lookbacks == [5]


def test_grouped_paper_run_command_parses() -> None:
    parser = build_parser()
    args = parser.parse_args(["paper", "run", "--symbols", "AAPL", "--top-n", "1"])

    assert args.command_family == "paper"
    assert args.paper_command == "run"
    assert args.top_n == 1


def test_legacy_alpha_command_rewrites_cleanly() -> None:
    argv, note = rewrite_legacy_cli_args(["alpha-research", "--symbols", "AAPL"])

    assert argv == ["research", "alpha", "--symbols", "AAPL"]
    assert "research alpha" in str(note)


def test_legacy_flat_research_command_rewrites_to_grouped_run() -> None:
    argv, note = rewrite_legacy_cli_args(["research", "--symbols", "AAPL"])

    assert argv == ["research", "run", "--symbols", "AAPL"]
    assert "research run" in str(note)
