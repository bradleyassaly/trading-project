from __future__ import annotations

from types import SimpleNamespace

from trading_platform.cli.commands.paper_run_scheduled import cmd_paper_run_scheduled


def test_cmd_paper_run_scheduled_calls_paper_run(monkeypatch, capsys) -> None:
    captured: dict[str, object] = {}

    def fake_cmd_paper_run(args) -> None:
        captured["args"] = args
        print("paper run invoked")

    monkeypatch.setattr(
        "trading_platform.cli.commands.paper_run_scheduled.cmd_paper_run",
        fake_cmd_paper_run,
    )

    args = SimpleNamespace(preset="xsec_nasdaq100_momentum_v1_deploy")
    cmd_paper_run_scheduled(args)

    stdout = capsys.readouterr().out
    assert "Starting scheduled paper preset run" in stdout
    assert "paper run invoked" in stdout
    assert "Scheduled paper preset run completed" in stdout
    assert captured["args"] is args
