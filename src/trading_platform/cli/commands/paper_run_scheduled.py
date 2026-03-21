from __future__ import annotations

from trading_platform.cli.commands.paper_run import cmd_paper_run


def cmd_paper_run_scheduled(args) -> None:
    print("Starting scheduled paper preset run")
    cmd_paper_run(args)
    print("Scheduled paper preset run completed")
