from __future__ import annotations

from trading_platform.db.services import build_research_memory_service


def _build_service(args):
    return build_research_memory_service(
        enable_database_metadata=getattr(args, "enable_database_metadata", None),
        database_url=getattr(args, "database_url", None),
        database_schema=getattr(args, "database_schema", None),
        write_candidates=bool(getattr(args, "tracking_write_candidates", True)),
        write_metrics=bool(getattr(args, "tracking_write_metrics", True)),
        write_promotions=bool(getattr(args, "tracking_write_promotions", True)),
    )


def _print_rows(rows: list[dict[str, object]]) -> None:
    if not rows:
        print("No rows found.")
        return
    columns = list(rows[0].keys())
    widths = {
        column: max(len(column), max(len(str(row.get(column, ""))) for row in rows))
        for column in columns
    }
    header = "  ".join(column.ljust(widths[column]) for column in columns)
    print(header)
    print("  ".join("-" * widths[column] for column in columns))
    for row in rows:
        print("  ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns))


def cmd_research_db_init(args) -> None:
    service = _build_service(args)
    initialized = service.init_schema(schema_name=getattr(args, "database_schema", None))
    if not initialized:
        print("Database tracking is disabled; schema init skipped.")
        return
    print("Research DB schema initialized.")


def cmd_research_db_list_runs(args) -> None:
    service = _build_service(args)
    _print_rows(service.list_recent_runs(limit=int(getattr(args, "limit", 20) or 20)))


def cmd_research_db_top_candidates(args) -> None:
    service = _build_service(args)
    _print_rows(
        service.top_candidates(
            metric=str(getattr(args, "metric", "mean_spearman_ic")),
            limit=int(getattr(args, "limit", 20) or 20),
        )
    )


def cmd_research_db_family_summary(args) -> None:
    service = _build_service(args)
    _print_rows(service.family_summary())


def cmd_research_db_promotions(args) -> None:
    service = _build_service(args)
    _print_rows(service.list_promotions(limit=int(getattr(args, "limit", 20) or 20)))
