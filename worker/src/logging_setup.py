"""Centralised structured logging configuration."""

from __future__ import annotations

import json
import logging
from typing import Any

import structlog
from structlog.types import EventDict, WrappedLogger


def _run_id_prefix_renderer(
    logger: WrappedLogger, method_name: str, event_dict: EventDict
) -> str:
    """Render log line as '<run_id> {json}' for easier scanning.

    Args:
        logger (WrappedLogger): Underlying stdlib logger instance (unused).
        method_name (str): Logging method name (unused).
        event_dict (EventDict): Structlog event payload about to be rendered.

    Returns:
        str: Single-line log entry prefixed with the bound run identifier.
    """
    del logger, method_name  # unused but required by structlog signature
    run_id = event_dict.get("run_id", "-")
    json_payload = json.dumps(event_dict, separators=(",", ":"))
    return f"{run_id} {json_payload}"


def configure_logging(run_id: str) -> None:
    """Configure structlog and stdlib logging for the app.

    Args:
        run_id (str): Unique identifier bound for correlation across log lines.
    """

    timestamper = structlog.processors.TimeStamper(fmt="iso")
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        timestamper,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _run_id_prefix_renderer,  # custom final renderer
    ]

    # Plain message format so our renderer controls the entire line
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=shared_processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(run_id=run_id)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger bound with the provided name.

    Args:
        name (str): Logical logger name (typically module or component).

    Returns:
        structlog.stdlib.BoundLogger: Structured logger ready for event binding.
    """
    return structlog.get_logger(name)
