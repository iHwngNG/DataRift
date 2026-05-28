"""
logging_config.py
~~~~~~~~~~~~~~~~~
Structured logging configuration for DataRift.

Controls the log level, output format (plain-text vs. JSON), service name
embedded in every log record, and whether an ISO-8601 timestamp is prepended.

The :func:`configure_logging` function applies this configuration to Python's
root ``logging`` module.  It should be called once at application startup,
before any logger is used.

JSON output is Cloud Logging-compatible and uses the standard severity field
mapping expected by Google Cloud's log ingestion:
    DEBUG    → severity: DEBUG
    INFO     → severity: INFO
    WARNING  → severity: WARNING
    ERROR    → severity: ERROR
    CRITICAL → severity: CRITICAL
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone

_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object.

    Compatible with Google Cloud Logging structured log ingestion.
    """

    def __init__(self, service_name: str, include_timestamp: bool) -> None:
        super().__init__()
        self._service_name = service_name
        self._include_timestamp = include_timestamp

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        payload: dict[str, object] = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "service": self._service_name,
        }
        if self._include_timestamp:
            payload["time"] = (
                datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
            )
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Config dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LoggingConfig:
    """Typed configuration for DataRift structured logging.

    Attributes:
        log_level: Python logging level string, e.g. ``"INFO"``, ``"DEBUG"``.
        json_output: When ``True``, emit logs as JSON (Cloud Logging-friendly).
            When ``False``, use a human-readable plain-text format.
        service_name: Identifies this service in every log record.
        include_timestamp: When ``True``, prepend an ISO-8601 UTC timestamp
            to each log record.
    """

    log_level: str
    json_output: bool
    service_name: str
    include_timestamp: bool

    @classmethod
    def from_env(cls) -> "LoggingConfig":
        """Instantiate :class:`LoggingConfig` from environment variables.

        Returns:
            A fully populated :class:`LoggingConfig` instance.

        Raises:
            ValueError: If ``LOG_LEVEL`` is set to an unrecognised value.
        """
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        if log_level not in _VALID_LOG_LEVELS:
            raise ValueError(
                f"Invalid LOG_LEVEL {log_level!r}. "
                f"Must be one of: {sorted(_VALID_LOG_LEVELS)}"
            )

        json_output_raw = os.getenv("LOG_JSON", "false").lower()
        include_ts_raw = os.getenv("LOG_INCLUDE_TIMESTAMP", "true").lower()

        return cls(
            log_level=log_level,
            json_output=json_output_raw in {"1", "true", "yes"},
            service_name=os.getenv("LOG_SERVICE_NAME", "datarift-ingestor"),
            include_timestamp=include_ts_raw in {"1", "true", "yes"},
        )

    # -----------------------------------------------------------------------
    # Application helper
    # -----------------------------------------------------------------------

    def configure_logging(self) -> None:
        """Apply this configuration to Python's root logging system.

        Should be called **once** at application startup before any logger
        is used.  Installs either a JSON formatter (for Cloud Logging) or a
        plain-text formatter (for local development), and sets the root logger
        level.

        Example::

            from config.logging_config import LoggingConfig

            cfg = LoggingConfig.from_env()
            cfg.configure_logging()

            logger = logging.getLogger(__name__)
            logger.info("Pipeline started.")
        """
        root = logging.getLogger()
        root.setLevel(self.log_level)

        # Avoid adding duplicate handlers if called more than once
        if root.handlers:
            root.handlers.clear()

        handler = logging.StreamHandler()

        if self.json_output:
            handler.setFormatter(
                _JsonFormatter(
                    service_name=self.service_name,
                    include_timestamp=self.include_timestamp,
                )
            )
        else:
            fmt_parts = []
            if self.include_timestamp:
                fmt_parts.append("%(asctime)s")
            fmt_parts += ["%(levelname)-8s", f"[{self.service_name}]", "%(name)s", "%(message)s"]
            handler.setFormatter(logging.Formatter(" | ".join(fmt_parts)))

        root.addHandler(handler)
