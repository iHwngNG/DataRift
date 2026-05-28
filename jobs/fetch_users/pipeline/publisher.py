"""
publisher.py
~~~~~~~~~~~~
Pub/Sub publisher for the ``fetch_users`` Cloud Run Job.

Design
------
Each call to :meth:`PubSubPublisher.publish_batch` serialises a list of Riot
league-entry dicts to JSON and publishes them as **individual messages** to the
configured Pub/Sub topic.

Publishing individually (rather than as one large message) keeps each Pub/Sub
message well under the 10 MB size limit and lets the ``parquet_writer``
consumer ACK at per-record granularity, which is important for durability.

Message format
~~~~~~~~~~~~~~
* **Data**: JSON-encoded dict of a single league-entry record.
* **Attributes**:
    - ``tier``     — e.g. ``"DIAMOND"``
    - ``division`` — e.g. ``"I"``
    - ``platform`` — e.g. ``"kr"``

Thread safety
~~~~~~~~~~~~~
:class:`google.cloud.pubsub_v1.PublisherClient` is thread-safe; multiple
fetcher threads can share a single :class:`PubSubPublisher` instance.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import Future

from google.cloud import pubsub_v1  # type: ignore[import-untyped]
from google.cloud.pubsub_v1.types import BatchSettings

logger = logging.getLogger(__name__)


class PubSubPublisher:
    """Publishes Riot league-entry records to a Pub/Sub topic.

    Args:
        project_id: GCP project ID (e.g. ``"datarift-dev"``).
        topic_id: Pub/Sub topic ID (e.g. ``"lol-league-entries-dev"``).
        platform: Platform slug injected as a message attribute and into each
            record dict (e.g. ``"kr"``).
        max_messages: Maximum messages to batch before forcing a publish.
        max_bytes: Maximum total bytes in a batch before forcing a publish.
        max_latency: Maximum seconds to wait before flushing a batch.
    """

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        platform: str,
        max_messages: int = 1000,
        max_bytes: int = 5 * 1024 * 1024,  # 5 MB per batch
        max_latency: float = 0.05,
    ) -> None:
        self._platform = platform
        self._topic_path = f"projects/{project_id}/topics/{topic_id}"

        batch_settings = BatchSettings(
            max_messages=max_messages,
            max_bytes=max_bytes,
            max_latency=max_latency,
        )
        self._client = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        self._pending_futures: list[Future] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def publish_batch(
        self, records: list[dict], tier: str, division: str
    ) -> None:
        """Publish a page of league-entry records to the Pub/Sub topic.

        Each record is published as a **separate** Pub/Sub message so that
        the downstream consumer can ACK individually.  The ``platform`` column
        is injected into each record dict before serialisation.

        Args:
            records: Raw dicts from the Riot League Entries API response.
            tier: Rank tier (e.g. ``"DIAMOND"``); stored as a message attribute.
            division: Rank division (e.g. ``"I"``); stored as a message attribute.
        """
        if not records:
            return

        attributes = {
            "tier": tier,
            "division": division,
            "platform": self._platform,
        }

        for rec in records:
            rec["platform"] = self._platform
            data = json.dumps(rec).encode("utf-8")
            future = self._client.publish(
                self._topic_path,
                data=data,
                **attributes,
            )
            self._pending_futures.append(future)

        logger.debug(
            "Queued %d messages for %s/%s → %s",
            len(records),
            tier,
            division,
            self._topic_path,
        )

    def flush(self) -> int:
        """Block until all queued publish futures have resolved.

        Returns:
            The total number of successfully published messages.

        Raises:
            Exception: If any publish future failed.
        """
        if not self._pending_futures:
            return 0

        success_count = 0
        errors: list[Exception] = []

        for future in self._pending_futures:
            try:
                future.result()  # blocks until the message is acknowledged by the server
                success_count += 1
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        self._pending_futures.clear()

        if errors:
            logger.error(
                "Failed to publish %d message(s). First error: %s",
                len(errors),
                errors[0],
            )
            raise errors[0]

        logger.info("Flushed %d published messages to Pub/Sub.", success_count)
        return success_count

    def close(self) -> None:
        """Flush pending messages and shut down the publisher client."""
        self.flush()
        logger.info("PubSubPublisher closed.")
