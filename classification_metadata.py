"""Canonical metadata policy for secondary-domain classifications.

Only documented source, decision-source, and classification combinations may
retain confidence, recommended action, justification, and exploit type
metadata. Every other combination is represented by nulls.
"""

from __future__ import annotations

from typing import Any


Metadata = tuple[str | None, int | None, int | None, list[str] | None]


EXPECTED_CLASSIFICATION_METADATA: dict[tuple[str, str, int], Metadata] = {
    ('similarweb', 'similarweb', 2): ('HIGH', 1, 1, ['Referral Cloaking']),
    ('similarweb', 'similarweb', 3): ('MEDIUM', 2, 4, ['Made for Arbitrage']),
    ('ad sniffer', 'similarweb', 2): ('MEDIUM', 2, 2, ['Referral Cloaking']),
    ('ad sniffer', 'similarweb', 3): ('LOW', 5, 3, ['Made for Arbitrage']),
    ('ad sniffer', 'ad sniffer', 2): ('MEDIUM', 2, 2, ['Referral Cloaking']),
    ('ad sniffer', 'ad sniffer', 3): ('LOW', 5, 3, ['Made for Arbitrage']),
    ('domain telemetry', 'domain telemetry', 2): ('LOW', 3, 6, ['Referral Cloaking']),
}

EMPTY_METADATA: Metadata = (None, None, None, None)


def normalize_value(value: Any) -> str:
    """Return a case- and whitespace-insensitive provenance key."""
    if value is None:
        return ''
    return ' '.join(str(value).split()).casefold()


def expected_metadata(source: Any, decision_source: Any, classification: Any) -> Metadata:
    """Return approved metadata for a source/decision/classification triple."""
    try:
        classification_id = int(classification)
    except (TypeError, ValueError):
        return EMPTY_METADATA

    return EXPECTED_CLASSIFICATION_METADATA.get(
        (
            normalize_value(source),
            normalize_value(decision_source),
            classification_id,
        ),
        EMPTY_METADATA,
    )
