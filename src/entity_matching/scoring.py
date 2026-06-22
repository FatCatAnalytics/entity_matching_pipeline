"""Reusable scoring helpers for candidate entity matches."""

from difflib import SequenceMatcher


def ratio(left: str | None, right: str | None) -> float:
    """Sequence similarity between two strings."""
    if not left or not right:
        return 0.0
    return float(SequenceMatcher(None, left, right).ratio())


def token_overlap(left: str | None, right: str | None) -> float:
    """Jaccard token overlap between two strings."""
    if not left or not right:
        return 0.0

    left_tokens = set(left.split())
    right_tokens = set(right.split())

    if not left_tokens or not right_tokens:
        return 0.0

    return float(len(left_tokens & right_tokens) / len(left_tokens | right_tokens))


def field_match_score(client_value: str | None, candidate_value: str | None) -> float:
    """Score an optional exact-match field.

    Missing client values are neutral because the client did not provide the evidence.
    """
    if not client_value:
        return 0.5
    if not candidate_value:
        return 0.3
    return 1.0 if client_value == candidate_value else 0.0


def address_score(client_address: str | None, candidate_address: str | None) -> float:
    """Weak supporting score based on address token overlap."""
    if not client_address:
        return 0.5
    if not candidate_address:
        return 0.3

    client_tokens = set(client_address.split())
    candidate_tokens = set(candidate_address.split())

    if not client_tokens or not candidate_tokens:
        return 0.3

    return float(len(client_tokens & candidate_tokens) / len(client_tokens | candidate_tokens))


def score_candidate(
    client_clean_name: str | None,
    candidate_clean_name: str | None,
    client_country: str | None,
    candidate_country: str | None,
    client_registration_number: str | None,
    candidate_registration_number: str | None,
    client_lei: str | None,
    candidate_lei: str | None,
    client_address: str | None,
    candidate_address: str | None,
    candidate_method: str | None,
) -> float:
    """Return a composite score for a candidate entity match."""
    if client_lei and candidate_lei and client_lei == candidate_lei:
        return 0.99

    if client_registration_number and candidate_registration_number and client_registration_number == candidate_registration_number:
        name_s = ratio(client_clean_name, candidate_clean_name)
        country_s = field_match_score(client_country, candidate_country)
        return round(0.80 + 0.10 * name_s + 0.10 * country_s, 4)

    name_s = ratio(client_clean_name, candidate_clean_name)
    token_s = token_overlap(client_clean_name, candidate_clean_name)
    country_s = field_match_score(client_country, candidate_country)
    address_s = address_score(client_address, candidate_address)

    total = 0.60 * name_s + 0.20 * token_s + 0.15 * country_s + 0.05 * address_s

    if candidate_method == "NAME_COUNTRY_BLOCK":
        total += 0.03

    return round(min(float(total), 0.98), 4)


def confidence_band(score: float | None) -> str:
    """Convert a numeric score into an operational confidence band."""
    if score is None:
        return "NO_MATCH"
    if score >= 0.90:
        return "AUTO_MATCH"
    if score >= 0.75:
        return "REVIEW"
    return "LOW_CONFIDENCE"
