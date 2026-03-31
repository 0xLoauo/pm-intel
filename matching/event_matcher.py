"""Fuzzy-match Polymarket events to Kalshi events.
Uses keyword/entity extraction + text similarity with strict validation."""
import re
from difflib import SequenceMatcher


# Specific entities (high signal — matching these strongly suggests same event)
SPECIFIC_ENTITIES = [
    # People (first + last name patterns)
    r"\b(trump|biden|harris|desantis|newsom|musk|elon musk|xi jinping|putin|zelensky|netanyahu)\b",
    r"\b(taylor swift|rihanna|drake|carti|beyonce|kanye|ye)\b",
    r"\b(openai|anthropic|spacex|coinbase|stripe)\b",
]

# Category entities (low signal — these are too broad to match alone)
CATEGORY_ENTITIES = [
    r"\b(recession|gdp|inflation|unemployment)\b",
    r"\b(interest rate|fed rate|fed cut|fed hike|federal reserve)\b",
    r"\b(bitcoin|btc|ethereum|eth|solana|crypto)\b",
    r"\b(ceasefire|ukraine|russia|iran|china|taiwan|north korea)\b",
    r"\b(election|president|governor|senate|congress|pope)\b",
    r"\b(ipo|stock market)\b",
    r"\b(earthquake|hurricane)\b",
    r"\b(nba|nfl|mlb|nhl|super bowl|world cup|oscar|grammy|emmy)\b",
    r"\b(james bond|gta|marvel|disney)\b",
]


def extract_specific_entities(text: str) -> set:
    """Extract high-signal specific entities (people, companies)."""
    text_lower = text.lower()
    entities = set()
    for pattern in SPECIFIC_ENTITIES:
        matches = re.findall(pattern, text_lower)
        entities.update(matches)
    return entities


def extract_category_entities(text: str) -> set:
    """Extract low-signal category entities (topics, sports leagues)."""
    text_lower = text.lower()
    entities = set()
    for pattern in CATEGORY_ENTITIES:
        matches = re.findall(pattern, text_lower)
        entities.update(matches)
    return entities


def extract_entities(text: str) -> set:
    """Extract all entities (for backward compatibility)."""
    return extract_specific_entities(text) | extract_category_entities(text)


def normalize_question(text: str) -> str:
    text = text.lower().strip()
    # Remove common filler
    for word in ["will ", "the ", "a ", "an ", "be ", "in ", "of ", "to ",
                 "by ", "on ", "for ", "at ", "before ", "after "]:
        text = text.replace(word, " ")
    text = re.sub(r"[^a-z0-9\s]", "", text)
    # Normalize numbers: 100k -> 100000, 1m -> 1000000
    text = re.sub(r"(\d+)k\b", lambda m: str(int(m.group(1)) * 1000), text)
    text = re.sub(r"(\d+)m\b", lambda m: str(int(m.group(1)) * 1000000), text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def text_similarity(a: str, b: str) -> float:
    """Pure text similarity without entity boosting."""
    na = normalize_question(a)
    nb = normalize_question(b)

    # SequenceMatcher
    seq = SequenceMatcher(None, na, nb).ratio()

    # Token Jaccard
    ta = set(na.split())
    tb = set(nb.split())
    jaccard = len(ta & tb) / len(ta | tb) if ta | tb else 0

    return 0.4 * seq + 0.6 * jaccard


def similarity(a: str, b: str) -> float:
    """Compute match score between two event questions.

    Matching rules (designed to eliminate false positives):
    1. If text similarity > 0.70 → match (questions are nearly identical)
    2. If 2+ specific entities match → match (same people/companies)
    3. If 1 specific entity + 1 category entity match → match (same person + same topic)
    4. If 1 category entity only → NOT a match (too generic, e.g. both mention "nba")
    5. If text similarity < 0.35 → never match regardless of entities
    """
    text_sim = text_similarity(a, b)

    # Rule 5: floor — if text is very different, no match
    if text_sim < 0.35:
        return text_sim

    # Rule 1: high text similarity alone is sufficient
    if text_sim >= 0.70:
        return text_sim

    # Entity analysis
    specific_a = extract_specific_entities(a)
    specific_b = extract_specific_entities(b)
    category_a = extract_category_entities(a)
    category_b = extract_category_entities(b)

    specific_overlap = specific_a & specific_b
    category_overlap = category_a & category_b

    # Rule 2: 2+ specific entities match
    if len(specific_overlap) >= 2:
        return max(0.75, text_sim)

    # Rule 3: 1 specific + 1 category
    if len(specific_overlap) >= 1 and len(category_overlap) >= 1:
        return max(0.65, text_sim)

    # Rule 3b: 1 specific entity + decent text similarity
    if len(specific_overlap) >= 1 and text_sim >= 0.50:
        return max(0.60, text_sim)

    # Rule 4: category-only overlap needs strong text support
    # "NBA MVP" vs "NBA franchise" — 1 category entity, low text sim → rejected
    # "bitcoin hit 100k" vs "bitcoin above 100000" — 1 category + decent text → allowed
    if len(category_overlap) >= 2 and text_sim >= 0.45:
        return max(0.60, text_sim)
    if len(category_overlap) >= 1 and text_sim >= 0.55:
        return text_sim

    # Default: just text similarity (will be below threshold for most non-matches)
    return text_sim


def match_events(polymarket_markets: list, kalshi_markets: list, threshold: float = 0.55) -> list:
    """Find matching events across platforms.

    Higher default threshold (0.55 vs old 0.35) to reduce false positives.
    """
    matches = []

    for pm in polymarket_markets:
        pm_question = pm.get("question", "")
        if not pm_question:
            continue

        best_match = None
        best_score = 0

        for km in kalshi_markets:
            km_title = km.get("title", km.get("_event_title", ""))
            if not km_title:
                continue
            score = similarity(pm_question, km_title)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = km

        if best_match:
            all_entities = extract_entities(pm_question) & extract_entities(best_match.get("title", ""))
            matches.append({
                "polymarket": pm,
                "kalshi": best_match,
                "similarity": round(best_score, 3),
                "matched_entities": list(all_entities),
            })

    return sorted(matches, key=lambda x: x["similarity"], reverse=True)
