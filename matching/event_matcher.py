"""Fuzzy-match Polymarket events to Kalshi events.
Uses keyword/entity extraction + text similarity with strict validation.

v2: Fixed normalizer (word-boundary aware), added blocking for O(n) matching,
    added rapidfuzz if available, pre-computed normalization caching.
"""
import re
from difflib import SequenceMatcher

# Try rapidfuzz for 10-100x faster matching
try:
    from rapidfuzz.fuzz import ratio as rapid_ratio
    HAS_RAPIDFUZZ = True
except ImportError:
    HAS_RAPIDFUZZ = False


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

# Filler words to remove — ONLY as whole words, never substrings
_FILLER_WORDS = {
    "will", "the", "a", "an", "be", "in", "of", "to",
    "by", "on", "for", "at", "before", "after", "is", "are",
    "was", "were", "has", "have", "been", "do", "does", "did",
    "this", "that", "these", "those", "it", "its",
    "next", "new", "announced", "as",
}

# Pre-compiled filler word regex (matches whole words only)
_FILLER_PATTERN = re.compile(
    r'\b(' + '|'.join(re.escape(w) for w in _FILLER_WORDS) + r')\b',
    re.IGNORECASE
)


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
    """Normalize a question for comparison.

    v2: Uses word-boundary-aware removal instead of str.replace().
    Old code: "bitcoin" -> "bitco" (stripped "in" substring). Fixed.
    """
    text = text.lower().strip()
    # Remove filler words using word-boundary regex (not substring replace!)
    text = _FILLER_PATTERN.sub(' ', text)
    # Remove punctuation
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

    if HAS_RAPIDFUZZ:
        seq = rapid_ratio(na, nb) / 100.0  # rapidfuzz returns 0-100
    else:
        seq = SequenceMatcher(None, na, nb).ratio()

    # Token Jaccard
    ta = set(na.split())
    tb = set(nb.split())
    jaccard = len(ta & tb) / len(ta | tb) if ta | tb else 0

    return 0.4 * seq + 0.6 * jaccard


def _has_semantic_conflict(a: str, b: str) -> bool:
    """Detect when two questions are about fundamentally different things
    even though they share entities.

    E.g., "finish 3rd" vs "finish top 6" — same team, different question.
    E.g., "will X win" vs "will X run for" — different actions.
    """
    al = a.lower()
    bl = b.lower()

    # Position-specific vs range: "finish in Xth place" vs "finish in top Y"
    has_position_a = bool(re.search(r'(\d+)(st|nd|rd|th)\s+place', al)) or 'last place' in al
    has_top_a = bool(re.search(r'top\s+\d+', al))
    has_position_b = bool(re.search(r'(\d+)(st|nd|rd|th)\s+place', bl)) or 'last place' in bl
    has_top_b = bool(re.search(r'top\s+\d+', bl))
    if (has_position_a and has_top_b) or (has_position_b and has_top_a):
        return True
    # Different positions: "finish 2nd" vs "finish 3rd"
    pos_a = re.findall(r'(\d+)(st|nd|rd|th)\s+place', al)
    pos_b = re.findall(r'(\d+)(st|nd|rd|th)\s+place', bl)
    if pos_a and pos_b and pos_a[0][0] != pos_b[0][0]:
        return True

    # "finish Xth" vs "win" — finishing in a specific position is not the same as winning
    has_finish_pos_a = bool(re.search(r'finish\s+(in\s+)?\d+(st|nd|rd|th)', al)) or 'last place' in al
    has_finish_pos_b = bool(re.search(r'finish\s+(in\s+)?\d+(st|nd|rd|th)', bl)) or 'last place' in bl
    has_win_a = bool(re.search(r'\bwin\b', al))
    has_win_b = bool(re.search(r'\bwin\b', bl))
    if (has_finish_pos_a and has_win_b and not has_finish_pos_b):
        return True
    if (has_finish_pos_b and has_win_a and not has_finish_pos_a):
        return True

    # "win" vs "run for" — different actions on same event
    if ('win' in al and 'run for' in bl) or ('run for' in al and 'win' in bl):
        return True

    # "win the X" vs "win the Y" where X and Y are different conferences/leagues
    nba_a = 'nba' in al
    nba_b = 'nba' in bl
    mls_a = 'mls' in al
    mls_b = 'mls' in bl
    if (nba_a and mls_b) or (mls_a and nba_b):
        return True

    # "Eastern Conference" vs "Western Conference"
    east_a = 'eastern' in al
    east_b = 'eastern' in bl
    west_a = 'western' in al
    west_b = 'western' in bl
    if (east_a and west_b) or (west_a and east_b):
        return True

    # "win X" vs "win X MVP" — winning the event vs winning MVP are different
    mvp_a = 'mvp' in al
    mvp_b = 'mvp' in bl
    if mvp_a != mvp_b:
        return True

    # "Champions League" vs "Women's Champions League" — different competitions
    womens_a = "women" in al
    womens_b = "women" in bl
    if womens_a != womens_b:
        return True

    # "Serie A" vs "Premier League" vs "La Liga" — different leagues
    leagues = ['serie a', 'premier league', 'la liga', 'bundesliga', 'ligue 1', 'champions league']
    leagues_a = {l for l in leagues if l in al}
    leagues_b = {l for l in leagues if l in bl}
    if leagues_a and leagues_b and not leagues_a.intersection(leagues_b):
        return True

    # Different people in same-structure question (e.g., different Colombian candidates)
    # Extract personal names — if both have names and they don't overlap, reject
    # This is handled by entity matching, but we need to prevent "Will X win Y" matching "Will Z win Y"
    # where X != Z. Check if both questions have a person name in similar position.
    name_pattern = r'(?:will\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)'
    names_a = set(re.findall(name_pattern, a))  # Use original case
    names_b = set(re.findall(name_pattern, b))
    if names_a and names_b and not names_a.intersection(names_b):
        # Both have proper names but no overlap — different people
        # Only reject if the questions are structurally similar (same event, different person)
        # Check if removing names makes them very similar
        stripped_a = re.sub(name_pattern, 'PERSON', a.lower())
        stripped_b = re.sub(name_pattern, 'PERSON', b.lower())
        if SequenceMatcher(None, stripped_a, stripped_b).ratio() > 0.80:
            return True

    # "win election" vs "declare for/contest election" — different questions
    declare_words = ['declare', 'contest', 'run for', 'founded by', 'announce']
    has_declare_a = any(w in al for w in declare_words)
    has_declare_b = any(w in al for w in declare_words)
    if has_win_a and any(w in bl for w in declare_words):
        return True
    if has_win_b and any(w in al for w in declare_words):
        return True

    # "next prime minister be X" vs "X leave prime minister" — opposite events
    if ('leave' in al and 'leave' not in bl) or ('leave' in bl and 'leave' not in al):
        if 'next' in al or 'next' in bl:
            return True

    return False


def similarity(a: str, b: str) -> float:
    """Compute match score between two event questions.

    Matching rules (designed to eliminate false positives):
    1. If text similarity > 0.70 -> match (questions are nearly identical)
    2. If 2+ specific entities match -> match (same people/companies)
    3. If 1 specific entity + 1 category entity match -> match (same person + same topic)
    4. If 1 category entity only -> NOT a match (too generic, e.g. both mention "nba")
    5. If text similarity < 0.35 -> never match regardless of entities
    6. If semantic conflict detected -> never match
    """
    # Check for semantic conflicts first
    if _has_semantic_conflict(a, b):
        return 0.0

    text_sim = text_similarity(a, b)

    # Rule 5: floor
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
    if len(category_overlap) >= 2 and text_sim >= 0.45:
        return max(0.60, text_sim)
    if len(category_overlap) >= 1 and text_sim >= 0.55:
        return text_sim

    return text_sim


def _extract_blocking_keys(text: str) -> set:
    """Extract blocking keys for candidate filtering.
    Two questions must share at least one blocking key to be compared.
    This reduces O(n*m) to O(n+m) for most pairs.
    """
    keys = set()
    # All entities are blocking keys
    keys.update(extract_specific_entities(text))
    keys.update(extract_category_entities(text))
    # Also add significant tokens (5+ chars, not filler)
    normalized = normalize_question(text)
    for token in normalized.split():
        if len(token) >= 5:  # Only meaningful words
            keys.add(token)
    return keys


def match_events(polymarket_markets: list, kalshi_markets: list, threshold: float = 0.55) -> list:
    """Find matching events across platforms.

    v2: Uses blocking keys to reduce candidate pairs from O(n*m) to ~O(n+m).
    Only pairs that share at least one blocking key are compared.
    """
    matches = []

    # Pre-compute blocking keys and normalized questions for Kalshi
    km_index = {}  # blocking_key -> list of kalshi markets
    km_data = {}   # id(market) -> {"title": str, "keys": set}

    for km in kalshi_markets:
        title = km.get("title", km.get("_event_title", ""))
        if not title:
            continue
        keys = _extract_blocking_keys(title)
        km_data[id(km)] = {"title": title, "keys": keys}
        for key in keys:
            if key not in km_index:
                km_index[key] = []
            km_index[key].append(km)

    for pm in polymarket_markets:
        pm_question = pm.get("question", "")
        if not pm_question:
            continue

        pm_keys = _extract_blocking_keys(pm_question)

        # Find candidate Kalshi markets that share at least one blocking key
        candidates = set()
        for key in pm_keys:
            for km in km_index.get(key, []):
                candidates.add(id(km))

        best_match = None
        best_score = 0

        for km in kalshi_markets:
            if id(km) not in candidates:
                continue
            km_title = km_data.get(id(km), {}).get("title", "")
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
