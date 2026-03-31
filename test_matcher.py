"""Test the event matcher against known true/false pairs."""
from matching.event_matcher import similarity, extract_specific_entities, extract_category_entities, text_similarity

tests = [
    ("Will Paolo Banchero win the 2025-2026 NBA MVP?", "Will the NBA approve a new franchise before 2030?", False, "Different NBA events"),
    ("Will the Democrats win the 2028 US Presidential Election?", "Will Liberal Democratic win the next U.K. election?", False, "US vs UK election"),
    ("Will Elon Musk win the 2028 Republican presidential nomination?", "Will Elon Musk be a trillionaire before 2028?", False, "Same person diff event"),
    ("Harris Dickinson announced as next James Bond?", "Will Harris Dickinson be the next James Bond?", True, "Same person same event"),
    ("Callum Turner announced as next James Bond?", "Will Callum Turner be the next James Bond?", True, "Same person same event"),
    ("Will bitcoin hit 100000 in 2026?", "Bitcoin above 100K by end of 2026?", True, "Same crypto event"),
    ("Russia-Ukraine Ceasefire in 2026?", "Will there be a Russia-Ukraine ceasefire before 2027?", True, "Same geopolitical event"),
    ("Will Greg Abbott win the 2028 US Presidential Election?", "Will Green win the next U.K. election?", False, "Totally different elections"),
]

passed = 0
for pm_q, km_q, should_match, reason in tests:
    score = similarity(pm_q, km_q)
    txt = text_similarity(pm_q, km_q)
    matched = score >= 0.55
    correct = matched == should_match
    if correct:
        passed += 1
    status = "PASS" if correct else "FAIL"
    expect = "match" if should_match else "no"
    print(f"  {status}  score={score:.3f}  txt={txt:.3f}  expect={expect:5}  {reason}")
    if not correct:
        sa = extract_specific_entities(pm_q)
        sb = extract_specific_entities(km_q)
        ca = extract_category_entities(pm_q)
        cb = extract_category_entities(km_q)
        print(f"    Specific: {sa} vs {sb} overlap={sa & sb}")
        print(f"    Category: {ca} vs {cb} overlap={ca & cb}")

print(f"\n{passed}/{len(tests)} tests passed")
