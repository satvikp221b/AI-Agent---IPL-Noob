# Router.py — robust router (The most basic of LLM building block ) 
import re
from typing import Optional, Dict, Any
from rapidfuzz import fuzz

# Short team tags 
TEAM_SHORTS = {
    "CSK":"CSK","MI":"MI","RCB":"RCB","KKR":"KKR","SRH":"SRH","DC":"DC","DD":"DD","KXIP":"KXIP",
    "PBKS":"PBKS","RR":"RR","RPS":"RPS","GL":"GL","KTK":"KTK","PWI":"PWI",
}

# Full team names
TEAM_FULL = {t.lower() for t in [
    "Chennai Super Kings","Mumbai Indians","Royal Challengers Bangalore","Kolkata Knight Riders",
    "Sunrisers Hyderabad","Delhi Capitals","Deccan Chargers","Kings XI Punjab","Punjab Kings",
    "Rajasthan Royals","Rising Pune Supergiant","Rising Pune Supergiants","Gujarat Lions",
    "Kochi Tuskers Kerala","Pune Warriors India",
]}

def is_team_token(tok: str) :
    """Return whether the team identified in the prompt is present in the roster"""
    u = tok.strip().upper()
    if u in TEAM_SHORTS: return True
    return tok.strip().lower() in TEAM_FULL

VS_WORDS = r"(?:vs|v\.?|versus|against)"
BETWEEN_WORDS = r"(?:between)"
SQUAD_WORDS = r"(?:squad|roster|line[-\s]?up|team list|players list)"
SUMMARY_WORDS = r"(?:summary|recap|result|tell me about|what happened|match report|scorecard)"
H2H_WORDS = r"(?:head\s*to\s*head|h2h|compare|comparison)"
STATS_WORDS = r"(?:stats?|statistics|figures|numbers|record|profile)"

INTENT_KEYWORDS = {
    "match_summary": ["summary","recap","result","what happened","match report","scorecard"],
    "team_squad":    ["squad","roster","lineup","line-up","team list","players list"],
    "player_stats":  ["stats","statistics","figures","record","profile"],
    "player_vs_team":["vs","against"],
    "head_to_head":  ["head to head","h2h","compare","comparison","versus","vs"],
    "best_phase_bowler": ["best","top","death","powerplay","power play","middle overs","slog","end overs"],
}

PHASE_ALIASES = {
    "pp": "PP",
    "powerplay": "PP",
    "power play": "PP",
    "power-play": "PP",
    "middle": "Middle",
    "middle overs": "Middle",
    "middles": "Middle",
    "death": "Death",
    "slog": "Death",
    "end overs": "Death",
}

# Strip leading intent words
INTENT_PREFIX = re.compile(
    r"""^\s*(?:show|tell|give|get|display|list|compare|comparison|
            head\s*to\s*head|h2h|versus|vs|v\.?|
            summary(?:\s+of)?|what\s+happened|match\s+report|result(?:\s+of)?|scorecard(?:\s+of)?)\b[:,\-\s]*""",
    re.IGNORECASE | re.VERBOSE,
)

def strip_intent_prefix(q: str):
    """Strip intent words from start to lower the chance of them getting detected as players"""
    prev = q
    while True:
        new = INTENT_PREFIX.sub("", prev, count=1)
        if new == prev:
            return re.sub(r"\s+", " ", new.strip())
        prev = new

def clean_space(s: str):
    """Strip spaces from the prompt if any at start or end"""
    return re.sub(r"\s+", " ", (s or "").strip())

def normalize_team_token(tok: str):
    """Normalize the team token provided in prompt"""
    u = re.sub(r"\s+", " ", tok.strip().upper())
    u = re.sub(r"\b(?:IN|FOR)\s+20\d{2}(?:/\d{2})?\b", "", u).strip()
    # Fallback to title-case string
    return TEAM_SHORTS.get(u, tok.strip().title())

def score_intent(text: str):
    """Detect intend words to identify the type of function the user wants to perform using fuzzy match"""
    t = text.lower()
    scores = {}
    for intent, keys in INTENT_KEYWORDS.items():
        best = 0
        for k in keys:
            best = max(best, fuzz.partial_ratio(t, k))
        scores[intent] = best
    return scores

def parse_season(q: str):
    """Get the season number out of the prompt"""
    m = re.search(r"\b(20\d{2})(?:/\d{2})?\b", q)
    return m.group(0) if m else None

def parse_nth(q: str, default: int = 1):
    """Get the specific match out of the prompt for example first or second match between teams of the season"""
    s = q.lower()
    ordmap = {"first":1,"second":2,"third":3,"fourth":4,"fifth":5}
    for w,n in ordmap.items():
        if re.search(rf"\b{w}\b", s): return n
    m = re.search(r"\b(\d+)(st|nd|rd|th)\b", s)
    if m: return int(m.group(1))
    nums = [int(x) for x in re.findall(r"\b(\d{1,2})\b", s)]
    small = [n for n in nums if 1 <= n <= 10]
    return small[0] if small else default

def detect_phase(q: str):
    """Detect the phase for bowling""" 
    low = q.lower()
    for k, v in PHASE_ALIASES.items():
        if re.search(rf"\b{k}\b", low): return v
    return None

# Entity Extractors 
def extract_teams_pair(q: str):
    """Accepts: 'A vs B', 'between A and B', and 'A & B' (when both look like teams)."""
    qs = strip_intent_prefix(q).replace("&", " and ")

    m = re.search(rf"\b([a-z .&/]+?)\s+{VS_WORDS}\s+([a-z .&/]+?)\b", qs, re.IGNORECASE)
    if m:
        a, b = clean_space(m.group(1)), clean_space(m.group(2))
        b = re.sub(r"\b(?:in|for)\s+20\d{2}(?:/\d{2})?\b.*$", "", b, flags=re.IGNORECASE).strip()
        return a, b

    # 'between A and B' anywhere in the string (not just at start)
    m = re.search(rf"{BETWEEN_WORDS}\s+([a-z .&/]+?)\s+(?:and)\s+([a-z .&/]+?)\b", qs, re.IGNORECASE)
    if m:
        return clean_space(m.group(1)), clean_space(m.group(2))

    # 'A and B' without vs/between — only if both look like team tokens
    m = re.search(rf"\b([a-z .&/]+?)\s+(?:and)\s+([a-z .&/]+?)\b", qs, re.IGNORECASE)
    if m:
        a, b = clean_space(m.group(1)), clean_space(m.group(2))
        if (is_team_token(a) or a.upper() in TEAM_SHORTS) and (is_team_token(b) or b.upper() in TEAM_SHORTS):
            return a, b

    return None

def extract_player_vs_team(q: str):
    """Detect if the prompt if asking for a players performance against a certain team"""
    qs = strip_intent_prefix(q).replace("&", " and ")
    
    m = re.search(rf"\b([a-z .]+?)\s+{VS_WORDS}\s+([a-z .&]+?)\b", qs, re.IGNORECASE) \
        or re.search(rf"\b([a-z .]+?)\s+(?:against)\s+([a-z .&]+?)\b", qs, re.IGNORECASE)
    if not m:
        return None
    left, right = clean_space(m.group(1)), clean_space(m.group(2))
    # Heuristic: left looks like a person (not a known team) and usually has a space (first + last)
    if not is_team_token(left) and left.upper() not in TEAM_SHORTS:
        return left, normalize_team_token(right)
    return None

def extract_team_for_squad(q: str) -> Optional[str]:
    """Detect if asking about the full squad of a IPL for a particular season"""
    qs = strip_intent_prefix(q)
    m = re.search(rf"\b({SQUAD_WORDS})\s+(?:of|for)?\s*([a-z .&]+)", qs, re.IGNORECASE)
    if m: return clean_space(m.group(2))
    m = re.search(rf"\b([a-z .&]+)\s+{SQUAD_WORDS}\b", qs, re.IGNORECASE)
    if m: return clean_space(m.group(1))
    return None

def extract_player_for_stats(q: str) -> Optional[str]:
    qs = strip_intent_prefix(q)
    m = re.search(rf"\b{STATS_WORDS}\b.*?\b([a-z .]+)\b", qs, re.IGNORECASE)
    if m: return clean_space(m.group(1))
    m = re.search(rf"\b([a-z .]+)\b.*?\b{STATS_WORDS}\b", qs, re.IGNORECASE)
    if m: return clean_space(m.group(1))
    return None

# Router (This decides which function does the prompt wants the Agent to perform (Precendence order)) 
def route(query: str) -> Dict[str, Any]:
    q = clean_space(query)
    season = parse_season(q)
    nth = parse_nth(q, 1)
    scores = score_intent(q)

    # Phase leaderboard
    if re.search(r"\b(best|top)\b", q, re.IGNORECASE) and re.search(r"\b(bowler|bowlers)\b", q, re.IGNORECASE):
        phase = detect_phase(q)
        if phase:
            scope = "season" if season else "career"
            return {"intent": "best_phase_bowler", "params": {"phase": phase, "scope": scope, "season": season}}

    tp = extract_teams_pair(q)
    pvt = extract_player_vs_team(q)

    # Match summary gets precedence when there are explicit summary words or match token
    if tp and (re.search(SUMMARY_WORDS, q, re.IGNORECASE) or re.search(r"\bmatch\b", q, re.IGNORECASE)):
        a_raw, b_raw = tp
        return {"intent": "match_summary",
                "params": {"team_a": normalize_team_token(a_raw),
                           "team_b": normalize_team_token(b_raw),
                           "season": season,
                           "nth": nth}}

    # Player vs Team before head-to-head 
    if pvt:
        player, opp = pvt
        return {"intent": "player_vs_team",
                "params": {"player": player, "opponent": opp,
                           "scope": "season" if season else "career", "season": season}}

    # Team squad
    team_for_squad = extract_team_for_squad(q)
    if team_for_squad and (scores["team_squad"] >= 55 or re.search(SQUAD_WORDS, q, re.IGNORECASE)):
        return {"intent": "team_squad",
                "params": {"team": normalize_team_token(team_for_squad), "season": season}}

    # Player stats
    player = extract_player_for_stats(q)
    if player and (scores["player_stats"] >= 55 or re.search(STATS_WORDS, q, re.IGNORECASE)):
        return {"intent": "player_stats",
                "params": {"player": player, "scope": "season" if season else "career", "season": season}}

    # Head-to-head 
    if tp and (scores["head_to_head"] >= 40 or re.search(H2H_WORDS, q, re.IGNORECASE) or
               " vs " in q.lower() or " between " in q.lower() or "&" in q):
        a_raw, b_raw = tp
        return {"intent": "head_to_head",
                "params": {"team_a": normalize_team_token(a_raw),
                           "team_b": normalize_team_token(b_raw),
                           "scope": "season" if season else "career",
                           "season": season}}

    # If matches none
    return {"intent": "unknown", "params": {}}
