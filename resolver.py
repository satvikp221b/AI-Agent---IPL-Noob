"""
@author: satvi
"""

"""Resolver with RapidFuzz for fuzzy matching
 as players mentioned in cricsheet is Initals + Last Name 
 for eg. Virat Kohli is VK Kohli 
 + wrappers around query.py
"""

import re
import duckdb
from typing import Dict, Tuple, Optional, List
import pandas as pd
from rapidfuzz import fuzz, process
import query as base


# Normalization utilities
def safe_list_col(df: pd.DataFrame, col: str):
    """Extract non-null strings from a DataFrame column safely."""
    if df is None or df.empty or col not in df.columns:
        return []
    vals: List[str] = []
    for v in df[col].tolist():
        if v is None or pd.isna(v):
            continue
        vals.append(str(v))
    return vals

def norm(s: str):
    """Normalize whitespace, punctuation, case. Can accomodate dots, hyphens etc."""
    if s is None:
        return ""
    s = s.replace("\xa0", " ")               
    s = re.sub(r"[.\u200d\-]", " ", s)       
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def initials_key(s: str):
    """Construct initials last key: 'Rohit Gurunath Sharma' = 'rg sharma'."""
    if s is None:
        return ""
    s = re.sub(r"[^a-zA-Z ]", " ", s).strip()
    parts = [p for p in s.split() if p]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0].lower()
    initials = "".join(p[0].lower() for p in parts[:-1])
    last = parts[-1].lower()
    return f"{initials} {last}"

def best_fuzzy_match(query: str, candidates: List[str], score_cutoff: int = 85):
    """Return best fuzzy match above score_cutoff (threshold)."""
    if not query or not candidates:
        return None
    result = process.extractOne(query, candidates, scorer=fuzz.partial_ratio, score_cutoff=score_cutoff)
    if result:
        match, score, _ = result
        return match
    return None

# Aliases

TEAM_ALIASES: Dict[str, str] = {
    "csk": "Chennai Super Kings",
    "mi": "Mumbai Indians",
    "rcb": "Royal Challengers Bangalore",
    "kkr": "Kolkata Knight Riders",
    "srh": "Sunrisers Hyderabad",
    "rr": "Rajasthan Royals",
    "dc": "Delhi Capitals",
    "dd": "Delhi Daredevils",
    "kxip": "Kings XI Punjab",
    "pbks": "Punjab Kings",
    "rps": "Rising Pune Supergiant",
    "pwi": "Pune Warriors",
    "gl": "Gujarat Lions",
    "ktk": "Kochi Tuskers Kerala",
    "delhi": "Delhi Capitals",
    "punjab": "Punjab Kings",
    "bangalore": "Royal Challengers Bangalore",
    "bengaluru": "Royal Challengers Bangalore",
    "mumbai":"Mumbai Indians",
    "chennai":"Chennai Super Kings",
}

#Just some common famous ones
PLAYER_ALIASES: Dict[str, str] = {
    "rohit sharma": "RG Sharma",
    "virat kohli": "V Kohli",
    "sachin tendulkar": "SR Tendulkar",
    "sourav ganguly": "SC Ganguly",
    "rahul dravid": "R Dravid",
    "ms dhoni": "MS Dhoni",
    "gautam gambhir": "G Gambhir",
    "ab de villiers": "AB de Villiers",
}

class Resolver:
    """
    Loads canonical teams/players from DuckDB and resolves user input to those.
    Resolution order:
      - manual alias
      - exact normalized match
      - initials key (Player only)
      - substring contains (case-insensitive)
      - RapidFuzz fuzzy match (partial_ratio)
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.teams: List[str] = []
        self.players: List[str] = []
        self.by_norm_team: Dict[str, str] = {}
        self.by_norm_player: Dict[str, str] = {}
        self.by_initials_player: Dict[str, str] = {}
        self.refresh()

    def refresh(self):
        con = duckdb.connect(self.db_path)

        # Teams from both columns
        tdf = con.execute("""
            WITH t AS (
              SELECT team1 AS team FROM matches_meta
              UNION ALL
              SELECT team2 AS team FROM matches_meta
            )
            SELECT DISTINCT team FROM t WHERE team IS NOT NULL
        """).df()
        self.teams = sorted(set(safe_list_col(tdf, "team")))
        self.by_norm_team = { norm(t): t for t in self.teams }

        # Players from striker/bowler
        pdf = con.execute("""
            WITH p AS (
              SELECT striker AS name FROM deliveries
              UNION ALL
              SELECT bowler  AS name FROM deliveries
            )
            SELECT DISTINCT name FROM p WHERE name IS NOT NULL
        """).df()
        self.players = sorted(set(safe_list_col(pdf, "name")))
        self.by_norm_player = { norm(p): p for p in self.players }
        self.by_initials_player = { initials_key(p): p for p in self.players }

        con.close()

    # Team resolution

    def resolve_team(self, user_text: str) -> Optional[str]:
        if not user_text:
            return None
        key = norm(user_text)

        # Manual Aliases
        if key in TEAM_ALIASES:
            alias_target = TEAM_ALIASES[key]
            alias_norm = norm(alias_target)
            # Canonical preference first
            if alias_norm in self.by_norm_team:
                return self.by_norm_team[alias_norm]
            return alias_target 

        # Exact normalized match
        if key in self.by_norm_team:
            return self.by_norm_team[key]

        # substring / contains
        for nk, canon in self.by_norm_team.items():
            if key and nk.find(key) >= 0:
                return canon

        # fuzzy matching
        fuzzy = best_fuzzy_match(user_text, self.teams, score_cutoff=85)
        if fuzzy:
            return fuzzy

        return None

    # Player resolution

    def resolve_player(self, user_text: str) -> Tuple[Optional[str], List[str]]:
        """
        Returns canonical_name or None and possible choices if ambiguous input : Like 'Rohit Sharma': '['RG Sharma','R Sharma']'
        """
        if not user_text:
            return None, []

        key = norm(user_text)

        # Manual alias
        if key in PLAYER_ALIASES:
            target = PLAYER_ALIASES[key]
            # Exact normalized match
            tnorm = norm(target)
            if tnorm in self.by_norm_player:
                return self.by_norm_player[tnorm], []
            # Initials key of the alias target
            ik = initials_key(target)
            if ik in self.by_initials_player:
                return self.by_initials_player[ik], []
            # If none : return alias literal
            return target, []

        # Exact Normalized
        if key in self.by_norm_player:
            return self.by_norm_player[key], []

        # Initials key
        ik = initials_key(user_text)
        if ik in self.by_initials_player:
            return self.by_initials_player[ik], []

        # Contains/substring, may lead to ambiguous choices
        choices: List[str] = []
        for p in self.players:
            if norm(p).find(key) >= 0:
                choices.append(p)
            if len(choices) >= 10:
                break
        if len(choices) == 1:
            return choices[0], []
        if len(choices) > 1:
            return None, choices

        # Fuzzy matching
        fuzzy = best_fuzzy_match(user_text, self.players, score_cutoff=85)
        if fuzzy:
            return fuzzy, []

        # No match
        return None, []



# Wrappers (From query.py)

def match_summary(db_path: str, team_a: str, team_b: str, season: str, nth: int = 1):
    res = Resolver(db_path)
    A = res.resolve_team(team_a) or team_a
    B = res.resolve_team(team_b) or team_b
    return base.match_summary(db_path, A, B, season, nth)

def player_stats(db_path: str, player: str, scope: str = "career", season: Optional[str] = None):
    res = Resolver(db_path)
    canonical, choices = res.resolve_player(player)
    if not canonical and choices:
        return {"error": f"Ambiguous player '{player}'", "choices": choices}
    if not canonical:
        return {"error": f"No appearances for '{player}' in current data."}
    return base.player_stats(db_path, canonical, scope=scope, season=season)

def team_squad(db_path: str, team: str, season: str):
    res = Resolver(db_path)
    T = res.resolve_team(team) or team
    return base.team_squad(db_path, T, season)

def player_vs_team(db_path: str, player: str, opponent: str, scope: str = "career", season: Optional[str] = None):
    res = Resolver(db_path)
    canonical, choices = res.resolve_player(player)
    if not canonical and choices:
        return {"error": f"Ambiguous player '{player}'", "choices": choices}
    if not canonical:
        return {"error": f"No appearances for '{player}' in current data."}
    opp = res.resolve_team(opponent) or opponent
    return base.player_vs_team(db_path, canonical, opp, scope=scope, season=season)

def head_to_head(db_path: str, team_a: str, team_b: str, scope: str = "career", season: Optional[str] = None):
    res = Resolver(db_path)
    A = res.resolve_team(team_a) or team_a
    B = res.resolve_team(team_b) or team_b
    return base.head_to_head(db_path, A, B, scope=scope, season=season)

def best_phase_bowlers(db_path: str, phase: str, scope: str = "career", season: Optional[str] = None, min_overs: int = 30):
    """
    Returns the best bowler of each season or entire career 
    and their stats.
    PP: 0-6 overs, Middle Overs - 7-15 overs, Death:16-20 overs
    """
    con = duckdb.connect(db_path)
    where = "phase = ?"
    params = [phase]
    if scope == "season" and season:
        where += " AND season = ?"
        params.append(season)

    df = con.execute(f"""
        WITH bowl AS (
          SELECT
            bowler,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS legal_balls,
            SUM(runs_total) AS runs_conceded,
            COUNT(CASE WHEN player_dismissed IS NOT NULL THEN 1 END) AS wickets,
            COUNT(DISTINCT match_id) AS matches,
            SUM(CASE WHEN runs_total=0 AND player_dismissed IS NULL THEN 1 ELSE 0 END)::DOUBLE AS dots,
            SUM(CASE WHEN runs_batter IN (4,6) THEN 1 ELSE 0 END)::DOUBLE AS boundaries
          FROM deliveries
          WHERE {where}
          GROUP BY bowler
        ),
        filt AS (
          SELECT *,
            (legal_balls/6) AS overs,
            (runs_conceded * 6.0) / NULLIF(legal_balls,0) AS economy,
            (runs_conceded / NULLIF(wickets,0)) AS average,
            (legal_balls / NULLIF(wickets,0)) AS strike_rate,
            (dots * 100.0) / NULLIF(legal_balls,0) AS dot_pct,
            (boundaries * 100.0) / NULLIF(legal_balls,0) AS boundary_pct
          FROM bowl
          WHERE legal_balls >= ?
        ),
        ranked AS (
          SELECT *,
            ROW_NUMBER() OVER (ORDER BY economy ASC NULLS LAST, average ASC NULLS LAST, strike_rate ASC NULLS LAST, overs DESC, bowler ASC) AS rk
          FROM filt
        )
        SELECT bowler,
               CAST(overs AS DOUBLE) AS overs,
               CAST(wickets AS INTEGER) AS wickets,
               CAST(runs_conceded AS INTEGER) AS runs_conceded,
               ROUND(economy, 2) AS economy,
               ROUND(average, 2) AS average,
               ROUND(strike_rate, 2) AS strike_rate,
               ROUND(dot_pct, 2) AS dot_pct,
               ROUND(boundary_pct, 2) AS boundary_pct,
               CAST(matches AS INTEGER) AS matches
        FROM ranked
        WHERE rk <= 10
    """, params + [min_overs * 6]).df()

    return {
        "input": {"phase": phase, "scope": scope, "season": season, "min_overs": min_overs},
        "leaders": df.to_dict("records")
    }
