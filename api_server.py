# Api Server
import os
import glob
import duckdb
import math
from typing import Any, Dict, Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi import Query as FQuery
from router import route
from resolver import match_summary, player_stats, team_squad, player_vs_team, head_to_head, best_phase_bowlers
from formatters import format_answer



# DuckDB auto-detection 
def sanitize_for_json(obj):
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(x) for x in obj]
    return obj

def looks_like_valid_db(path: str):
    """Open DuckDB and check required tables + non-zero rows."""
    if not os.path.exists(path):
        return False
    try:
        con = duckdb.connect(path)
        have_meta = con.execute("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'matches_meta'
        """).fetchone()
        have_delv = con.execute("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'deliveries'
        """).fetchone()
        if not (have_meta and have_delv):
            con.close()
            return False

        cnt_meta = con.execute("SELECT COUNT(*) FROM matches_meta").fetchone()[0]
        cnt_delv = con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
        con.close()
        return (cnt_meta or 0) > 0 and (cnt_delv or 0) > 0
    except Exception:
        return False


def auto_find_db():
    """
    Priority:
      -IPL_DB env var
      -./ipl_data.duckdb in current working dir
      -Any *.duckdb in current working dir
      -Any ipl*.duckdb one level up
      -Any *.duckdb one level up
    Returns absolute path or raises RuntimeError.
    """
    # Env var
    env = os.environ.get("IPL_DB")
    candidates = []
    if env:
        candidates.append(env)

    # Common local names
    candidates += [
        "ipl_data.duckdb",
    ]
    # Any *.duckdb in CWD
    candidates += glob.glob("*.duckdb")

    # Look one level up
    candidates += glob.glob(os.path.join("..", "ipl*.duckdb"))
    candidates += glob.glob(os.path.join("..", "*.duckdb"))

    # Preserving order de-dup
    seen = set()
    uniq = []
    for c in candidates:
        if c not in seen:
            uniq.append(c); seen.add(c)

    # Validate in order
    for cand in uniq:
        abs_path = os.path.abspath(cand)
        if looks_like_valid_db(abs_path):
            return abs_path

    raise RuntimeError(
        "Could not auto-detect a valid DuckDB.\n"
        "Tips:\n"
        "  • Make sure you've run ingest.py to create/populate ipl_data.duckdb\n"
        "  • Or set IPL_DB to the absolute path of your DuckDB file\n"
        "  • Run:  python ingest.py --db ipl_data.duckdb --folder C:\\path\\to\\cricsheet\n"
    )


DB_PATH = auto_find_db()  


# FastAPI app 

app = FastAPI(title="Cricket Insight Agent", version="0.2")

# CORS - Allow browser calls during dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AskIn(BaseModel):
    query: str


def run_intent(intent: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Run Function based on prompts""" 
    if intent == "match_summary":
        season = params.get("season")
        nth = params.get("nth", 1)
        if not season:
            return {"error": "Please specify a season, e.g. 'in 2011'."}
        return match_summary(DB_PATH, params["team_a"], params["team_b"], season, nth)

    if intent == "player_stats":
        scope = params.get("scope", "career")
        return player_stats(DB_PATH, params["player"], scope=scope, season=params.get("season"))

    if intent == "team_squad":
        return team_squad(DB_PATH, params["team"], params["season"])

    if intent == "player_vs_team":
        scope = params.get("scope", "career")
        return player_vs_team(DB_PATH, params["player"], params["opponent"], scope=scope, season=params.get("season"))

    if intent == "head_to_head":
        scope = params.get("scope", "career")
        return head_to_head(DB_PATH, params["team_a"], params["team_b"], scope=scope, season=params.get("season"))
    if intent == "best_phase_bowler":
        phase  = params.get("phase")
        scope  = params.get("scope", "career")
        season = params.get("season")
        min_overs = 10 if scope == "season" else 30
        return best_phase_bowlers(DB_PATH, phase, scope=scope, season=season, min_overs=min_overs)
    
    return {"error": "Unknown intent"}


@app.on_event("startup")
def _verify_db_on_start():
    # Fail if the DB isn't valid
    if not looks_like_valid_db(DB_PATH):
        raise RuntimeError(
            f"DuckDB at {DB_PATH} is missing required tables or has zero rows."
        )


@app.get("/health")
def health():
    """Display status of the API"""
    exists = os.path.exists(DB_PATH)
    status = "ok" if exists else "warn"
    return {"status": status, "db_path": DB_PATH, "db_exists": exists}


@app.get("/dbinfo")
def dbinfo():
    """Displays DB Info"""
    info = {"db_path": DB_PATH, "db_exists": os.path.exists(DB_PATH)}
    if not info["db_exists"]:
        return info
    try:
        con = duckdb.connect(DB_PATH)
        cnt_meta = con.execute("SELECT COUNT(*) FROM matches_meta").fetchone()[0]
        cnt_delv = con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
        # DIsplay some seasons & teams head for verification
        seasons = con.execute("""
            SELECT season, COUNT(*) AS matches
            FROM matches_meta
            GROUP BY season
            ORDER BY season
            LIMIT 10
        """).df().to_dict("records")
        sample = con.execute("""
            SELECT match_id, season, team1, team2, date, winner
            FROM matches_meta
            ORDER BY date NULLS LAST, match_id
            LIMIT 5
        """).df().to_dict("records")
        con.close()
        info.update({
            "matches_meta_rows": int(cnt_meta),
            "deliveries_rows": int(cnt_delv),
            "sample_seasons": seasons,
            "sample_matches": sample,
        })
    except Exception as e:
        info["error"] = str(e)
    return info



@app.get("/debug/h2h")
def debug_h2h(team_a: str = FQuery(...), team_b: str = FQuery(...), season: str = FQuery(...)):
    """
    Show the exact matches_meta rows the server sees for (team_a, team_b, season).
    Use exact team names or short aliases .
    """
    import duckdb
    from resolver import Resolver

    # Normalize
    res = Resolver(DB_PATH)
    A = res.resolve_team(team_a) or team_a
    B = res.resolve_team(team_b) or team_b

    con = duckdb.connect(DB_PATH)
    df = con.execute("""
        SELECT match_id, season, date, team1, team2, winner, venue
        FROM matches_meta
        WHERE season = ?
          AND (
                (team1 = ? AND team2 = ?) OR
                (team1 = ? AND team2 = ?)
              )
        ORDER BY date NULLS LAST, match_id
    """, [season, A, B, B, A]).df()

    #  Distinct Team names in the season
    teams = con.execute("""
        WITH t AS (
          SELECT team1 AS team FROM matches_meta WHERE season = ?
          UNION ALL
          SELECT team2 AS team FROM matches_meta WHERE season = ?
        )
        SELECT DISTINCT team FROM t ORDER BY team
    """, [season, season]).df()["team"].tolist()

    return {
        "input": {"team_a": team_a, "team_b": team_b, "season": season},
        "resolved": {"A": A, "B": B},
        "rows": df.to_dict("records"),
        "season_teams": teams,
        "rowcount": int(df.shape[0]),
    }


@app.get("/debug/player")
def debug_player(name: str = FQuery(...), season: Optional[str] = None):
    """
    See whether the player exists in deliveries as striker or bowler.
    """
    import duckdb
    from resolver import Resolver

    res = Resolver(DB_PATH)
    canonical, choices = res.resolve_player(name)

    con = duckdb.connect(DB_PATH)
    if season:
        df = con.execute("""
           WITH names AS (
             SELECT DISTINCT striker AS who FROM deliveries WHERE season = ? AND striker ILIKE ?
             UNION
             SELECT DISTINCT bowler  AS who FROM deliveries WHERE season = ? AND bowler  ILIKE ?
           )
           SELECT who FROM names ORDER BY who
        """, [season, f"%{name}%", season, f"%{name}%"]).df()
    else:
        df = con.execute("""
           WITH names AS (
             SELECT DISTINCT striker AS who FROM deliveries WHERE striker ILIKE ?
             UNION
             SELECT DISTINCT bowler  AS who FROM deliveries WHERE bowler  ILIKE ?
           )
           SELECT who FROM names ORDER BY who
        """, [f"%{name}%", f"%{name}%"]).df()

    return {
        "input": {"name": name, "season": season},
        "resolved": canonical,
        "choices": choices,
        "examples": df["who"].tolist(),
        "count": int(df.shape[0]),
    }


@app.get("/debug/sql")
def debug_sql(sql: str = FQuery(...)):
    """
    Run a read-only SQL quickly for debugging.Ex:/debug/sql?sql=SELECT%20season,%20COUNT(*)%20FROM%20matches_meta%20GROUP%20BY%201
    """
    import duckdb
    con = duckdb.connect(DB_PATH)
    try:
        df = con.execute(sql).df()
        return {"rows": df.to_dict("records"), "rowcount": int(df.shape[0])}
    except Exception as e:
        return {"error": str(e)}

@app.post("/ask")
def ask(body: AskIn):
    """ Prompt"""
    parsed = route(body.query)
    print("[ASK] parsed:", parsed)
    intent = parsed["intent"]
    if intent == "unknown":
        return {"ok": False, "intent": intent, "query": body.query, "hint": parsed.get("hint")}

    result = run_intent(intent, parsed["params"])
    ok = "error" not in result
    answer_text = format_answer(intent, result)
    result = sanitize_for_json(result)          
    return {"ok": ok, "intent": intent, "query": body.query,
            "result": result, "answer_text": answer_text}