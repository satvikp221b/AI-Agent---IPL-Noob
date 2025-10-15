"""
@author: satvi
"""

"""
DuckDB manager for CricSheet IPL data.

Modes:
  1) SANITY CHECK (no --folder and no --match/--info):
       python ingest.py --db ipl_data.duckdb

  2) SINGLE-PAIR INGEST:
       python ingest.py --db ipl_data.duckdb \
         --match /path/match_id.csv --info /path/match_id__info.csv

  3) BULK INGEST FOLDER (Ingest all the available matches data into a DuckDB):
       python ingest.py --db ipl_data.duckdb \
         --folder /path/to/data
"""

import argparse, os, re, sys, json
from typing import Dict
import pandas as pd
import duckdb
import csv

def read_info_kv(info_csv_path: str) -> dict:
    """
    Build a flat dict like:
      info.season, info.date, info.venue, info.winner, info.winner_runs, ...
    """
    out = {}
    with open(info_csv_path, "r", encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if not row:
                continue
            tag = (row[0] or "").strip()
            if len(row) == 2:
                key = tag
                val = (row[1] or "").strip()
            else:
                key = (row[1] or "").strip()
                val = ",".join(row[2:]).strip()

            if tag.lower() in {"info", "outcome", "innings", "player"}:
                flat_key = f"{tag.lower()}.{key}"
            else:
                flat_key = key if key else tag

            if flat_key and val:
                out[flat_key] = val
    return out

def get_winner(info: dict, team1: str, team2: str):
    """
    Return the winner string or None for Tie/NR.
    """
    cand = (
        info.get("info.winner")
    )

    result = (info.get("outcome.result") or info.get("result") or "").strip().lower()
    if result in {"tie", "no result", "abandoned"}:
        return None

    if not cand:
        return None

    def norm(s: str) -> str:
        s = s.replace("\xa0", " ")
        s = re.sub(r"[.\u200d\-]", " ", s)
        s = re.sub(r"\s+", " ", s).strip().lower()
        return s

    n_cand = norm(cand)
    if team1 and n_cand == norm(team1): return team1
    if team2 and n_cand == norm(team2): return team2
    if team1 and norm(team1) in n_cand: return team1
    if team2 and norm(team2) in n_cand: return team2
    return cand

def parse_info_csv(path: str) -> Dict:
    """
    Parses info from info file about match like umpires, players, venue, winner etc.
    """
    info_raw = {"version": None, "info": {}, "teams": [], "players": {},
                "umpires": [], "referees": [], "outcome": {},
                "player_of_match": [], "innings_order": []}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line: continue
            parts = line.split(",", 2)
            tag = parts[0]
            if tag == "version":
                info_raw["version"] = parts[1] if len(parts) > 1 else None
                continue
            if tag == "info" and len(parts) >= 3:
                k, v = parts[1], parts[2]
                if k == "team": info_raw["teams"].append(v)
                elif k in ("umpire","tv_umpire","umpire1","umpire2"): info_raw["umpires"].append(v)
                elif k in ("match_referee"): info_raw["referees"].append(v)
                elif k == "player_of_match": info_raw["player_of_match"].append(v)
                else: info_raw["info"][k] = v
                continue
            if tag == "innings" and len(parts) == 3:
                sub = parts[1].split(",", 1)
                if len(sub) == 2 and sub[1] == "team": info_raw["innings_order"].append(parts[2])
                continue
            if tag == "player" and len(parts) >= 3:
                team, player = parts[1], parts[2]
                info_raw["players"].setdefault(team, []).append(player)
                continue
            if tag == "outcome" and len(parts) >= 3:
                k, v = parts[1], parts[2]
                info_raw["outcome"][k] = v
                continue

    # Build base fields
    team1 = info_raw["teams"][0] if len(info_raw["teams"]) > 0 else None
    team2 = info_raw["teams"][1] if len(info_raw["teams"]) > 1 else None

    #flatten the csv out
    info_kv = read_info_kv(path)
    winner_final = get_winner(info_kv, team1 or "", team2 or "")

    return {
        "match_id": None,
        "season":  info_raw["info"].get("season"),
        "date":    info_raw["info"].get("date"),
        "venue":   info_raw["info"].get("venue"),
        "event":   info_raw["info"].get("event"),
        "match_number": info_raw["info"].get("match_number"),
        "teams":   info_raw["teams"],
        "team1":   team1,
        "team2":   team2,
        "player_of_match": info_raw["player_of_match"][0] if info_raw["player_of_match"] else None,
        "winner":  winner_final,  
        "umpires": info_raw["umpires"],
        "referees": info_raw["referees"],
        "innings_order": info_raw["innings_order"],
        "players_map": info_raw["players"],
    }

def split_over_ball(x):
    """
    Splits a ball string like "4.2" into (over, ball) integers.
    """
    try:
        s = str(x)
        if "." in s:
            o, b = s.split(".", 1)
            return int(o), int(b)
        return int(s), 0
    except Exception:
        return None, None

def phase_from_over(o: int):
    """
    Returns the bowling phase ("PP", "Middle", or "Death") based on the over number.
    """
    if o is None: return None
    if 1 <= o <= 6: return "PP"
    if 7 <= o <= 15: return "Middle"
    return "Death"

def parse_deliveries_csv(path: str):
    """
    Loads and cleans a deliveries CSV, computes derived columns (over, ball, phase, boundaries, dots), and ensures schema consistency.
    """
    df = pd.read_csv(path)
    rename_map = {
        "runs_off_bat": "runs_batter",
        "extras": "runs_extras",
        "batsman": "striker",
    }
    df = df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns})

    if "runs_extras" not in df.columns: df["runs_extras"] = 0
    if "runs_batter" not in df.columns: df["runs_batter"] = 0

    df["runs_total"] = df["runs_batter"].fillna(0) + df["runs_extras"].fillna(0)

    ob = df["ball"].apply(split_over_ball)
    df["over"] = ob.apply(lambda t: t[0])
    df["ball_number"] = ob.apply(lambda t: t[1])
    df["over_ball"] = df["over"].astype("Int64").astype(str) + "." + df["ball_number"].astype("Int64").astype(str)

    wicket_cols = [c for c in ["wicket_type","dismissal_kind"] if c in df.columns]
    is_wicket = pd.Series(False, index=df.index)
    for c in wicket_cols: is_wicket = is_wicket | df[c].notna()

    df["is_boundary"] = df["runs_batter"].fillna(0).isin([4,6])
    df["is_dot"] = (df["runs_total"].fillna(0) == 0) & (~is_wicket)
    df["phase"] = df["over"].apply(phase_from_over)

    # Enforcing required columns exist check
    required = ["match_id","season","start_date","venue","innings",
                "batting_team","bowling_team","striker","non_striker","bowler"]
    for col in required:
        if col not in df.columns: df[col] = pd.NA

    # Ensure optional columns exist 
    optional = [
        "wides","noballs","byes","legbyes","penalty",
        "wicket_type","other_wicket_type",
        "dismissal_kind","player_dismissed","other_player_dismissed",
    ]
    for col in optional:
        if col not in df.columns:
            df[col] = 0 if col in ["wides","noballs","byes","legbyes","penalty"] else pd.NA

    return df


# DuckDB Schema
# Defines the DuckDB schema for the deliveries table containing every ball-by-ball record of an IPL match.
DDL_DELIVERIES = """
CREATE TABLE IF NOT EXISTS deliveries (
    match_id BIGINT, season TEXT, start_date DATE, venue TEXT,
    innings INTEGER, ball VARCHAR, over INTEGER, ball_number INTEGER, over_ball VARCHAR,
    batting_team TEXT, bowling_team TEXT,
    striker TEXT, non_striker TEXT, bowler TEXT,
    runs_batter INTEGER, runs_extras INTEGER, runs_total INTEGER,
    wides INTEGER, noballs INTEGER, byes INTEGER, legbyes INTEGER, penalty INTEGER,
    wicket_type TEXT, other_wicket_type TEXT, dismissal_kind TEXT,
    player_dismissed TEXT, other_player_dismissed TEXT,
    is_boundary BOOLEAN, is_dot BOOLEAN, phase TEXT
);
"""
# Defines the DuckDB schema for the matches_meta table storing per-match metadata like teams, venue, date, and participants.
DDL_MATCHES_META = """
CREATE TABLE IF NOT EXISTS matches_meta (
    match_id BIGINT,
    season TEXT, date DATE, venue TEXT, event TEXT, match_number TEXT,
    team1 TEXT, team2 TEXT,
    teams_json JSON, player_of_match TEXT, winner TEXT,
    umpires_json JSON, referees_json JSON,
    innings_order_json JSON, players_map_json JSON
);
"""

def ensure_tables(con: duckdb.DuckDBPyConnection):
    """
    Ensures both deliveries and matches_meta tables exist by creating them if missing.
    """
    con.execute(DDL_DELIVERIES)
    con.execute(DDL_MATCHES_META)

def upsert_match(con: duckdb.DuckDBPyConnection, deliveries: pd.DataFrame, meta: Dict):
    """
    Replaces existing match data and inserts updated deliveries plus match metadata into the database.
    """
    match_id = int(str(deliveries["match_id"].iloc[0])) if "match_id" in deliveries.columns and pd.notna(deliveries["match_id"].iloc[0]) else None
    if match_id is None:
        raise ValueError("match_id not found in deliveries CSV")

    meta_row = {
        "match_id": match_id,
        "season": meta.get("season"), "date": meta.get("date"), "venue": meta.get("venue"),
        "event": meta.get("event"), "match_number": meta.get("match_number"),
        "team1": meta.get("team1"), "team2": meta.get("team2"),
        "teams_json": json.dumps(meta.get("teams", [])),
        "player_of_match": meta.get("player_of_match"), "winner": meta.get("winner"),
        "umpires_json": json.dumps(meta.get("umpires", [])),
        "referees_json": json.dumps(meta.get("referees", [])),
        "innings_order_json": json.dumps(meta.get("innings_order", [])),
        "players_map_json": json.dumps(meta.get("players_map", {})),
    }

    ensure_tables(con)

    con.execute("DELETE FROM deliveries   WHERE match_id = ?", [match_id])
    con.execute("DELETE FROM matches_meta WHERE match_id = ?", [match_id])

    con.register("tmp_deliveries", deliveries)
    con.execute("""
        INSERT INTO deliveries
        SELECT
            match_id, season, start_date, venue, innings, CAST(ball AS VARCHAR),
            over, ball_number, over_ball, batting_team, bowling_team,
            striker, non_striker, bowler,
            COALESCE(runs_batter,0), COALESCE(runs_extras,0), COALESCE(runs_total,0),
            COALESCE(wides,0), COALESCE(noballs,0), COALESCE(byes,0),
            COALESCE(legbyes,0), COALESCE(penalty,0),
            wicket_type, other_wicket_type, dismissal_kind,
            player_dismissed, other_player_dismissed,
            is_boundary, is_dot, phase
        FROM tmp_deliveries
    """)
    con.unregister("tmp_deliveries")

    con.register("tmp_meta", pd.DataFrame([meta_row]))
    con.execute("""
        INSERT INTO matches_meta
        SELECT match_id, season, date, venue, event, match_number, team1, team2,
               teams_json, player_of_match, winner, umpires_json, referees_json,
               innings_order_json, players_map_json
        FROM tmp_meta
    """)
    con.unregister("tmp_meta")

def find_pairs(folder: str):
    """
    Scans a directory for match_id.csv and match_id_info.csv pairs for bulk data ingestion.
    """
    PAIR_INFO_RE = re.compile(r"^(\d+)_info\.csv$", re.IGNORECASE)
    entries = os.listdir(folder)
    info_files = [f for f in entries if PAIR_INFO_RE.match(f)]
    pairs = []
    for info_name in info_files:
        mid = PAIR_INFO_RE.match(info_name).group(1)
        deliveries_name = f"{mid}.csv"
        info_path = os.path.join(folder, info_name)
        deliveries_path = os.path.join(folder, deliveries_name)
        if os.path.exists(deliveries_path):
            pairs.append((mid, deliveries_path, info_path))
        else:
            print(f"[skip] Missing deliveries for {mid}: {deliveries_path}")
    return pairs


# Sanity Mode (one pair of match_id.csv and match_id_info.csv)

# SQL query summarizing match coverage per season, including number of matches and participating teams.
COVERAGE_SQL = """
WITH seasons AS (
  SELECT season, COUNT(*) AS matches
  FROM matches_meta
  GROUP BY season
),
teams_union AS (
  SELECT season, team1 AS team FROM matches_meta
  UNION ALL
  SELECT season, team2 AS team FROM matches_meta
),
teams_agg AS (
  SELECT season, STRING_AGG(DISTINCT team, ', ') AS teams_csv
  FROM teams_union
  GROUP BY season
)
SELECT s.season, s.matches, t.teams_csv AS teams
FROM seasons s
LEFT JOIN teams_agg t USING (season)
ORDER BY s.season;
"""

def sanity(con: duckdb.DuckDBPyConnection):
    """
    Performs integrity checks: prints record counts, coverage summary, sample matches, and the deliveries table schema.
    """
    ensure_tables(con)
    # counts total matches in the db
    cnt_meta = con.execute("SELECT COUNT(*) FROM matches_meta").fetchone()[0]
    cnt_delv = con.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
    print(f"[sanity] matches_meta rows: {cnt_meta}")
    print(f"[sanity] deliveries rows:   {cnt_delv}")

    # show a few seasons + teams
    try:
        df_cov = con.execute(COVERAGE_SQL).df()
        if not df_cov.empty:
            print("\n[coverage by season]")
            print(df_cov.to_string(index=False))
        else:
            print("\n[coverage by season] (no rows)")
    except Exception as e:
        print(f"[coverage] skipped due to error: {e}")

    # peek five matches (head)
    df_matches = con.execute("""
        SELECT match_id, season, date, team1, team2, venue, player_of_match, winner
        FROM matches_meta
        ORDER BY date NULLS LAST, match_id
        LIMIT 5
    """).df()
    print("\n[sample matches_meta]")
    print(df_matches.to_string(index=False))

    # columns present in deliveries
    df_cols = con.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'deliveries'
        ORDER BY column_name
    """).df()
    print("\n[deliveries schema columns]")
    print(df_cols.to_string(index=False))


# CLI

#Entry point handling CLI arguments for bulk ingestion, single match ingestion, or running database sanity checks.
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="ipl_data.duckdb", help="DuckDB file path")
    ap.add_argument("--folder", help="Folder with <id>.csv and <id>_info.csv for BULK INGEST")
    ap.add_argument("--match", help="Path to <id>.csv for SINGLE-PAIR INGEST")
    ap.add_argument("--info",  help="Path to <id>_info.csv for SINGLE-PAIR INGEST")
    args = ap.parse_args()

    con = duckdb.connect(args.db)

    # Mode selection
    if args.folder:
        # BULK INGEST
        pairs = find_pairs(args.folder)
        if not pairs:
            print("[bulk] No pairs found. Check --folder path.")
            sanity(con)
            con.close()
            sys.exit(1)
        ok = fail = 0
        for mid, deliveries_path, info_path in sorted(pairs, key=lambda x: int(x[0])):
            try:
                deliveries = parse_deliveries_csv(deliveries_path)
                meta = parse_info_csv(info_path)
                if "match_id" in deliveries.columns and pd.notna(deliveries["match_id"].iloc[0]):
                    meta["match_id"] = int(str(deliveries["match_id"].iloc[0]))
                upsert_match(con, deliveries, meta)
                ok += 1
                if ok % 100 == 0:
                    print(f"[progress] Ingested {ok} matches...")
            except Exception as e:
                fail += 1
                print(f"[fail] {mid}: {e}")
        print(f"\n[bulk] Success: {ok}  Failed: {fail}  Total: {ok+fail}")
        sanity(con)
        con.close()
        return

    if args.match and args.info:
        # SINGLE-PAIR INGEST
        deliveries = parse_deliveries_csv(args.match)
        meta = parse_info_csv(args.info)
        if "match_id" in deliveries.columns and pd.notna(deliveries["match_id"].iloc[0]):
            meta["match_id"] = int(str(deliveries["match_id"].iloc[0]))
        upsert_match(con, deliveries, meta)
        print("[pair] Ingested one match.")
        sanity(con)
        con.close()
        return

    # SANITY-ONLY (no folder, no match/info)
    print("[sanity-only] Connecting to DB and inspecting…")
    sanity(con)
    con.close()


if __name__ == "__main__":
    main()
