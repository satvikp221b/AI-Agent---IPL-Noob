# -*- coding: utf-8 -*-
"""
@author: satvi
"""

import duckdb
import pandas as pd
import json

#Functions used to calculate the queries' many ground levels
def safe_int(x, default=0):
    """Convert to int safely, treating pd.NA / NaN / None as default."""
    if pd.isna(x):
        return default
    try:
        return int(x)
    except Exception:
        return default

def safe_float(x, default=0.0):
    """Convert to float safely, treating pd.NA / NaN / None as default."""
    if pd.isna(x):
        return default
    try:
        return float(x)
    except Exception:
        return default

def safe_div(n, d, scale=1.0):
    """Safe division with NaN/0 protection."""
    if d in (0, None) or pd.isna(d):
        return None
    try:
        return round((n * scale) / d, 2)
    except Exception:
        return None

def safe_mean(values):
    """Safe mean ignoring NaN/None."""
    vals = [v for v in values if not pd.isna(v)]
    return round(sum(vals) / len(vals), 2) if vals else None


#Query functions 

def match_summary(db_path, team_a, team_b, season, nth=1):
    """
    Rich match summary for the nth meeting of two teams in a season.
    - Innings: runs/wkts/overs, RR
    - Top batters: 2 per innings (runs, balls, 4s/6s, SR)
    - Top bowlers: 2 per innings (wkts, runs conceded, overs, Econ)
    """
    import pandas as pd
    import duckdb
    con = duckdb.connect(db_path)

    # Pick the nth match (2 matches in a season by default most fo the times, more if met in playoffs)
    meta = con.execute(f"""
        SELECT match_id, season, date, venue, team1, team2, winner, player_of_match
        FROM matches_meta
        WHERE season = ?
          AND (
                (team1 = ? AND team2 = ?) OR
                (team1 = ? AND team2 = ?)
              )
        ORDER BY date NULLS LAST, match_id
        LIMIT {nth}
    """, [season, team_a, team_b, team_b, team_a]).df()
    if meta.empty:
        return {"error": f"No match found between {team_a} and {team_b} in {season}"}

    m = meta.iloc[-1].to_dict()
    match_id = int(m["match_id"])

    # Innings summary (legal balls; RR = runs*6 / legal_balls)
    inn = con.execute("""
        WITH base AS (
          SELECT
            innings,
            batting_team,
            SUM(runs_total) AS runs,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS legal_balls,
            COUNT(CASE WHEN player_dismissed IS NOT NULL THEN 1 END) AS wickets
          FROM deliveries
          WHERE match_id = ?
          GROUP BY innings, batting_team
        )
        SELECT
          innings,
          batting_team,
          runs::INTEGER AS runs,
          wickets::INTEGER AS wickets,
          legal_balls::INTEGER AS legal_balls,
          (legal_balls / 6) || '.' || (legal_balls % 6) AS overs_str,
          ROUND((runs * 6.0) / NULLIF(legal_balls, 0), 2) AS run_rate
        FROM base
        ORDER BY innings
    """, [match_id]).df()

    # Get the top 2 batters per innings
    top_batters = con.execute("""
        WITH bat AS (
          SELECT
            innings,
            striker AS batter,
            SUM(runs_batter) AS runs,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS balls,
            SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
            SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) AS sixes
          FROM deliveries
          WHERE match_id = ?
          GROUP BY innings, striker
        ),
        ranked AS (
          SELECT
            innings,
            batter,
            runs::INTEGER AS runs,
            balls::INTEGER AS balls,
            fours::INTEGER AS fours,
            sixes::INTEGER AS sixes,
            ROUND((runs * 100.0) / NULLIF(balls, 0), 2) AS strike_rate,
            ROW_NUMBER() OVER (
              PARTITION BY innings
              ORDER BY runs DESC, strike_rate DESC NULLS LAST, balls ASC, batter ASC
            ) AS rk
          FROM bat
        )
        SELECT innings, batter, runs, balls, fours, sixes, strike_rate
        FROM ranked
        WHERE rk <= 2
        ORDER BY innings, rk
    """, [match_id]).df()

    # Get the top 2 bowlers per innings
    top_bowlers = con.execute("""
        WITH bowl AS (
          SELECT
            innings,
            bowler,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS legal_balls,
            SUM(runs_total) AS runs_conceded,
            COUNT(CASE WHEN player_dismissed IS NOT NULL THEN 1 END) AS wickets
          FROM deliveries
          WHERE match_id = ?
          GROUP BY innings, bowler
        ),
        ranked AS (
          SELECT
            innings,
            bowler,
            wickets::INTEGER AS wickets,
            runs_conceded::INTEGER AS runs_conceded,
            (legal_balls / 6) || '.' || (legal_balls % 6) AS overs,
            ROUND((runs_conceded * 6.0) / NULLIF(legal_balls, 0), 2) AS economy,
            ROW_NUMBER() OVER (
              PARTITION BY innings
              ORDER BY wickets DESC, economy ASC NULLS LAST, runs_conceded ASC, bowler ASC
            ) AS rk
          FROM bowl
        )
        SELECT innings, bowler, wickets, runs_conceded, overs, economy
        FROM ranked
        WHERE rk <= 2
        ORDER BY innings, rk
    """, [match_id]).df()

    # Build payload
    innings_payload = []
    for _, r in inn.iterrows():
        innings_payload.append({
            "innings": int(r["innings"]),
            "batting_team": r["batting_team"],
            "runs": int(r["runs"]) if not pd.isna(r["runs"]) else 0,
            "wickets": int(r["wickets"]) if not pd.isna(r["wickets"]) else 0,
            "overs": r["overs_str"] if r.get("overs_str", None) is not None else None,
            "run_rate": None if pd.isna(r["run_rate"]) else float(r["run_rate"]),
        })

    top_batters_payload = []
    for _, r in top_batters.iterrows():
        top_batters_payload.append({
            "innings": int(r["innings"]),
            "batter": r["batter"],
            "runs": int(r["runs"]) if not pd.isna(r["runs"]) else 0,
            "balls": int(r["balls"]) if not pd.isna(r["balls"]) else 0,
            "fours": int(r["fours"]) if not pd.isna(r["fours"]) else 0,
            "sixes": int(r["sixes"]) if not pd.isna(r["sixes"]) else 0,
            "strike_rate": None if pd.isna(r["strike_rate"]) else float(r["strike_rate"]),
        })

    top_bowlers_payload = []
    for _, r in top_bowlers.iterrows():
        top_bowlers_payload.append({
            "innings": int(r["innings"]),
            "bowler": r["bowler"],
            "wickets": int(r["wickets"]) if not pd.isna(r["wickets"]) else 0,
            "runs_conceded": int(r["runs_conceded"]) if not pd.isna(r["runs_conceded"]) else 0,
            "overs": r["overs"],
            "economy": None if pd.isna(r["economy"]) else float(r["economy"]),
        })

    # Formatter convenience
    m["teams"] = [m.get("team1"), m.get("team2")]

    return {
        "meta": m,
        "innings": innings_payload,
        "top_batters": top_batters_payload,
        "top_bowlers": top_bowlers_payload,
        "evidence": {"match_id": match_id},
    }


def player_stats(db_path, player, scope="career", season=None):
    """Aggregate batting & bowling stats for a player with all teams/last team and best matchup against which bowler and batter."""
    con = duckdb.connect(db_path)

    # Base filter selecting season or career
    where = f"(striker = ? OR bowler = ?)"
    params = [player, player]
    if scope == "season" and season:
        where += " AND season = ?"
        params.append(season)

    df = con.execute(f"SELECT * FROM deliveries WHERE {where}", params).df()
    if df.empty:
        return {"error": f"No data found for player {player}"}

    # Batting aggregates
    bat_df = df[df["striker"] == player]
    runs = safe_int(bat_df["runs_batter"].sum())
    balls = safe_int(len(bat_df))
    fours = safe_int((bat_df["runs_batter"] == 4).sum())
    sixes = safe_int((bat_df["runs_batter"] == 6).sum())
    dismissals = safe_int(bat_df["player_dismissed"].notna().sum())
    sr = safe_div(runs, balls, 100)
    avg = safe_div(runs, dismissals, 1.0)

    batting = {
        "matches": safe_int(df["match_id"].nunique()),
        "inns": safe_int(bat_df["innings"].nunique()),
        "runs": runs,
        "balls": balls,
        "fours": fours,
        "sixes": sixes,
        "sr": sr,
        "average": avg,
    }

    # Bowling aggregates
    bowl_df = df[df["bowler"] == player]
    balls_bowled = safe_int(len(bowl_df))
    runs_conceded = safe_int(bowl_df["runs_total"].sum())
    wickets = safe_int(bowl_df["player_dismissed"].notna().sum())
    economy = safe_div(runs_conceded, balls_bowled / 6.0, 1.0)

    bowling = {
        "matches": safe_int(bowl_df["match_id"].nunique()),
        "overs": safe_div(balls_bowled, 6.0, 1.0),
        "wickets": wickets,
        "runs_conceded": runs_conceded,
        "economy": economy,
    }

    # Teams represented throughout career
    teams_df = con.execute(
        """
        WITH appearances AS (
          SELECT DISTINCT match_id,
                 CASE WHEN striker = ? THEN batting_team END AS team_a,
                 CASE WHEN bowler  = ? THEN bowling_team END AS team_b
          FROM deliveries
          WHERE striker = ? OR bowler = ?
        ),
        teams AS (
          SELECT team_a AS team FROM appearances WHERE team_a IS NOT NULL
          UNION
          SELECT team_b AS team FROM appearances WHERE team_b IS NOT NULL
        )
        SELECT DISTINCT team FROM teams WHERE team IS NOT NULL ORDER BY team
        """,
        [player, player, player, player],
    ).df()
    teams = sorted([t for t in teams_df["team"].tolist() if t])

    # Last team played for (calculated using latest match appearance) 
    last_df = con.execute(
        """
        WITH ap AS (
          SELECT d.match_id, m.date,
                 CASE
                   WHEN d.striker = ? THEN d.batting_team
                   WHEN d.bowler  = ? THEN d.bowling_team
                 END AS team
          FROM deliveries d
          JOIN matches_meta m USING (match_id)
          WHERE d.striker = ? OR d.bowler = ?
        ),
        ranked AS (
          SELECT *, ROW_NUMBER() OVER (ORDER BY date DESC NULLS LAST, match_id DESC) AS rk
          FROM ap
          WHERE team IS NOT NULL
        )
        SELECT match_id, date, team
        FROM ranked
        WHERE rk = 1
        """,
        [player, player, player, player],
    ).df()

    last_team = None
    if not last_df.empty:
        r = last_df.iloc[0]
        last_team = {
            "team": r["team"],
            "match_id": int(r["match_id"]) if not pd.isna(r["match_id"]) else None,
            "date": str(r["date"]) if not pd.isna(r["date"]) else None,
        }

    
    # Matchups (Batting)

    # Nemesis bowler (most dismissals of this batter)
    nemesis = con.execute(
        """
        WITH agg AS (
          SELECT
            bowler,
            COUNT(CASE WHEN player_dismissed = ? THEN 1 END) AS outs,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS legal_balls_vs,
            SUM(runs_total) AS runs_vs
          FROM deliveries
          WHERE striker = ?
          GROUP BY bowler
        )
        SELECT
          bowler,
          outs::INTEGER AS outs,
          legal_balls_vs::INTEGER AS balls,
          ROUND((runs_vs * 6.0) / NULLIF(legal_balls_vs,0), 2) AS econ_vs
        FROM agg
        WHERE outs IS NOT NULL AND outs > 0
        ORDER BY outs DESC, econ_vs ASC NULLS LAST, bowler ASC
        LIMIT 1
        """,
        [player, player],
    ).df()

    nemesis_bowler = None
    if not nemesis.empty:
        r = nemesis.iloc[0]
        nemesis_bowler = {
            "bowler": r["bowler"],
            "outs": int(r["outs"]) if not pd.isna(r["outs"]) else 0,
            "balls": int(r["balls"]) if not pd.isna(r["balls"]) else 0,
            "economy_against": None if pd.isna(r["econ_vs"]) else float(r["econ_vs"]),
        }

    # Favourite bowler (highest economy conceded to this batter (min 10 overs = 60 balls))
    fav = con.execute(
        """
        WITH agg AS (
          SELECT
            bowler,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS legal_balls_vs,
            SUM(runs_total) AS runs_vs
          FROM deliveries
          WHERE striker = ?
          GROUP BY bowler
        )
        SELECT
          bowler,
          legal_balls_vs::INTEGER AS balls,
          ROUND((runs_vs * 6.0) / NULLIF(legal_balls_vs,0), 2) AS economy
        FROM agg
        WHERE legal_balls_vs >= 60
        ORDER BY economy DESC, bowler ASC
        LIMIT 1
        """,
        [player],
    ).df()

    favourite_bowler = None
    if not fav.empty:
        r = fav.iloc[0]
        favourite_bowler = {
            "bowler": r["bowler"],
            "balls": int(r["balls"]) if not pd.isna(r["balls"]) else 0,
            "economy": None if pd.isna(r["economy"]) else float(r["economy"]),
        }

    
    #Matchup (Bowling)
    

    # Bunny Batter (dismissed most by this bowler)
    bunny = con.execute(
        """
        WITH agg AS (
          SELECT
            striker AS batter,
            COUNT(CASE WHEN player_dismissed = striker THEN 1 END) AS outs,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS legal_balls_vs,
            SUM(runs_total) AS runs_vs
          FROM deliveries
          WHERE bowler = ?
          GROUP BY striker
        )
        SELECT
          batter,
          outs::INTEGER AS outs,
          legal_balls_vs::INTEGER AS balls,
          ROUND((runs_vs * 6.0) / NULLIF(legal_balls_vs,0), 2) AS econ_vs
        FROM agg
        WHERE outs IS NOT NULL AND outs > 0
        ORDER BY outs DESC, econ_vs ASC NULLS LAST, batter ASC
        LIMIT 1
        """,
        [player],
    ).df()

    most_dismissed_batter = None
    if not bunny.empty:
        r = bunny.iloc[0]
        most_dismissed_batter = {
            "batter": r["batter"],
            "outs": int(r["outs"]) if not pd.isna(r["outs"]) else 0,
            "balls": int(r["balls"]) if not pd.isna(r["balls"]) else 0,
            "economy_against": None if pd.isna(r["econ_vs"]) else float(r["econ_vs"]),
        }

    # Worst economy vs a batter (min 10 overs bowled to that batter)
    worst = con.execute(
        """
        WITH agg AS (
          SELECT
            striker AS batter,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS legal_balls_vs,
            SUM(runs_total) AS runs_vs
          FROM deliveries
          WHERE bowler = ?
          GROUP BY striker
        )
        SELECT
          batter,
          legal_balls_vs::INTEGER AS balls,
          ROUND((runs_vs * 6.0) / NULLIF(legal_balls_vs,0), 2) AS economy
        FROM agg
        WHERE legal_balls_vs >= 60
        ORDER BY economy DESC, batter ASC
        LIMIT 1
        """,
        [player],
    ).df()

    worst_vs_batter = None
    if not worst.empty:
        r = worst.iloc[0]
        worst_vs_batter = {
            "batter": r["batter"],
            "balls": int(r["balls"]) if not pd.isna(r["balls"]) else 0,
            "economy": None if pd.isna(r["economy"]) else float(r["economy"]),
        }

    return {
        "input": {"player_query": player, "resolved_name": player, "scope": scope, "season": season},
        "batting": batting,
        "bowling": bowling,
        "teams": teams,
        "last_team": last_team,
        "matchups": {
            "batting": {
                "nemesis_bowler": nemesis_bowler,
                "favourite_bowler": favourite_bowler,
            },
            "bowling": {
                "most_dismissed_batter": most_dismissed_batter,
                "worst_vs_batter": worst_vs_batter,
            }
        }
    }


def team_squad(db_path, team, season):
    """Get the squad for any team for a particular season"""
    con = duckdb.connect(db_path)

    # Try to read listed squad from matches_meta 
    q_json = """
        WITH base AS (
          SELECT players_map_json
          FROM matches_meta
          WHERE season = ? AND (team1 = ? OR team2 = ?)
        )
        SELECT DISTINCT CAST(j.value AS VARCHAR) AS players_json
        FROM base, json_each(base.players_map_json) AS j
        WHERE j.key = ?
    """
    df_json = con.execute(q_json, [season, team, team, team]).df()

    listed = set()
    if not df_json.empty:
        for _, row in df_json.iterrows():
            val = row.get("players_json")
            if pd.isna(val) or not val:
                continue
            s = str(val).strip()
            if s.lower() == "null":
                continue
            parsed = None
            try:
                parsed = json.loads(s)
            except Exception:
                try:
                    parsed = json.loads(s.replace("'", '"'))
                except Exception:
                    if s.startswith("[") and s.endswith("]"):
                        inner = s[1:-1].strip().replace("'", "").replace('"', "")
                        parsed = [p.strip() for p in inner.split(",") if p.strip()]
            if isinstance(parsed, list):
                listed.update([p for p in parsed if p])
            elif isinstance(parsed, str) and parsed:
                listed.add(parsed)

    # Number of actual appearances from deliveries
    q_apps = """
        WITH ap AS (
            -- one row per (match, player) when he batted for the team
            SELECT DISTINCT match_id, striker AS player
            FROM deliveries
            WHERE season = ? AND batting_team = ? AND striker IS NOT NULL

            UNION

            -- one row per (match, player) when he bowled for the team
            SELECT DISTINCT match_id, bowler AS player
            FROM deliveries
            WHERE season = ? AND bowling_team = ? AND bowler IS NOT NULL
        )
        SELECT player, COUNT(DISTINCT match_id) AS matches
        FROM ap
        WHERE player IS NOT NULL
        GROUP BY player
        ORDER BY matches DESC, player
    """
    df_apps = con.execute(q_apps, [season, team, season, team]).df()

    # Players who actually appeared
    appeared = set(df_apps["player"].tolist()) if not df_apps.empty else set()

    # Union of sources i.e., everyone who appeared + anyone listed in match_id_info.csv
    all_players = sorted(appeared.union(listed))

    # Build appearances map
    app_map = {row["player"]: int(row["matches"]) for _, row in df_apps.iterrows()} if not df_apps.empty else {}
    squad = [{"player": p, "appearances": app_map.get(p, 0 if p in listed else None)} for p in all_players]

    # If absolutely nothing found, return error
    if not squad:
        return {"error": f"No squad data found for {team} in {season}"}

    return {"input": {"team": team, "season": season}, "squad": squad}

def player_vs_team(db_path, player, opponent, scope="career", season=None):
    """Stats of a particular player against a team during a particular season or throughout their career"""
    con = duckdb.connect(db_path)
    where = f"WHERE (striker = '{player}' OR bowler = '{player}')"
    where += f" AND (batting_team = '{opponent}' OR bowling_team = '{opponent}')"
    if scope == "season" and season:
        where += f" AND season = '{season}'"

    df = con.execute(f"SELECT * FROM deliveries {where}").df()
    if df.empty:
        return {"error": f"No data found for {player} vs {opponent}"}

    # Batting vs Opponent Team
    bat_df = df[df["striker"] == player]
    bat_runs = safe_int(bat_df["runs_batter"].sum())
    bat_balls = safe_int(len(bat_df))
    bat_fours = safe_int((bat_df["runs_batter"] == 4).sum())
    bat_sixes = safe_int((bat_df["runs_batter"] == 6).sum())
    dismissals = safe_int(bat_df["player_dismissed"].notna().sum())
    sr = safe_div(bat_runs, bat_balls, 100)
    avg = safe_div(bat_runs, dismissals, 1.0)

    batting_vs_team = {
        "runs": bat_runs, "balls": bat_balls,
        "fours": bat_fours, "sixes": bat_sixes,
        "sr": sr, "average": avg
    }

    # Bowling vs Opponent Team
    bowl_df = df[df["bowler"] == player]
    balls_bowled = safe_int(len(bowl_df))
    runs_conceded = safe_int(bowl_df["runs_total"].sum())
    wickets = safe_int(bowl_df["player_dismissed"].notna().sum())
    economy = safe_div(runs_conceded, balls_bowled / 6.0, 1.0)

    bowling_vs_team = {
        "overs": safe_div(balls_bowled, 6.0, 1.0),
        "wickets": wickets,
        "runs_conceded": runs_conceded,
        "economy": economy
    }

    return {
        "input": {"player_query": player, "opponent": opponent, "scope": scope, "season": season},
        "batting_vs_team": batting_vs_team,
        "bowling_vs_team": bowling_vs_team,
    }


def head_to_head(db_path, team_a, team_b, scope="career", season=None):
    """
    H2H summary + star performers for each team (bat & bowl).
    Batting star: runs, balls (legal), avg, 50s/100s (vs that opponent team).
    Bowling star: legal balls, runs conceded, econ, wickets (vs that opponent team).
    """
    import duckdb, pandas as pd
    con = duckdb.connect(db_path)

    
    season_filter = " AND season = ? " if (scope == "season" and season) else ""
    season_args = [season] if (scope == "season" and season) else []

    # Basic H2H summary
    base = con.execute(f"""
        WITH h2m AS (
          SELECT match_id, season, date, venue, team1, team2, winner
          FROM matches_meta
          WHERE ((team1 = ? AND team2 = ?) OR (team1 = ? AND team2 = ?))
            {season_filter}
        )
        SELECT *
        FROM h2m
        ORDER BY date NULLS LAST, match_id
    """, [team_a, team_b, team_b, team_a] + season_args).df()

    if base.empty:
        return {"error": f"No head-to-head matches found between {team_a} and {team_b}"
                         + (f" in {season}" if season else "")}

    matches = base["match_id"].tolist()

    # Win counts
    wins_a = int((base["winner"] == team_a).sum())
    wins_b = int((base["winner"] == team_b).sum())
    ties = int((base["winner"].isna()).sum()) 
    nores = 0

    earliest = base.iloc[0]
    latest = base.iloc[-1]
    summary = {
        "matches": int(base.shape[0]),
        f"wins_{team_a}": wins_a,
        f"wins_{team_b}": wins_b,
        "ties": ties,
        "no_result": nores,
        "earliest": {
            "match_id": int(earliest["match_id"]),
            "season": earliest.get("season"),
            "date": str(earliest.get("date")),
        },
        "latest": {
            "match_id": int(latest["match_id"]),
            "season": latest.get("season"),
            "date": str(latest.get("date")),
        },
    }

    # Star performers 
    # Batting (for team_a vs team_b): aggregate per batter only when he batted for team_a against team_b.
    bat_sql = f"""
        WITH scope_delv AS (
          SELECT *
          FROM deliveries
          WHERE match_id IN ({",".join(["?"]*len(matches))})
            {season_filter}
        ),
        bat AS (
          SELECT
            striker           AS batter,
            batting_team      AS team_for,
            bowling_team      AS team_against,
            SUM(runs_batter)  AS runs,
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS balls,
            COUNT(CASE WHEN player_dismissed = striker THEN 1 END) AS outs
          FROM scope_delv
          GROUP BY striker, batting_team, bowling_team
        ),
        inns_runs AS (
          SELECT
            striker AS batter,
            batting_team AS team_for,
            bowling_team AS team_against,
            match_id, innings,
            SUM(runs_batter) AS inns_runs
          FROM scope_delv
          GROUP BY striker, batting_team, bowling_team, match_id, innings
        ),
        milestones AS (
          SELECT
            batter, team_for, team_against,
            SUM(CASE WHEN inns_runs >= 100 THEN 1 ELSE 0 END) AS hundreds,
            SUM(CASE WHEN inns_runs >=  50 AND inns_runs < 100 THEN 1 ELSE 0 END) AS fifties
          FROM inns_runs
          GROUP BY batter, team_for, team_against
        ),
        agg AS (
          SELECT
            b.batter, b.team_for, b.team_against,
            b.runs, b.balls, b.outs,
            COALESCE(m.hundreds,0) AS hundreds,
            COALESCE(m.fifties,0)  AS fifties,
            ROUND((b.runs * 100.0) / NULLIF(b.balls,0), 2) AS sr,
            ROUND((b.runs * 1.0)   / NULLIF(b.outs,0),  2) AS avg
          FROM bat b
          LEFT JOIN milestones m
            ON (b.batter=m.batter AND b.team_for=m.team_for AND b.team_against=m.team_against)
        )
        SELECT *
        FROM agg
        WHERE team_for = ? AND team_against = ?
        ORDER BY runs DESC, avg DESC NULLS LAST, sr DESC NULLS LAST, balls ASC, batter ASC
        LIMIT 1
    """

    # Bowling (for team_a vs team_b): aggregate per bowler only when he bowled for team_a against team_b.
    bowl_sql = f"""
        WITH scope_delv AS (
          SELECT *
          FROM deliveries
          WHERE match_id IN ({",".join(["?"]*len(matches))})
            {season_filter}
        ),
        agg AS (
          SELECT
            bowler            AS bowler,
            bowling_team      AS team_for,
            batting_team      AS team_against,
            -- legal balls bowled
            SUM(CASE WHEN COALESCE(wides,0)>0 OR COALESCE(noballs,0)>0 THEN 0 ELSE 1 END) AS balls,
            SUM(runs_total)   AS runs_conceded,
            COUNT(
              CASE WHEN player_dismissed IS NOT NULL
                     AND LOWER(COALESCE(dismissal_kind, wicket_type,'')) NOT LIKE '%run out%'
                   THEN 1 END
            ) AS wickets
          FROM scope_delv
          GROUP BY bowler, bowling_team, batting_team
        )
        SELECT
          bowler, team_for, team_against,
          balls::INTEGER AS balls,
          runs_conceded::INTEGER AS runs_conceded,
          wickets::INTEGER AS wickets,
          ROUND((runs_conceded * 6.0) / NULLIF(balls,0), 2) AS economy
        FROM agg
        WHERE team_for = ? AND team_against = ?
        ORDER BY wickets DESC, economy ASC NULLS LAST, runs_conceded ASC, balls DESC, bowler ASC
        LIMIT 1
    """

    # Execute stars for both teams 
    args_common = matches + season_args
    # A batting vs B
    bat_a = con.execute(bat_sql, args_common + [team_a, team_b]).df()
    # B batting vs A
    bat_b = con.execute(bat_sql, args_common + [team_b, team_a]).df()
    # A bowling vs B
    bowl_a = con.execute(bowl_sql, args_common + [team_a, team_b]).df()
    # B bowling vs A
    bowl_b = con.execute(bowl_sql, args_common + [team_b, team_a]).df()

    def bat_payload(dfrow):
        """Get all the star performer with the bat info together"""
        if dfrow is None:
            return None
        r = dfrow
        return {
            "player": r["batter"],
            "runs": int(r["runs"]) if not pd.isna(r["runs"]) else 0,
            "balls": int(r["balls"]) if not pd.isna(r["balls"]) else 0,
            "avg": None if pd.isna(r["avg"]) else float(r["avg"]),
            "fifties": int(r["fifties"]) if not pd.isna(r["fifties"]) else 0,
            "hundreds": int(r["hundreds"]) if not pd.isna(r["hundreds"]) else 0,
        }

    def bowl_payload(dfrow):
        """Get all the star performer with the bowl info together"""
        if dfrow is None:
            return None
        r = dfrow
        return {
            "player": r["bowler"],
            "balls": int(r["balls"]) if not pd.isna(r["balls"]) else 0,
            "runs_conceded": int(r["runs_conceded"]) if not pd.isna(r["runs_conceded"]) else 0,
            "wickets": int(r["wickets"]) if not pd.isna(r["wickets"]) else 0,
            "economy": None if pd.isna(r["economy"]) else float(r["economy"]),
        }

    star = {
        team_a: {
            "batting": bat_payload(bat_a.iloc[0] if not bat_a.empty else None),
            "bowling": bowl_payload(bowl_a.iloc[0] if not bowl_a.empty else None),
        },
        team_b: {
            "batting": bat_payload(bat_b.iloc[0] if not bat_b.empty else None),
            "bowling": bowl_payload(bowl_b.iloc[0] if not bowl_b.empty else None),
        }
    }

    return {
        "summary": summary,
        "star_performers": star,
    }

