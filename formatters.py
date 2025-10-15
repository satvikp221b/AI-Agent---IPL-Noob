# Readable text formatters for display
from typing import Dict, Any, List

def s(x):
    """Safely stringify a value, returning "" for None."""
    return "" if x is None else str(x)

def nz(x, default="0"):
    """Return x as a string unless it is empty/None, then use default."""
    return default if x is None or x == "" else str(x)

def overs(v):
    """Pretty print overs."""
    return "-" if v in (None, 0) else str(v)

def bullet_list(items: List[str], indent="  * "):
    """Join a list into a newline-separated bulleted list (ASCII)."""
    return "\n".join(indent + line for line in items if line)

def header(title: str):
    """Render a section header as title + underline."""
    bar = "-" * len(title)
    return f"{title}\n{bar}"

def meta_teams(meta: Dict[str, Any]):
    """Extract (team1, team2) from meta['teams'] or team1/team2 keys."""
    if isinstance(meta, dict):
        teams = meta.get("teams")
        if isinstance(teams, list) and len(teams) >= 2:
            return s(teams[0]), s(teams[1])
        t1, t2 = meta.get("team1"), meta.get("team2")
        return s(t1), s(t2)
    return "", ""

def format_match_summary(payload: Dict[str, Any]):
    """Render a human-readable match summary with innings, top batters/bowlers, and results."""
    if "error" in payload:
        return f"{payload['error']}"

    m = payload.get("meta", {}) or {}
    team1, team2 = meta_teams(m)
    title = f"{team1} vs {team2}"
    if m.get("season"):
        title += f" - {s(m.get('season'))}"

    lines = [
        header(title),
        f"Date: {s(m.get('date'))}  |  Venue: {s(m.get('venue'))}",
        f"Result: {s(m.get('winner')) or '-'}  |  Player of the Match: {s(m.get('player_of_match')) or '-'}",
    ]

    inn = payload.get("innings", [])
    if isinstance(inn, list) and inn:
        lines.append("")
        lines.append(header("Innings Summary"))
        for i in inn:
            bt = s(i.get("batting_team"))
            runs = nz(i.get("runs"))
            wk = nz(i.get("wickets"))
            ov = overs(i.get("overs"))
            rr = s(i.get("run_rate"))
            lines.append(f"{bt}: {runs}/{wk} in {ov} overs (RR {rr})")

    tb = payload.get("top_batters", [])
    if isinstance(tb, list) and tb:
        lines.append("")
        lines.append(header("Top Batters"))
        lines.append(bullet_list([
            f"Inns {r.get('innings')}: {r.get('batter')} - {r.get('runs')} ({r.get('balls')}) "
            f"4x{r.get('fours')} 6x{r.get('sixes')} SR {r.get('strike_rate')}"
            for r in tb
        ]))

    bw = payload.get("top_bowlers", [])
    if isinstance(bw, list) and bw:
        lines.append("")
        lines.append(header("Top Bowlers"))
        lines.append(bullet_list([
            f"Inns {r.get('innings')}: {r.get('bowler')} - {r.get('wickets')}/{r.get('runs_conceded')} "
            f"in {overs(r.get('overs'))} (Econ {r.get('economy')})"
            for r in bw
        ]))

    ev = payload.get("evidence", {})
    if isinstance(ev, dict) and ev.get("match_id"):
        lines.append("")
        lines.append(f"(Match ID: {ev['match_id']})")

    return "\n".join(lines)

def format_player_stats(payload: Dict[str, Any]):
    """Render a player's batting/bowling overview plus matchup nuggets and team history."""
    if "error" in payload:
        if "choices" in payload and payload["choices"]:
            return payload["error"] + "\n" + header("Did you mean?") + "\n" + bullet_list(payload["choices"])
        return f"{payload['error']}"

    inp = payload.get("input", {}) or {}
    lines = [
        header(f"Player: {s(inp.get('resolved_name') or inp.get('player_query'))}"),
        f"Scope: {s(inp.get('scope'))}" + (f"  |  Season: {s(inp.get('season'))}" if inp.get('season') else "")
    ]

    bat = payload.get("batting", {}) or {}
    lines += [
        "",
        header("Batting"),
        f"Matches: {nz(bat.get('matches'))}  |  Inns: {nz(bat.get('inns'))}",
        f"Runs: {nz(bat.get('runs'))}  |  Balls: {nz(bat.get('balls'))}",
        f"4s/6s: {nz(bat.get('fours'))}/{nz(bat.get('sixes'))}",
        f"SR: {nz(bat.get('sr','-'))}  |  Avg: {nz(bat.get('average','-'))}",
    ]

    # Batting matchups
    m_bat = (payload.get("matchups", {}) or {}).get("batting", {}) or {}
    nb = m_bat.get("nemesis_bowler")
    fb = m_bat.get("favourite_bowler")
    if nb or fb:
        lines += ["", header("Batting Matchups")]
        if nb:
            lines.append(
                f"Nemesis bowler: {s(nb.get('bowler'))} - outs {nz(nb.get('outs'))}, "
                f"balls {nz(nb.get('balls'))}, econ vs {nz(nb.get('economy_against','-'))}"
            )
        if fb:
            lines.append(
                f"Favourite bowler: {s(fb.get('bowler'))} - balls {nz(fb.get('balls'))}, "
                f"econ {nz(fb.get('economy','-'))} (min 10 overs)"
            )

    bowl = payload.get("bowling", {}) or {}
    lines += [
        "",
        header("Bowling"),
        f"Matches: {nz(bowl.get('matches'))}",
        f"Overs: {overs(bowl.get('overs'))}  |  Wkts: {nz(bowl.get('wickets'))}",
        f"Runs Conceded: {nz(bowl.get('runs_conceded'))}  |  Econ: {nz(bowl.get('economy','-'))}",
    ]

    # Bowling matchups
    m_bowl = (payload.get("matchups", {}) or {}).get("bowling", {}) or {}
    md = m_bowl.get("most_dismissed_batter")
    ww = m_bowl.get("worst_vs_batter")
    if md or ww:
        lines += ["", header("Bowling Matchups")]
        if md:
            lines.append(
                f"Most dismissals: {s(md.get('batter'))} - outs {nz(md.get('outs'))}, "
                f"balls {nz(md.get('balls'))}, econ vs {nz(md.get('economy_against','-'))}"
            )
        if ww:
            lines.append(
                f"Worst econ vs batter: {s(ww.get('batter'))} - balls {nz(ww.get('balls'))}, "
                f"econ {nz(ww.get('economy','-'))} (min 10 overs)"
            )

    teams = payload.get("teams", []) or []
    last_team = payload.get("last_team") or {}
    if teams or last_team:
        lines += ["", header("Teams")]
        if teams:
            lines.append(", ".join(teams))
        if last_team:
            lt = f"{s(last_team.get('team'))}"
            ctx = []
            if last_team.get("date"):
                ctx.append(s(last_team.get("date")))
            if last_team.get("match_id"):
                ctx.append(f"ID {last_team.get('match_id')}")
            if ctx:
                lt += f"  (last appearance: {', '.join(ctx)})"
            lines.append(lt)

    return "\n".join(lines)

def format_team_squad(payload: Dict[str, Any]):
    """Render a team's season squad as a bulleted list."""
    if "error" in payload:
        return f"{payload['error']}"
    inp = payload.get("input", {}) or {}
    lines = [header(f"Squad: {s(inp.get('team') or inp.get('team_query'))} - {s(inp.get('season'))}")]
    squad = payload.get("squad", []) or []
    if not squad:
        lines.append("No players found.")
    else:
        lines.append(bullet_list([f"{s(p.get('player'))} ({nz(p.get('appearances','?'))} matches)" for p in squad]))
    return "\n".join(lines)

def format_player_vs_team(payload: Dict[str, Any]):
    """Summarize one player's batting/bowling vs a specific opponent (optionally per season)."""
    if "error" in payload:
        if "choices" in payload and payload["choices"]:
            return payload["error"] + "\n" + header("Did you mean?") + "\n" + bullet_list(payload["choices"])
        return f"{payload['error']}"
    inp = payload.get("input", {}) or {}
    title = f"{s(inp.get('resolved_name') or inp.get('player_query'))} vs {s(inp.get('opponent'))}"
    if inp.get("scope") == "season" and inp.get("season"):
        title += f" - {s(inp.get('season'))}"
    lines = [header(title)]
    bat = payload.get("batting_vs_team", {}) or {}
    lines += [
        "",
        header("Batting vs Opponent"),
        f"Runs: {nz(bat.get('runs'))}  |  Balls: {nz(bat.get('balls'))}  |  4s/6s: {nz(bat.get('fours'))}/{nz(bat.get('sixes'))}",
        f"SR: {nz(bat.get('sr','-'))}  |  Avg: {nz(bat.get('average','-'))}"
    ]
    bowl = payload.get("bowling_vs_team", {}) or {}
    lines += [
        "",
        header("Bowling vs Opponent"),
        f"Overs: {overs(bowl.get('overs'))}  |  Wkts: {nz(bowl.get('wickets'))}  |  Runs: {nz(bowl.get('runs_conceded'))}",
        f"Econ: {nz(bowl.get('economy','-'))}"
    ]
    return "\n".join(lines)

def format_head_to_head(payload: Dict[str, Any]):
    """Show H2H totals (matches/wins/ties/NR), first/last meeting, and star performers."""
    if "error" in payload:
        return f"{payload['error']}"
    summ = payload.get("summary", {}) or {}

    lines = [header("Head-to-Head")]
    wins_bits = []
    for k, v in summ.items():
        if k.startswith("wins_"):
            team = k.replace("wins_", "")
            wins_bits.append(f"{team}: {nz(v)}")
    lines.append(
        f"Matches: {nz(summ.get('matches'))} | Wins " + ", ".join(wins_bits) +
        f" | Ties: {nz(summ.get('ties', 0))} | No Result: {nz(summ.get('no_result', 0))}"
    )

    e = summ.get("earliest", {}) or {}
    l = summ.get("latest", {}) or {}
    if e or l:
        lines += [
            f"Earliest: {e.get('season','-')} {e.get('date','-')} (ID {e.get('match_id','-')})",
            f"Latest:   {l.get('season','-')} {l.get('date','-')} (ID {l.get('match_id','-')})"
        ]

    # Star performers per team
    stars = payload.get("star_performers", {}) or {}
    if stars:
        lines.append("")
        lines.append(header("Star Performers"))

        def fmt_bat(tag, rec):
            """Format batting star performer stats."""
            if not rec:
                return f"{tag}: -"
            return (
                f"{tag}: {rec.get('player')} - "
                f"Runs {nz(rec.get('runs'))}, Balls {nz(rec.get('balls'))}, "
                f"Avg {nz(rec.get('avg','-'))}, 50s {nz(rec.get('fifties'))}, 100s {nz(rec.get('hundreds'))}"
            )

        def fmt_bowl(tag, rec):
            """Format bowling star performer stats."""
            if not rec:
                return f"{tag}: -"
            balls = rec.get('balls') or 0
            overs_str = f"{balls//6}.{balls%6}" if balls else "-"
            return (
                f"{tag}: {rec.get('player')} - "
                f"Overs {overs_str}, Runs {nz(rec.get('runs_conceded'))}, "
                f"Econ {nz(rec.get('economy','-'))}, Wkts {nz(rec.get('wickets'))}"
            )

        for team, packs in stars.items():
            lines.append(f"\n{team}")
            lines.append(fmt_bat("Batting", packs.get("batting")))
            lines.append(fmt_bowl("Bowling", packs.get("bowling")))

    return "\n".join(lines)

def format_best_phase_bowlers(payload: Dict[str, Any]):
    """List best bowlers for a phase (PP/Middle/Death) with key rate stats."""
    if "error" in payload:
        return f"{payload['error']}"

    phase = payload.get("input", {}).get("phase", "")
    scope = payload.get("input", {}).get("scope", "career")
    season = payload.get("input", {}).get("season")
    min_overs = payload.get("input", {}).get("min_overs")

    phase_label = {"PP": "Powerplay", "Middle": "Middle", "Death": "Death"}.get(phase, phase)

    title = f"Best {phase_label} Bowlers - " + (f"IPL {season}" if scope == "season" and season else "IPL (All Seasons)")
    lines = [header(title), f"Minimum overs in phase: {min_overs}"]

    leaders = payload.get("leaders", []) or []
    if not leaders:
        lines.append("No qualifying bowlers.")
        return "\n".join(lines)

    lines.append("")
    for i, r in enumerate(leaders, 1):
        overs_val = r.get("overs") or "-"
        econ = nz(r.get("economy", "-"))
        avg = nz(r.get("average", "-"))
        sr = nz(r.get("strike_rate", "-"))
        dotp = nz(r.get("dot_pct", "-"))
        bdp = nz(r.get("boundary_pct", "-"))
        lines.append(
            f"{i}. {r['bowler']} - Overs {overs_val}, Wkts {nz(r.get('wickets'))}, "
            f"Runs {nz(r.get('runs_conceded'))}, Econ {econ}, Avg {avg}, SR {sr}, "
            f"Dot% {dotp}, Boundary% {bdp} (Matches {nz(r.get('matches'))})"
        )

    return "\n".join(lines)

def format_answer(intent: str, result: Dict[str, Any]):
    """Dispatch to the appropriate formatter based on intent/prompt."""
    mapping = {
        "match_summary": format_match_summary,
        "player_stats": format_player_stats,
        "team_squad": format_team_squad,
        "player_vs_team": format_player_vs_team,
        "head_to_head": format_head_to_head,
        "best_phase_bowler": format_best_phase_bowlers,
    }
    f = mapping.get(intent)
    if not f:
        return "I didn't understand that request."
    return f(result)
