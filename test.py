# -*- coding: utf-8 -*-
"""
@author: satvi
"""

#Just some internal testing
from resolver import match_summary
#out = match_summary("ipl_data.duckdb", "CSK", "MI", "2011", nth=1)
#print(out["meta"])
#print(out["innings"])
#print(out["top_batters"])
#print(out["top_bowlers"])

from resolver import player_stats, team_squad,player_vs_team, head_to_head

# Player career
#print(player_stats("ipl_data.duckdb", player="RG Sharma", scope="career"))

# Player single season
#print(player_stats("ipl_data.duckdb", player="SC Ganguly", scope="season", season="2012"))

# Team squad for a season
#print(team_squad("ipl_data.duckdb", team="Mumbai Indians", season="2009"))

#print(player_vs_team('ipl_data.duckdb', player='RG Sharma', opponent= "MI", scope="career"))

#print(head_to_head('ipl_data.duckdb',team_a='MI',team_b='CSK', scope='career'))
