# File: build_game_tables.py
# Date Created: 2018-11-10
# Author(s): Mahkah Wu
# Purpose: Validates retrosheet and 538 wrangling against each other and builds db tables



import pandas as pd
import numpy as np
import datetime
import psycopg2
from ignore import db_cred



df_retro = pd.read_csv('ignore\\large_data\\game_data_retrosheet2\\combined_retrosheet.csv')
df_elo = pd.read_csv('ignore\\large_data\\538\\wrangled_mlb_elo.csv')

df = pd.merge(df_retro, df_elo, how='left', left_on='game_id', right_on='game_id')

df['end_game_flag'] = df['end_game_flag'].map({'T': 1, 'F':0})

# Validate by checking that df_retro and df have the same length
df_retro.shape
df.shape

# Validate by seeing if final game scores from both datasets match
df_test = df.loc[df['end_game_flag'] == 1]
print(df_test.loc[df_test['f_home_runs']!=df_test['score1']].shape)
print(df_test.loc[df_test['f_away_runs']!=df_test['score2']].shape)

# Validate by making sure that away team match in both datasets
print(df_test.loc[df_test['away_team']!=df_test['team2']][['year', 'away_team', 'team2']].drop_duplicates())

# Split game and event level fields
game_fields = list(df_elo) + ['game_id', 'home_team', 'away_team', 'top_bat_team', 'bot_bat_team']
df_game = df[game_fields]

event_fields = list(df_retro) - ['game_id', 'home_team', 'away_team', 'top_bat_team', 'bot_bat_team']
df_event = df[event_fields]
