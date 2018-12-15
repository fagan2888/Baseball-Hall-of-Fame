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

df = pd.merge(df_retro, df_elo, how='inner', left_on='game_id', right_on='game_id')
# Left join and inner join should be the same, but an inner join will be easier to check

df['end_game_flag'] = df['end_game_flag'].map({'T': 1, 'F':0})

# Validate by checking that df_retro and df have the same length
print('These dataframes should have the same number of rows, proving that there is a game entry for every event entry.')
print(df_retro.shape)
print(df.shape)

# Validate by seeing if final game scores from both datasets match
df_test = df.loc[df['end_game_flag'] == 1]
print('These dataframes should have length zero, proving that our event and game datasets have the same final score for every game.')
print(df_test.loc[df_test['f_home_runs']!=df_test['score1']].shape)
print(df_test.loc[df_test['f_away_runs']!=df_test['score2']].shape)

# Validate by making sure that away team match in both datasets
print('This dataframe should be empty, proving that the away team matches for every game.')
print(df_test.loc[df_test['away_team']!=df_test['team2']][['year', 'away_team', 'team2']].drop_duplicates())

# Split game and event level fields
df_game = df_retro[['game_id', 'top_bat_team', 'bot_bat_team']].drop_duplicates()
df_game = pd.merge(df_game, df_elo, how='right', left_on = 'game_id', right_on = 'game_id')
df_game.sample(200).to_csv('ignore\\large_data\\game_table_sample.csv', index=False)
df_game.to_csv('ignore\\large_data\\game_table.csv', index=False)

event_fields = list(df_retro)
for item in ['home_team', 'away_team', 'top_bat_team', 'bot_bat_team']:
    event_fields.remove(item)
df_event = df[event_fields]
df_event.sample(10000).to_csv('ignore\\large_data\\event_table_sample.csv', index=False)
df_event.to_csv('ignore\\large_data\\event_table.csv', index=False)
