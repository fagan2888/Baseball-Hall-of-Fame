# File: wrangle_retrosheet_pt2.py
# Date Created: 2018-11-10
# Author(s): Mahkah Wu
# Purpose: Extracts game state data from Retrosheet event files

import pandas as pd
import numpy as np
import datetime
import psycopg2
from ignore import db_cred

df = pd.DataFrame()

for i in range(1950, 2017):
    print("Appending {}: {}".format(i, datetime.datetime.now()))
    df_year = pd.read_csv('ignore\\large_data\\game_data_retrosheet2\\wrangled\\wrangled{}.csv'.format(i), usecols=range(1,41))
    df_year['year'] = i
    df = df.append(df_year)

# home_teams = df['home_team'].unique()
# Team ID's mostly match database
# Convert MLN -> ML1
team_fields = ['home_team', 'away_team', 'top_bat_team', 'bot_bat_team']
print('Mapping Team IDs: {}'.format(datetime.datetime.now()))
df['game_id'] = df['game_id'].str.replace(r'MLN([0-9]+)', r'ML1\1')
for col in team_fields:
    df[col].loc[df[col] == 'MLN'] = 'ML1'

# Convert MIL -> ML4 (1970-1997)
df['game_id'] = df['game_id'].str.replace(r'MIL(19[78][0-9]+)', r'ML4\1')
df['game_id'] = df['game_id'].str.replace(r'MIL(199[0-7][0-9]+)', r'ML4\1')
for col in team_fields:
    df[col].loc[(df[col] == 'MIL') & (df['year'] >= 1970) & (df['year'] <= 1997)] = 'ML4'

# Convert ANA -> LAA (2005-2016)
df['game_id'] = df['game_id'].str.replace(r'ANA(200[5-9][0-9]+)', r'LAA\1')
df['game_id'] = df['game_id'].str.replace(r'ANA(201[0-6][0-9]+)', r'LAA\1')
for col in team_fields:
    df[col].loc[(df[col] == 'ANA') & (df['year'] >= 2005) & (df['year'] <= 2016)] = 'LAA'


# Change retrosheet player ID's to db player ID's
query = '''SELECT "playerID", "retroID" FROM master'''
conn = db_cred.connect_db()
df_player_ids = pd.read_sql(query, conn)
dict_ids = df_player_ids.set_index('retroID').to_dict()['playerID']

player_fields = ['batter_id', 'pitcher_id', 'fielder_id', 'fielder2_id',
    'fielder3_id' , 'fielder4_id' ,'fielder5_id' , 'fielder6_id', 'fielder7_id',
     'fielder8_id', 'fielder9_id']

for field in player_fields:
    print('Mapping IDs in {}: {}'.format(field, datetime.datetime.now()))
    df[field] = df[field].map(dict_ids)

df.to_csv('ignore\\large_data\\game_data_retrosheet2\\combined_retrosheet.csv', index=False)
