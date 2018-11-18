# File: wrangle_538.py
# Date Created: 2018-11-10
# Author(s): Mahkah Wu
# Purpose: Extracts team elo rankings from 538 file


import pandas as pd
import psycopg2
from ignore import db_cred


df = pd.read_csv('ignore\\large_data\\538\\mlb_elo.csv')

df = df[(df['season'] >= 1950) & (df['season'] < 2017)]
df = df.loc[df['playoff'].isnull()]

df.reset_index(drop=True, inplace=True)

# 538 uses franchise id rather than team id; fix that here
df['franchise1'] = df['team1']
df['franchise2'] = df['team2']

query = '''SELECT "yearID", "teamID", "franchID" FROM teams
WHERE "yearID">=1950'''
conn = db_cred.connect_db()
df_team_ids = pd.read_sql(query, conn)

df['team1'] = pd.merge(df, df_team_ids, how='left', left_on=['season', 'franchise1'], right_on=['yearID', 'franchID'])['teamID']
df['team2'] = pd.merge(df, df_team_ids, how='left', left_on=['season', 'franchise2'], right_on=['yearID', 'franchID'])['teamID']


# Create the game id field
df['game_id'] = df['team1'] + df['date'].str.replace('-', '')
df['game_id'] = df['game_id'] + df['game_id'].duplicated(keep=False).astype(int).astype(str)
idx = df['game_id'].loc[df['game_id'].duplicated(keep=False)].loc[df['game_id'].duplicated(keep='last')].index
df['game_id'].iloc[idx] = df['game_id'][idx].str.replace('1$', '2')
idx = df['game_id'].loc[df['game_id'].duplicated(keep=False)].loc[df['game_id'].duplicated(keep='last')].index
df['game_id'].iloc[idx] = df['game_id'][idx].str.replace('2$', '3')

# Deal with a couple game_id special cases
map = {'NYA200007080': 'NYA200007082',
    'NYN200007080': 'NYN200007081',
    'NYA200306280': 'NYA200306281',
    'NYN200306280': 'NYN200306282',
    'NYA200806270': 'NYA200806271',
    'NYN200806270': 'NYN200806272',
    'CIN201307230': 'CIN201307232',
    'SFN201307230': 'SFN201307231'}

df['game_id'] = df['game_id'].replace(map)

# Save data
df.to_csv('ignore\\large_data\\538\\wrangled_mlb_elo.csv', index=False)
