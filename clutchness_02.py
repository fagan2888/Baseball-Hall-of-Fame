# File: clutchness_02.py
# Date Created: 2018-12-10
# Author(s): Mahkah Wu
# Purpose: Applies win probability models built by clutchness_01.ipynb





### Set Up
import psycopg2
import pandas as pd
import numpy as np
import pickle

from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import LinearSVC
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (brier_score_loss, precision_score, recall_score,
                             f1_score)
from sklearn.calibration import CalibratedClassifierCV, calibration_curve



# Import database credentials from file ignored by GitHub and set up connection
from ignore import db_cred
conn = db_cred.connect_db()

df = pd.read_csv('ignore\large_data\event_table.csv')
df_game = pd.read_csv('ignore\large_data\game_table.csv')


df = pd.merge(df, df_game, how='left', left_on='game_id', right_on='game_id')
del df_game

### Feature Engineering
# Game Level
df['home_team_won'] = 0
df.loc[df['score1'] > df['score2'], 'home_team_won'] = 1
df[['score1', 'score2', 'home_team_won']].sample(10)

df[(df['batting_team']!=df['team1']) &  (df['batting_team']!=df['team2'])][['batting_team', 'team1', 'team2', 'franchise1', 'franchise2']].shape

df['batting_team_won'] = df['home_team_won']
df.loc[df['batting_team'] == df['team2'], 'batting_team_won'] = (df['home_team_won'] + 1) % 2
df[['batting_team_won', 'batting_team', 'fielding_team', 'team1', 'team2', 'score1', 'score2']].sample(10)

df['win_prob'] = df['elo_prob1']
df.loc[df['batting_team'] == df['team2'], 'win_prob'] = df['elo_prob2']
df[['win_prob', 'batting_team', 'fielding_team', 'team1', 'team2', 'elo_prob1', 'elo_prob2']].sample(10)

df['year_0'] = 1
df['year_1'] = (df['year'] - df['year'].min()) / (df['year'].max() - df['year'].min())
df['year_2'] = 2 * df['year_1'] * df['year_1'] - df['year_0']
df['year_3'] = 2 * df['year_1'] * df['year_2'] - df['year_1']
df['year_4'] = 2 * df['year_1'] * df['year_3'] - df['year_2']
df['year_5'] = 2 * df['year_1'] * df['year_4'] - df['year_3']

df[['year', 'year_0', 'year_1', 'year_2', 'year_3', 'year_4', 'year_5']].sample(10)

# Event Level
df['total_s_outs'] = ((df['inning']-1) * 6 + df['bottom'] * 3 + df['s_outs'])/54
df[['inning', 'bottom', 's_outs', 'total_s_outs']].sample(10)

df['s_run_delta'] = df['s_batting_runs'] - df['s_fielding_runs']
df['s_run_delta_1'] = (df['s_run_delta'] - df['s_run_delta'].min()) / (df['s_run_delta'].max() - df['s_run_delta'].min())
df['s_run_delta_2'] = 2 * df['s_run_delta_1'] * df['s_run_delta_1'] - 1
df[['s_batting_runs', 's_fielding_runs', 's_run_delta', 's_run_delta_1', 's_run_delta_2']].sample(10)

df['s_win_prob_vanishing_ln'] = df['win_prob']/(1 + np.log(df['total_s_outs'] + 1))
df['s_win_prob_vanishing_1'] = df['win_prob']/(1 + df['total_s_outs'])
df['s_win_prob_vanishing_2'] = df['win_prob'] / ((1 + df['total_s_outs']) ** 2)
df[['total_s_outs', 'win_prob', 'win_prob', 's_win_prob_vanishing_ln', 's_win_prob_vanishing_1', 's_win_prob_vanishing_2']].sample(10)

df['total_f_outs'] = ((df['inning']-1) * 6 + df['bottom'] * 3 + df['f_outs'])/54
df[['inning', 'bottom', 'f_outs', 'total_f_outs']].sample(10)

df['f_run_delta'] = df['f_batting_runs'] - df['f_fielding_runs']
df['f_run_delta_1'] = (df['f_run_delta'] - df['s_run_delta'].min()) / (df['s_run_delta'].max() - df['s_run_delta'].min())
df['f_run_delta_2'] = 2 * df['f_run_delta_1'] * df['f_run_delta_1'] - 1
df[['f_batting_runs', 'f_fielding_runs', 'f_run_delta', 'f_run_delta_1', 'f_run_delta_2']].sample(10)

df['f_win_prob_vanishing_ln'] = df['win_prob']/(1 + np.log(df['total_f_outs'] + 1))
df['f_win_prob_vanishing_1'] = df['win_prob']/(1 + df['total_f_outs'])
df['f_win_prob_vanishing_2'] = df['win_prob'] / ((1 + df['total_f_outs']) ** 2)
df[['total_f_outs', 'win_prob', 'f_win_prob_vanishing_ln', 'f_win_prob_vanishing_1', 'f_win_prob_vanishing_2']].sample(10)

# Flip final on base to match wrangling bug of starting on base
df['f_base1'] = (df['f_base1'] + 1) % 2
df['f_base2'] = (df['f_base2'] + 1) % 2
df['f_base3'] = (df['f_base3'] + 1) % 2


### Apply win prob models
s_features = ['total_s_outs', 's_batting_runs', 's_fielding_runs', 's_run_delta_1', 's_run_delta_2',
           's_base1', 's_base2', 's_base3', 'win_prob', 'year_1', 'year_2', 'year_3', 'year_4', 'year_5',
          's_win_prob_vanishing_ln', 's_win_prob_vanishing_1', 's_win_prob_vanishing_2']
f_features = ['total_f_outs', 'f_batting_runs', 'f_fielding_runs', 'f_run_delta_1', 'f_run_delta_2',
           'f_base1', 'f_base2', 'f_base3', 'win_prob', 'year_1', 'year_2', 'year_3', 'year_4', 'year_5',
          'f_win_prob_vanishing_ln', 'f_win_prob_vanishing_1', 'f_win_prob_vanishing_2']

drop_cols = ['event_str', 'fielder2_id', 'fielder3_id', 'fielder4_id', 'fielder5_id', 'fielder6_id',
        'fielder7_id', 'fielder8_id', 'fielder9_id', 'top_bat_team', 'bot_bat_team', 'date',
        'season', 'neutral', 'playoff', 'team1', 'team2', 'elo1_pre', 'elo2_pre', 'elo_prob1',
        'elo_prob2', 'elo1_post', 'elo2_post', 'rating1_pre', 'rating2_pre', 'pitcher1',
        'pitcher2', 'pitcher1_rgs', 'pitcher2_rgs', 'pitcher1_adj', 'pitcher2_adj', 'rating_prob1',
        'rating_prob2', 'rating1_post', 'rating2_post', 'franchise1', 'franchise2']

df.drop(drop_cols, inplace=True, axis=1)

for i in range(0,12):
    print('Model {}'.format(i))
    pickleFile = open("win_prob_models\\logit_default_{}.pickle".format(i), 'rb')
    model = pickle.load(pickleFile)
    df['s_logit_{}'.format(i)] = [item[1] for item in model.predict_proba(df[s_features])]
    df['f_logit_{}'.format(i)] = [item[1] for item in model.predict_proba(df[f_features])]

    pickleFile = open("win_prob_models\\nb_default_isotonic_{}.pickle".format(i), 'rb')
    model = pickle.load(pickleFile)
    df['s_nb_iso_{}'.format(i)] = [item[1] for item in model.predict_proba(df[s_features])]
    df['f_nb_iso_{}'.format(i)] = [item[1] for item in model.predict_proba(df[f_features])]



#df.to_csv('ignore\\predicted_wins.csv')


drop_cols = ['f_win_prob_vanishing_2', 'f_base1', 'f_base3', 'f_base2', 's_batting_runs',
    's_base1', 's_outs', 'year_5', 'batting_team_won', 'f_fielding_runs', 'home_team_won',
    'f_outs', 'year_0', 'f_batting_runs', 'score2', 'f_win_prob_vanishing_ln',
    'f_home_runs', 'score1', 'year_1', 's_fielding_runs', 's_run_delta_2',
    's_win_prob_vanishing_ln', 's_run_delta_1', 'home_batting', 'year_3', 'f_win_prob_vanishing_1',
    's_base2', 'bottom', 's_base3', 's_win_prob_vanishing_1', 'fielding_team',
    'year_2', 's_win_prob_vanishing_2', 'f_run_delta_2', 's_away_runs', 'f_away_runs',
    'total_s_outs', 'win_prob', 's_run_delta', 'f_run_delta_1', 'year_4',
    'inning', 'batting_team', 's_home_runs']
df.drop(drop_cols, inplace=True, axis=1)


s_col_logit = []
s_col_nb_iso = []
f_col_logit = []
f_col_nb_iso = []
for i in range(0,12):
    s_col_logit.append('s_logit_{}'.format(i))
    s_col_nb_iso.append('s_nb_iso_{}'.format(i))
    f_col_logit.append('f_logit_{}'.format(i))
    f_col_nb_iso.append('f_nb_iso_{}'.format(i))

#col = s_col_logit + s_col_nb_iso + f_col_logit + f_col_nb_iso + ['game_id', 'event_id', 'year', 'batter_id', 'pitcher_id', 'fielder_id', 'end_game_flag', 'total_f_outs', 'f_run_delta']

df['s_win_prob_logit'] = df[s_col_logit].mean(axis=1)
df['f_win_prob_logit'] = df[f_col_logit].mean(axis=1)

df['s_win_prob_nb_iso'] = df[s_col_nb_iso].mean(axis=1)
df['f_win_prob_nb_iso'] = df[f_col_nb_iso].mean(axis=1)

df['s_win_ens'] = df[['s_win_prob_logit', 's_win_prob_nb_iso']].mean(axis=1)
df['f_win_ens'] = df[['f_win_prob_logit', 'f_win_prob_nb_iso']].mean(axis=1)

for col in ['f_win_prob_logit', 'f_win_prob_nb_iso', 'f_win_ens']:
    df.loc[df['end_game_flag']==1, col] = 1
    df.loc[(df['end_game_flag']==1) & (df['f_run_delta']<0), col] = 0
    df[['total_f_outs', 'f_run_delta', col]].sample(10)

df = df[['game_id', 'event_id', 'year', 'batter_id', 'pitcher_id', 'fielder_id', 's_win_prob_logit', 'f_win_prob_logit', 's_win_prob_nb_iso', 'f_win_prob_nb_iso', 's_win_ens', 'f_win_ens']]

df.to_csv('ignore\\predicted_wins2.csv')
