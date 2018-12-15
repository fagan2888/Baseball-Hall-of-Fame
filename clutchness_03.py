# File: clutchness_02.py
# Date Created: 2018-12-11
# Author(s): Mahkah Wu
# Purpose: Uses win probability computed by clutchness_02 to produce clutchness measures

import pandas as pd
import seaborn as sns

df = pd.read_csv('ignore\predicted_wins2.csv')
df.drop('Unnamed: 0', axis=1, inplace=True)


df['delta_ens'] = df['f_win_ens'] - df['s_win_ens']
# https://www.wolframalpha.com/input/?i=itegrate+-(2x)%5E2%2B1
df['delta_int'] = ((df['f_win_ens']-0.5) - 4 * df['f_win_ens'] ** 3 / 3) - ((df['s_win_ens']-0.5) - 4 * df['s_win_ens'] ** 3 / 3)

df_batter = df.groupby(['batter_id', 'year'])[['delta_ens']].count()
df_batter['average_win_change'] = df.groupby(['batter_id', 'year'])[['delta_ens']].mean()
df_batter['center_weighted_win_change'] = df.groupby(['batter_id', 'year'])[['delta_int']].mean()
df_batter.reset_index(inplace=True)
df_batter.rename(index=str, columns={"delta_ens": "event_count"}, inplace=True)
df_batter.to_csv('ignore\\batter_clutch.csv', index=False)

df_pitch = df.groupby(['pitcher_id', 'year'])[['delta_ens']].count()
df_pitch['average_win_change'] = df.groupby(['pitcher_id', 'year'])[['delta_ens']].mean()
df_pitch['average_win_change'] = df_pitch['average_win_change'] * -1
df_pitch['center_weighted_win_change'] = df.groupby(['pitcher_id', 'year'])[['delta_int']].mean()
df_pitch['center_weighted_win_change'] = df_pitch['center_weighted_win_change'] * -1
df_pitch.rename(index=str, columns={"delta_ens": "event_count"}, inplace=True)
df_pitch.reset_index(inplace=True)
df_pitch.to_csv('ignore\\pitcher_clutch.csv', index=False)




sns_plot = sns.jointplot(x="s_win_prob_logit", y="s_win_prob_nb_iso", data=df, kind="kde")
sns_plot.savefig("figures\\compare_logit_iso_nb.png")
