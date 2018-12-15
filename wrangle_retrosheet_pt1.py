# File: wrangle_retrosheet_pt1.py
# Date Created: 2018-11-10
# Author(s): Mahkah Wu
# Purpose: Extracts game state data from Retrosheet event files

import pandas as pd
import numpy as np
import datetime


def extract_game_state(df_event):
    df = pd.DataFrame(index=df_event.index)
    '''    Produces a dataframe with the following:
    Row: 1 for each game state
    Col: Game ID, Teams, Home Team, Game State (Score, Innings, Bases, Outs, Runners on),
    Batter, Pitcher, Next Game State

    Event String and Fielders might be useful later so we'll take that too

    We'll directly take the fields we can.
    Three options for next game state:
      1. Might be sufficent columns
      2. Try to parse the event string
      3. Look at the next row
    Looks like option #1 will work'''

    # Relevent fields in event files:
    # GAME_ID           Home Team,Year,Month,Day,Dame Number
    # AWAY_TEAM_ID      Away Team
    # INN_CT            Inning
    # BAT_HOME_ID       Whether home team is batting
    # OUTS_CT           Outs (At beginning of event)
    # BALLS_CT          Balls (Just before end of event)
    # STRIKES_CT        Strikes (Just before end of event)
    # PITCH_SEQ_TX      Pitch code
    # AWAY_SCORE_CT     Away score (At beginning of event)
    # HOME_SCORE_CT     Home score (At beginning of event)
    # BAT_ID            Batter ID
    # PIT_ID            Pitcher ID
    # POS2_FLD_ID       Fielder 2 ID
    # POS3_FLD_ID       Fielder 3 ID
    # POS4_FLD_ID       Fielder 4 ID
    # POS5_FLD_ID       Fielder 5 ID
    # POS6_FLD_ID       Fielder 6 ID
    # POS7_FLD_ID       Fielder 7 ID
    # POS8_FLD_ID       Fielder 8 ID
    # POS9_FLD_ID       Fielder 9 ID
    # BASE1_RUN_ID      Runner on first
    # BASE2_RUN_ID      Runner on second
    # BASE3_RUN_ID      Runner on third
    # EVENT_TX          Event code
    # EVENT_OUTS_CT     Outs that occured on play
    # RBI_CT            Runs batted in off event
    # BAT_DEST_ID       Batter Result
    # RUN1_DEST_ID      First Runner Result
    # RUN2_DEST_ID      Second Runner Results
    # RUN3_DEST_ID      Third Runner Results
    # GAME_NEW_FL       Game begins
    # GAME_END_FL       Game ends
    # EVENT_ID          Events
    # HOME_TEAM_ID      Home team
    # BAT_TEAM_ID       Batting team
    # FLD_TEAM_ID       Fielding team
    # BAT_LAST_ID       Top or bottom of the inning
    # INN_NEW_FL        Inning begins
    # INN_END_FL        Inning ends
    # FLD_ID            Fielding Player ID

    # Game Metadata
    print('\tGame Metadata: {}'.format(datetime.datetime.now()))
    df['game_id'] = df_event['GAME_ID']
    df['home_team'] = df_event['HOME_TEAM_ID']
    df['away_team'] = df_event['AWAY_TEAM_ID']
    df['top_bat_team'] = df_event['BAT_TEAM_ID']
    df['top_bat_team'].loc[df_event['BAT_LAST_ID'] == 1] = df_event['FLD_TEAM_ID']
    df['bot_bat_team'] = df_event['FLD_TEAM_ID']
    df['bot_bat_team'].loc[df_event['BAT_LAST_ID'] == 1] = df_event['BAT_TEAM_ID']
    # Lesson learned: don't use np.where on large dataframes

    # Event Metadata
    print('\tEvent Metadata: {}'.format(datetime.datetime.now()))
    df['event_id'] = df_event['EVENT_ID']
    df['batting_team'] = df_event['BAT_TEAM_ID']
    df['fielding_team'] = df_event['FLD_TEAM_ID']
    df['inning'] = df_event['INN_CT']
    df['bottom'] = df_event['BAT_LAST_ID']
    df['home_batting'] = 0
    df['home_batting'].loc[df_event['HOME_TEAM_ID']==df_event['BAT_TEAM_ID']] = 1

    df['batter_id'] = df_event['BAT_ID']
    df['pitcher_id'] = df_event['PIT_ID']

    # Start State
    print('\tStart State: {}'.format(datetime.datetime.now()))
    df['s_home_runs'] = df_event['HOME_SCORE_CT']
    df['s_away_runs'] = df_event['AWAY_SCORE_CT']
    df['s_batting_runs'] = df['s_home_runs']
    df['s_batting_runs'].loc[df['home_batting'] == 0] = df['s_away_runs']
    df['s_fielding_runs'] = df['s_home_runs']
    df['s_fielding_runs'].loc[df['home_batting'] == 1] = df['s_away_runs']

    df['s_outs'] = df_event['OUTS_CT']
    # These should be is not null, but discovered late, so f_base* were flipped in model application
    df['s_base1'] = 0
    df['s_base1'].loc[df_event['BASE1_RUN_ID'].isnull()] = 1
    df['s_base2'] = 0
    df['s_base2'].loc[df_event['BASE2_RUN_ID'].isnull()] = 1
    df['s_base3'] = 0
    df['s_base3'].loc[df_event['BASE3_RUN_ID'].isnull()] = 1

    # End State
    print('\tEnd State: {}'.format(datetime.datetime.now()))
    df['f_batting_runs'] = df['s_batting_runs']
    df['f_batting_runs'].loc[df_event['BAT_DEST_ID'] >= 4] += 1
    df['f_batting_runs'].loc[df_event['RUN1_DEST_ID'] >= 4] += 1
    df['f_batting_runs'].loc[df_event['RUN2_DEST_ID'] >= 4] += 1
    df['f_batting_runs'].loc[df_event['RUN3_DEST_ID'] >= 4] += 1
    df['f_fielding_runs'] = df['s_fielding_runs']
    df['f_home_runs'] = df['f_batting_runs']
    df['f_home_runs'].loc[df['home_batting'] == 0] = df['f_fielding_runs']
    df['f_away_runs'] = df['f_batting_runs']
    df['f_away_runs'].loc[df['home_batting'] == 1] = df['f_fielding_runs']

    df['f_outs'] = df['s_outs'] + df_event['EVENT_OUTS_CT']
    df['f_base1'] = 0
    df['f_base1'].loc[(df_event['BAT_DEST_ID'] == 1) | (df_event['RUN1_DEST_ID'] == 1)] = 1
    df['f_base2'] = 0
    df['f_base2'].loc[(df_event['BAT_DEST_ID'] == 2) | (df_event['RUN1_DEST_ID'] == 2) | (df_event['RUN2_DEST_ID'] == 2)] = 1
    df['f_base3'] = 0
    df['f_base3'].loc[(df_event['BAT_DEST_ID'] == 3) | (df_event['RUN1_DEST_ID'] == 3) | (df_event['RUN2_DEST_ID'] == 3) | (df_event['RUN3_DEST_ID'] == 3)] = 1

    df['end_game_flag'] = df_event['GAME_END_FL']

    # Errata
    print('\tErrata: {}'.format(datetime.datetime.now()))
    df['event_str'] = df_event['EVENT_TX']
    df['fielder2_id'] = df_event['POS2_FLD_ID']
    df['fielder3_id'] = df_event['POS3_FLD_ID']
    df['fielder4_id'] = df_event['POS4_FLD_ID']
    df['fielder5_id'] = df_event['POS5_FLD_ID']
    df['fielder6_id'] = df_event['POS6_FLD_ID']
    df['fielder7_id'] = df_event['POS7_FLD_ID']
    df['fielder8_id'] = df_event['POS8_FLD_ID']
    df['fielder9_id'] = df_event['POS9_FLD_ID']
    df['fielder_id'] = df_event['FLD_ID']

    return df


dtypes = {
    'GAME_ID': object,
    'AWAY_TEAM_ID': object,
    'INN_CT': np.int64,
    'BAT_HOME_ID': np.int64,
    'OUTS_CT': np.int64,
    'BALLS_CT': np.int64,
    'STRIKES_CT': np.int64,
    'PITCH_SEQ_TX': object,
    'AWAY_SCORE_CT': np.int64,
    'HOME_SCORE_CT': np.int64,
    'BAT_ID': object,
    'BAT_HAND_CD': object,
    'RESP_BAT_ID': object,
    'RESP_BAT_HAND_CD': object,
    'PIT_ID': object,
    'PIT_HAND_CD': object,
    'RESP_PIT_ID': object,
    'RESP_PIT_HAND_CD': object,
    'POS2_FLD_ID': object,
    'POS3_FLD_ID': object,
    'POS4_FLD_ID': object,
    'POS5_FLD_ID': object,
    'POS6_FLD_ID': object,
    'POS7_FLD_ID': object,
    'POS8_FLD_ID': object,
    'POS9_FLD_ID': object,
    'BASE1_RUN_ID': object,
    'BASE2_RUN_ID': object,
    'BASE3_RUN_ID': object,
    'EVENT_TX': object,
    'LEADOFF_FL': object,
    'PH_FL': object,
    'BAT_FLD_CD': np.int64,
    'BAT_LINEUP_ID': np.int64,
    'EVENT_CD': np.int64,
    'BAT_EVENT_FL': object,
    'AB_FL': object,
    'H_CD': np.int64,
    'SH_FL': object,
    'SF_FL': object,
    'EVENT_OUTS_CT': np.int64,
    'DP_FL': object,
    'TP_FL': object,
    'RBI_CT': np.int64,
    'WP_FL': object,
    'PB_FL': object,
    'FLD_CD': np.int64,
    'BATTEDBALL_CD': object,
    'BUNT_FL': object,
    'FOUL_FL': object,
    'BATTEDBALL_LOC_TX': object,
    'ERR_CT': np.int64,
    'ERR1_FLD_CD': np.int64,
    'ERR1_CD': object,
    'ERR2_FLD_CD': np.int64,
    'ERR2_CD': object,
    'ERR3_FLD_CD': np.int64,
    'ERR3_CD': object,
    'BAT_DEST_ID': np.int64,
    'RUN1_DEST_ID': np.int64,
    'RUN2_DEST_ID': np.int64,
    'RUN3_DEST_ID': np.int64,
    'BAT_PLAY_TX': np.float64,
    'RUN1_PLAY_TX': object,
    'RUN2_PLAY_TX': object,
    'RUN3_PLAY_TX': object,
    'RUN1_SB_FL': object,
    'RUN2_SB_FL': object,
    'RUN3_SB_FL': object,
    'RUN1_CS_FL': object,
    'RUN2_CS_FL': object,
    'RUN3_CS_FL': object,
    'RUN1_PK_FL': object,
    'RUN2_PK_FL': object,
    'RUN3_PK_FL': object,
    'RUN1_RESP_PIT_ID': object,
    'RUN2_RESP_PIT_ID': object,
    'RUN3_RESP_PIT_ID': object,
    'GAME_NEW_FL': object,
    'GAME_END_FL': object,
    'PR_RUN1_FL': object,
    'PR_RUN2_FL': object,
    'PR_RUN3_FL': object,
    'REMOVED_FOR_PR_RUN1_ID': object,
    'REMOVED_FOR_PR_RUN2_ID': object,
    'REMOVED_FOR_PR_RUN3_ID': object,
    'REMOVED_FOR_PH_BAT_ID': object,
    'REMOVED_FOR_PH_BAT_FLD_CD': np.int64,
    'PO1_FLD_CD': np.int64,
    'PO2_FLD_CD': np.int64,
    'PO3_FLD_CD': np.int64,
    'ASS1_FLD_CD': np.int64,
    'ASS2_FLD_CD': np.int64,
    'ASS3_FLD_CD': np.int64,
    'ASS4_FLD_CD': np.int64,
    'ASS5_FLD_CD': np.int64,
    'EVENT_ID': np.int64,
    'HOME_TEAM_ID': object,
    'BAT_TEAM_ID': object,
    'FLD_TEAM_ID': object,
    'BAT_LAST_ID': np.int64,
    'INN_NEW_FL': object,
    'INN_END_FL': object,
    'START_BAT_SCORE_CT': np.int64,
    'START_FLD_SCORE_CT': np.int64,
    'INN_RUNS_CT': np.int64,
    'GAME_PA_CT': np.int64,
    'INN_PA_CT': np.int64,
    'PA_NEW_FL': object,
    'PA_TRUNC_FL': object,
    'START_BASES_CD': np.int64,
    'END_BASES_CD': np.int64,
    'BAT_START_FL': object,
    'RESP_BAT_START_FL': object,
    'BAT_ON_DECK_ID': object,
    'BAT_IN_HOLD_ID': object,
    'PIT_START_FL': object,
    'RESP_PIT_START_FL': object,
    'RUN1_FLD_CD': np.int64,
    'RUN1_LINEUP_CD': np.int64,
    'RUN1_ORIGIN_EVENT_ID': np.int64,
    'RUN2_FLD_CD': np.int64,
    'RUN2_LINEUP_CD': np.int64,
    'RUN2_ORIGIN_EVENT_ID': np.int64,
    'RUN3_FLD_CD': np.int64,
    'RUN3_LINEUP_CD': np.int64,
    'RUN3_ORIGIN_EVENT_ID': np.int64,
    'RUN1_RESP_CAT_ID': object,
    'RUN2_RESP_CAT_ID': object,
    'RUN3_RESP_CAT_ID': object,
    'PA_BALL_CT': np.int64,
    'PA_CALLED_BALL_CT': np.int64,
    'PA_INTENT_BALL_CT': np.int64,
    'PA_PITCHOUT_BALL_CT': np.int64,
    'PA_HITBATTER_BALL_CT': np.int64,
    'PA_OTHER_BALL_CT': np.int64,
    'PA_STRIKE_CT': np.int64,
    'PA_CALLED_STRIKE_CT': np.int64,
    'PA_SWINGMISS_STRIKE_CT': np.int64,
    'PA_FOUL_STRIKE_CT': np.int64,
    'PA_INPLAY_STRIKE_CT': np.int64,
    'PA_OTHER_STRIKE_CT': np.int64,
    'EVENT_RUNS_CT': np.int64,
    'FLD_ID': object,
    'BASE2_FORCE_FL': object,
    'BASE3_FORCE_FL': object,
    'BASE4_FORCE_FL': object,
    'BAT_SAFE_ERR_FL': object,
    'BAT_FATE_ID': np.int64,
    'RUN1_FATE_ID': np.int64}



if __name__ == '__main__':

    for i in range(1950, 2017):
        print('Wrangling {}: {}'.format(i, datetime.datetime.now()))
        df_event = pd.read_csv('ignore\\large_data\\game_data_retrosheet2\\parsed\\all{}.csv'.format(i), dtype=dtypes)
        df = extract_game_state(df_event)
        df.to_csv('ignore\\large_data\\game_data_retrosheet2\\wrangled\\wrangled{}.csv'.format(i))
