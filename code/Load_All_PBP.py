# Pandas
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
import numpy as np

# Polars (Arrow)
from pyarrow.dataset import dataset
import polars as pl
pl.Config.set_tbl_rows(n=-1)
pl.Config.set_tbl_cols(n=-1)

# Hit API
import requests

# Tools
from itertools import chain
from datetime import datetime, timedelta
from math import pi
import time
import statistics

# Save
import pickle
import json
import os
import pathlib

# System Stats
import psutil
import timeit


# Path
roster_file = 'Data/NHL_Rosters_2014_2024.csv'

# All Players - Connect To event_player_1_id, event_player_2_id, event_player_3_id, event_player_4_id, event_goalie_id, home_goalie, away_goalie
ROSTER_DF_RAW = pl.read_csv(roster_file)

ROSTER_DF = (
    ROSTER_DF_RAW
    .with_columns([
        pl.col("player_id").cast(pl.Int32),
        (pl.col("first_name").str.to_uppercase() + '.' + pl.col("last_name").str.to_uppercase()).alias('player_name'),
        pl.when((pl.col('pos_G') == 1) & (pl.col('hand_R') == 1)).then(pl.lit(1)).otherwise(pl.lit(0)).alias('G_hand_R'),
        pl.when((pl.col('pos_G') == 1) & (pl.col('hand_L') == 1)).then(pl.lit(1)).otherwise(pl.lit(0)).alias('G_hand_L')
        ])
    .select(['player_id', 'player_name', 'hand_R', 'hand_L', 'pos_F', 'pos_D', 'pos_G', 'G_hand_R', 'G_hand_L'])
    .unique()
)

#ROSTER_DF.head()

### END ROSTER LOAD ###

### PREPROCESSING FUNCTIONS - DEFINE ###

# 1) Create Schema For API to Clean Transformation
raw_schema = {
    'id': 'i32',
    'gameDate': 'str',
    'season': 'i32',
    'sortOrder': 'i32',
    'gameType': 'i32',
    'period': 'i32',
    'periodType': 'str',
    'timeRemaining': 'str',
    'timeInPeriod': 'str',
    'situationCode': 'str',
    'homeTeamDefendingSide': 'str',
    'eventOwnerTeamId': 'str',
    'awayTeam.id': 'str',
    'awayTeam.abbrev': 'str',
    'awayScore': 'f32',
    'homeTeam.id': 'str',
    'homeTeam.abbrev': 'str',
    'homeScore': 'f32',
    'eventId': 'i32',
    'typeCode': 'i32',
    'penaltytTypeCode': 'str',
    'typeDescKey': 'str',
    'descKey': 'str',
    'reason': 'str',
    'secondaryReason': 'str',
    'shotType': 'str',
    'zoneCode': 'str',
    'xCoord': 'f32',
    'yCoord': 'f32',
    'scoringPlayerId': 'str',
    'shootingPlayerId': 'str',
    'goalieInNetId': 'str',
    'blockingPlayerId': 'str',
    'committedByPlayerId': 'str',
    'drawnByPlayerId': 'str',
    'servedByPlayerId': 'str',
    'duration': 'str',
    'hittingPlayerId': 'str',
    'hitteePlayerId': 'str',
    'winningPlayerId': 'str',
    'losingPlayerId': 'str',
    'assist1PlayerId': 'str',
    'assist2PlayerId': 'str',
    'playerId': 'str'    
}

# 2) FUNCTION: Create Connection To NHL API
def ping_nhl_api(i):
    """This function will get the raw data from the NHL API and normalize 'details'
    to ensure we collect every detail from each event"""

    # 1) Create Link For API Endpoint
    pbp_link = 'https://api-web.nhle.com/v1/gamecenter/'+str(i)+'/play-by-play'

    # 2) Get Game Data From Response
    pbp_response = requests.get(pbp_link).json()
    game_data = pl.DataFrame({
            'id': pbp_response.get('id'),
            'season': pbp_response.get('season'),
            'gameDate': pbp_response.get('gameDate'),
            'gameType': pbp_response.get('gameType'),
            'awayTeam.id': pbp_response.get('awayTeam', {}).get('id'),
            'awayTeam.abbrev': pbp_response.get('awayTeam', {}).get('abbrev'),
            'homeTeam.id': pbp_response.get('homeTeam', {}).get('id'),
            'homeTeam.abbrev': pbp_response.get('homeTeam', {}).get('abbrev'),
            'key': 1
        })
    # 3) Get Plays Data (Stored As List)
    raw_list = pbp_response.get('plays', [])

    # 4) Normalize Details Dictionary
    ### a) Create Keys To Compare Set
    keys_to_compare = {
        'descKey','reason','secondaryReason','shotType', #Event Description
        'xCoord','yCoord','zoneCode', # Location
        'homeScore','awayScore','homeSOG','awaySOG', 'scoringPlayerTotal','assist1PlayerTotal','assist2PlayerTotal', # Game Details
        'eventOwnerTeamId', # Team ID
        'goalieInNetId','scoringPlayerId','assist1PlayerId','assist2PlayerId','shootingPlayerId','blockingPlayerId', # Player IDs (Shots)
        'winningPlayerId','losingPlayerId','hittingPlayerId','hitteePlayerId','playerId', # Faceoff/Hit/GiveTakeAway Player IDs
        'typeCode', 'committedByPlayerId', 'drawnByPlayerId','servedByPlayerId','duration' # Penalty IDs
    }
    ### b) Loop Through Rows To Get Complete Details Dictionary 
    for entry in raw_list:
        details_dict = entry.get("details", {})

        ##### i) Check For Extra Keys
        extra_keys = set(details_dict.keys()) - keys_to_compare
        if extra_keys:
            print(f"GameID: {i} | Extra keys in details_dict: {extra_keys}")
        
        ##### ii) Update Details To Include All Keys
        entry["details"] = {key: details_dict.get(key, None) for key in keys_to_compare}

    # 5) Build New DataFrame With Full Details
    plays_raw =  (
        pl.DataFrame(raw_list)
        .rename({"typeCode":"eventTypeCode"})
        .unnest('periodDescriptor')
        .unnest('details')
        .rename({"typeCode":"penaltyTypeCode","eventTypeCode":"typeCode"})
        .with_columns(pl.lit(1).cast(pl.Int64).alias('key'))
    )
    
    return_df = game_data.join(plays_raw, on=pl.col("key"), how="inner").drop("key")

    return return_df

# 3) FUNCTION: Normalize Schema
def align_and_cast_columns(data, sch):
    # Identify missing and extra columns
    missing_cols_int = set(sch.keys()) - set(data.columns)
    extra_cols_int = set(data.columns) - set(sch.keys())
    data = data.drop(extra_cols_int)

    # Fill missing columns with null values and cast to the correct type
    for col in sch.keys():

        col_type = sch.get(col)

        if (col in data.columns) & (col_type == 'str'):
            data = data.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
        elif (col in data.columns) & (col_type == 'i32'):
            data = data.with_columns(pl.col(col).cast(pl.Int32).alias(col))
        elif (col in data.columns) & (col_type == 'f32'):
            data = data.with_columns(pl.col(col).cast(pl.Float32).alias(col))
        elif (col not in data.columns) & (col_type == 'str'):
            data = data.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
        elif (col not in data.columns) & (col_type == 'f32'):
            data = data.with_columns(pl.lit(None).cast(pl.Float32).alias(col))

    # Select columns and update schema
    data = data.select(sch.keys())

    return data

# 4) FUNCTION: String Period Time to Numeric Seconds Function
def min_to_sec(time_str):
    """This function will help to convert time's formatted like MM:SS to a round seconds number"""
    if time_str is None:
        return None
    
    minutes, seconds = map(int, time_str.split(':'))
    return minutes * 60 + seconds

# 5) FUNCTION: Reconcile New API Columns/Data To Previous Format + Additional Feature Columns
def reconcile_api_data(data):
    """ This Function will take a polars dataframe and reconcile column names, values, and data types to match SDV cleaning functions to save time and effort in building more tweak functions"""

    # Create Dictionaries For Column Name/Value Rename
    rename_dict = {
        "id": "game_id",
        "gameDate": "game_date",
        "awayTeam.id": "away_id",
        "awayTeam.abbrev": "away_abbreviation",
        "homeTeam.id": "home_id",
        "homeTeam.abbrev": "home_abbreviation",
        "gameType": "season_type",
        "eventId": "event_id",
        "typeDescKey": "event_type",
        "sortOrder": "event_idx",
        "periodType": "period_type",
        "eventOwnerTeamId": "event_team_id",
        "xCoord": "x",
        "yCoord": "y",
        "zoneCode": "event_zone",
        "shotType": "secondary_type",
        "awayScore": "away_score",
        "homeScore": "home_score",
        "goalieInNetId": "event_goalie_id",
        "blockingPlayerId": "blocking_player_id",
        "drawnByPlayerId": "drawnby_player_id",
        "servedByPlayerId": "servedby_player_id",
        "committedByPlayerId": "committedby_player_id",
        "hittingPlayerId": "hitting_player_id",
        "hitteePlayerId": "hittee_player_id",
        "assist1PlayerId": "assist_1_player_id",
        "assist2PlayerId": "assist_2_player_id",
        "shootingPlayerId": "shooting_player_id",
        "reason": "reason",
        "scoringPlayerId": "scoring_player_id",
        "duration": "penalty_minutes",
        "winningPlayerId": "winning_player_id",
        "losingPlayerId": "losing_player_id"
    }

    # Event Type
    event_type_dict = {
        "faceoff": "FACEOFF",
        "shot-on-goal": "SHOT",
        "stoppage": "STOPPAGE",
        "hit": "HIT",
        "blocked-shot": "BLOCKED_SHOT",
        "missed-shot": "MISSED_SHOT",
        "giveaway": "GIVEAWAY",
        "takeaway": "TAKEAWAY",
        "penalty": "PENALTY",
        "goal": "GOAL",
        "period-start": "PERIOD_START",
        "period-end": "PERIOD_END",
        "delayed-penalty": "DELAYED_PENALTY",
        "game-end": "GAME_END",
        "shootout-complete": "SHOOTOUT_COMPLETE",
        "failed-shot-attempt": "FAILED_SHOT",
        None:None
    }

    # Season Type
    season_type_dict = {
        2: "R",
        3: "P",
        None:None
    }

    # Shot Type
    shot_type_dict = {
        "snap": "Snap",
        "between-legs": "Between Legs",
        "wrap-around": "Wrap-Around",
        "tip-in": "Tip-In",
        "cradle": "Wrap-Around",
        "poke": 'Poked',
        "bat": 'Batted',
        "deflected": "Deflected",
        "wrist": "Wrist",
        "slap":	"Slap",
        "backhand": "Backhand",
        None: None
    }

    # Rename Columns + Values AND Add Event/Season Type Helpers
    data = data.rename(rename_dict).filter((pl.col('period_type') != 'SO') & (pl.col('season_type').is_in([2, 3])))

    data = (
        data
        .with_columns([
            (pl.col('season_type').map_dict(season_type_dict, default = pl.col('season_type'))).alias('season_type'),
            (pl.col('event_type').map_dict(event_type_dict,default = pl.col('event_type'))).alias('event_type'),
            (pl.col('secondary_type').map_dict(shot_type_dict,default = pl.col('secondary_type'))).alias('secondary_type'),
            pl.when(pl.col('event_team_id') == pl.col('home_id')).then(pl.lit('home')).otherwise(pl.lit('away')).alias('event_team_type'),
            pl.when(pl.col('event_team_id') == pl.col('home_id')).then(pl.col('home_abbreviation')).otherwise(pl.col('away_abbreviation')).alias('event_team_abbr')
            ])
        #.drop('gameType', 'typeDescKey', 'shotType')
        .filter(~pl.col('situationCode').is_in(["PERIOD_START", "PERIOD_END", "GAME_START", "GAME_END"]))
    )

    # Create Game and Period Seconds Remaining from timeInPeriod, timeRemaining: 'period', 'period_seconds', 'period_seconds_remaining', 'game_seconds', 'game_seconds_remaining'
    data = (
        data
        .with_columns(pl.when(pl.col('timeInPeriod').is_null()).then(pl.lit(None)).otherwise(pl.col('timeInPeriod').apply(min_to_sec)).alias('period_seconds'))
        .with_columns([
            (1200 - pl.col('period_seconds')).alias('period_seconds_remaining'),
            (pl.col('period_seconds') + ((pl.col('period')-1)*1200)).alias('game_seconds'),
            ((3600 - pl.col('period_seconds')) + ((pl.col('period') - 3) * 1200)).alias('game_seconds_remaining')
        ])
    )

    # Create event_player_1_id and event_player_2_id columns based on event_type and corresponding columns
    remove_ply_ids = ['winning_player_id', 'hitting_player_id', 'scoring_player_id', 'shooting_player_id', 'committedby_player_id',
                      'playerId', 'losing_player_id', 'hittee_player_id', 'drawnby_player_id', 'assist_1_player_id', 'assist_2_player_id',
                      'blocking_player_id']
    data = (
        data
        .with_columns([
            (pl.when(pl.col('event_type') == 'FACEOFF').then(pl.col('winning_player_id'))
               .when(pl.col('event_type') == 'HIT').then(pl.col('hitting_player_id'))
               .when(pl.col('event_type') == 'GOAL').then(pl.col('scoring_player_id'))
               .when(pl.col('event_type').is_in(['SHOT', 'MISSED_SHOT', "BLOCKED_SHOT"])).then(pl.col('shooting_player_id'))
               .when(pl.col('event_type') == 'PENALTY').then(pl.col('committedby_player_id'))
               .when(pl.col('event_type') == 'GIVEAWAY').then(pl.col('playerId'))
               .when(pl.col('event_type') == 'TAKEAWAY').then(pl.col('playerId'))
               .otherwise(pl.lit(None))
             ).alias("event_player_1_id"),
             (pl.when(pl.col('event_type') == 'FACEOFF').then(pl.col('losing_player_id'))
               .when(pl.col('event_type') == 'HIT').then(pl.col('hittee_player_id'))
               .when(pl.col('event_type').is_in(['GOAL','SHOT', 'MISSED_SHOT', 'BLOCKED_SHOT'])).then(pl.col('event_goalie_id'))
               .when(pl.col('event_type') == 'PENALTY').then(pl.col('drawnby_player_id'))
               .otherwise(pl.lit(None))
             ).alias("event_player_2_id"),
             (pl.when((pl.col('event_type') == 'GOAL') & (~pl.col('assist_1_player_id').is_null())).then(pl.col('assist_1_player_id'))
               .when((pl.col('event_type') == 'PENALTY') & (~pl.col('servedby_player_id').is_null())).then(pl.col('servedby_player_id'))
               .when((pl.col('event_type') == 'BLOCKED_SHOT') & (~pl.col('blocking_player_id').is_null())).then(pl.col('blocking_player_id'))
               .otherwise(pl.lit(None))
             ).alias("event_player_3_id"),
             (pl.when((pl.col('event_type') == 'GOAL') & (~pl.col('assist_2_player_id').is_null())).then(pl.col('assist_2_player_id'))
               .otherwise(pl.lit(None))
             ).alias("event_player_4_id"),
             (pl.when(pl.col('event_type') == 'FACEOFF').then(pl.lit('Winner'))
               .when(pl.col('event_type') == 'HIT').then(pl.lit('Hitter'))
               .when(pl.col('event_type') == 'GOAL').then(pl.lit('Scorer'))
               .when(pl.col('event_type').is_in(['SHOT', 'MISSED_SHOT', "BLOCKED_SHOT"])).then(pl.lit('Shooter'))
               .when(pl.col('event_type') == 'PENALTY').then(pl.lit('PenaltyOn'))
               .when(pl.col('event_type') == 'GIVEAWAY').then(pl.lit('PlayerID'))
               .when(pl.col('event_type') == 'TAKEAWAY').then(pl.lit('PlayerID'))
               .otherwise(pl.lit(None))
             ).alias("event_player_1_type"),
             (pl.when(pl.col('event_type') == 'FACEOFF').then(pl.lit('Loser'))
               .when(pl.col('event_type') == 'HIT').then(pl.lit('Hittee'))
               .when((pl.col('event_type') == 'GOAL') & (~pl.col('event_goalie_id').is_null())).then(pl.lit('Goalie'))
               .when((pl.col('event_type') == 'GOAL') & (pl.col('event_goalie_id').is_null())).then(pl.lit('EmptyNet'))
               .when(pl.col('event_type').is_in(['SHOT', 'MISSED_SHOT', 'BLOCKED_SHOT'])).then(pl.lit('Goalie'))
               .when(pl.col('event_type') == 'PENALTY').then(pl.lit('DrewBy'))
               .when(pl.col('event_type') == 'GIVEAWAY').then(pl.lit('PlayerID'))
               .when(pl.col('event_type') == 'TAKEAWAY').then(pl.lit('PlayerID'))
               .otherwise(pl.lit(None))
             ).alias("event_player_2_type"),
             (pl.when((pl.col('event_type') == 'GOAL') & (~pl.col('assist_1_player_id').is_null())).then(pl.lit('Assist'))
               .when((pl.col('event_type') == 'PENALTY') & (~pl.col('servedby_player_id').is_null())).then(pl.lit('ServedBy'))
               .when((pl.col('event_type') == 'BLOCKED_SHOT') & (~pl.col('blocking_player_id').is_null())).then(pl.lit('Blocker'))
               .otherwise(pl.lit(None))
             ).alias("event_player_3_type"),
             (pl.when((pl.col('event_type') == 'GOAL') & (~pl.col('assist_2_player_id').is_null())).then(pl.lit('Assist'))
               .otherwise(pl.lit(None))
             ).alias("event_player_4_type")
        ])
        .drop(remove_ply_ids)
    )
    # Parse Situation Code For Home/Away Skaters/EmptyNet
    data = (
        data
        .sort('season', 'game_id', 'period', 'event_idx')
        .with_columns(
            pl.when(pl.col('situationCode').is_null()).then(pl.col("situationCode").fill_null(strategy="forward")).otherwise(pl.col('situationCode')).alias('situationCode')
        )
        .filter(~pl.col('situationCode').is_in(['0101', '1010']))
        .with_columns([
            pl.col("situationCode").str.slice(0, 1).cast(pl.Int32).alias("away_en"),
            pl.col("situationCode").str.slice(3, 1).cast(pl.Int32).alias("home_en"),
            pl.col("situationCode").str.slice(1, 1).cast(pl.Int32).alias("away_skaters"),
            pl.col("situationCode").str.slice(2, 1).cast(pl.Int32).alias("home_skaters")
        ])
        .with_columns([
            (pl.concat_str([pl.col('home_skaters'), pl.lit('v'), pl.col('away_skaters')])).alias('strength_state'),
            (pl.concat_str([pl.col('home_skaters'), pl.lit('v'), pl.col('away_skaters')])).alias('true_strength_state')
        ])
    )

    # Create x_fixed and y_fixed. These coordinates will be relative to the event team's attacking zone (i.e., x_abs is positive)
    data = (
        data
        .with_columns([
            pl.when((pl.col('event_zone') == 'O') & (pl.col('x').mean() > 0)).then(pl.lit(1)).otherwise(pl.lit(-1)).alias('flipped_coords')
        ])
        .with_columns([
            # Where homeTeamDefendingSide Exists
            (pl.when((~pl.col('homeTeamDefendingSide').is_null()) &
                     ( pl.col('homeTeamDefendingSide') == 'left') &
                     ( pl.col('event_team_type') == 'home'))
                     .then(pl.col('x'))
               .when((~pl.col('homeTeamDefendingSide').is_null()) &
                     ( pl.col('homeTeamDefendingSide') == 'right') &
                     ( pl.col('event_team_type') == 'home'))
                     .then(pl.col('x')*-1)
               .when((~pl.col('homeTeamDefendingSide').is_null()) &
                     ( pl.col('homeTeamDefendingSide') == 'left') &
                     ( pl.col('event_team_type') == 'away'))
                     .then(pl.col('x')*-1)
               .when((~pl.col('homeTeamDefendingSide').is_null()) &
                     ( pl.col('homeTeamDefendingSide') == 'right') &
                     ( pl.col('event_team_type') == 'away'))
                     .then(pl.col('x'))
              # Where homeTeamDefendingSide does not exist
              .when((pl.col('homeTeamDefendingSide').is_null()) &
                    (pl.col('event_zone') == 'O'))
                    .then(pl.col('x').abs())
              .when((pl.col('homeTeamDefendingSide').is_null()) &
                    (pl.col('event_zone') == 'D'))
                    .then((pl.col('x').abs())*-1)
              .when((pl.col('homeTeamDefendingSide').is_null()) &
                    (pl.col('event_zone') == 'N'))
                    .then((pl.col('x')) * (pl.col('flipped_coords').max().over(['season', 'game_id', 'period'])))
              .otherwise(pl.lit(None)).alias('x_abs')
            ),
            # Where homeTeamDefendingSide does exist
            (pl.when((~pl.col('homeTeamDefendingSide').is_null()) &
                     ( pl.col('homeTeamDefendingSide') == 'left') &
                     ( pl.col('event_team_type') == 'home'))
                     .then(pl.col('y'))
               .when((~pl.col('homeTeamDefendingSide').is_null()) &
                     ( pl.col('homeTeamDefendingSide') == 'right') &
                     ( pl.col('event_team_type') == 'home'))
                     .then(pl.col('y')*-1)
               .when((~pl.col('homeTeamDefendingSide').is_null()) &
                     ( pl.col('homeTeamDefendingSide') == 'left') &
                     ( pl.col('event_team_type') == 'away'))
                     .then(pl.col('y')*-1)
               .when((~pl.col('homeTeamDefendingSide').is_null()) &
                     ( pl.col('homeTeamDefendingSide') == 'right') &
                     ( pl.col('event_team_type') == 'away'))
                     .then(pl.col('y'))
              # Where homeTeamDefendingSide does not exist
              .when((pl.col('homeTeamDefendingSide').is_null()) &
                    (pl.col('event_zone') == 'O'))
                    .then(pl.col('y').abs())
              .when((pl.col('homeTeamDefendingSide').is_null()) &
                    (pl.col('event_zone') == 'D'))
                    .then((pl.col('y').abs())*-1)
              .when((pl.col('homeTeamDefendingSide').is_null()) &
                    (pl.col('event_zone') == 'N'))
                    .then((pl.col('y')) * (pl.col('flipped_coords').max().over(['season', 'game_id', 'period'])))
              .otherwise(pl.lit(None)).alias('y_abs')
            )
        ])
        .drop("flipped_coords")
    )

    # Create Event Distance Calculation
    data = data.with_columns(
        pl.when(pl.col('x_abs') >= 0).then(pl.Series.sqrt((89 - pl.Series.abs(data['x_abs']))**2 + data['y_abs']**2))
          .when(pl.col('x_abs') <  0).then(pl.Series.sqrt((pl.Series.abs(data['x_abs']) + 89)**2 + data['y_abs']**2))
          .alias('event_distance')
    )

    # Create Event Angle Calculation
    data = (
        data
        .with_columns(
        pl.when(data['x_abs'] >= 0)
          .then(pl.Series.arctan(data['y_abs'] / (89 - pl.Series.abs(data['x_abs'])))
                .apply(lambda x: abs(x * (180 / pi))))
          .when(data['x_abs'] < 0)
          .then(pl.Series.arctan(data['y_abs'] / (pl.Series.abs(data['x_abs']) + 89))
                .apply(lambda x: abs(x * (180 / pi))))
          .alias('event_angle')
        )
        .with_columns(
            pl.when(pl.col('x_abs') > 89).then((180 - pl.col('event_angle'))).otherwise(pl.col('event_angle')).alias('event_angle')
        )
    )

    return data

# 5) FUNCTION: Load and Append Shift Data From NHL API
def append_shift_data(data):
    """ This function will load shift data allowing the user to see which players are on the ice at a given time in each game"""
    # Load Game ID and Home/Away Ids
    i = data['game_id'][0]
    bad_shift_ids = []

    game_info_slim = (
        data
        .filter(pl.col('game_id') == i)
        .select('game_id', 'home_id', 'away_id', 'period', 'game_seconds', 'period_seconds', 'event_id', 'event_idx', 'event_type')
        .unique()
    )

    shift_link = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId="+str(i)
    shift_response = requests.get(shift_link).json()

    # Assuming "data" is the key containing nested data
    data_list = shift_response.get('data', [])
    keep_keys = ['id', 'endTime', 'firstName', 'gameId', 'lastName', 'period', 'playerId', 'startTime', 'teamAbbrev', 'teamId', 'duration']
    filtered_data = [{key: item[key] for key in keep_keys} for item in data_list]
    shift_raw = pl.DataFrame(filtered_data)
    try:
        shift_raw = (
            shift_raw
            .with_columns([
                pl.col('endTime').str.lengths().alias('endTime_min'),
                pl.col('startTime').str.lengths().alias('startTime_min'),
                pl.when(pl.col('startTime').str.lengths() == 4).then(pl.concat_str(pl.lit('0'), pl.col('startTime'))).otherwise(pl.col('startTime')).alias('startTime'),
                pl.when(pl.col('endTime').str.lengths() == 4).then(pl.concat_str(pl.lit('0'), pl.col('endTime'))).otherwise(pl.col('endTime')).alias('endTime')
            ])
            .filter((pl.col('startTime_min') != 0) & (pl.col('endTime_min') != 0))
            .drop('startTime_min', 'endTime_min')
            .with_columns([
                (pl.col('firstName') + ' ' + pl.col('lastName')).alias('player_name'),
                ((pl.col('startTime').str.slice(0, 2).cast(pl.Int32) * 60) + (pl.col('startTime').str.slice(3, 5).cast(pl.Int32))).alias('period_start_seconds'),
                ((pl.col('endTime').str.slice(0, 2).cast(pl.Int32) * 60) + (pl.col('endTime').str.slice(3, 5).cast(pl.Int32))).alias('period_end_seconds')
            ])
            .with_columns([
                (pl.col('period_start_seconds') + ((pl.col('period') - 1) * 1200)).alias('game_start_seconds'),
                (pl.col('period_end_seconds') + ((pl.col('period') - 1) * 1200)).alias('game_end_seconds'),
            ])
            .rename({
                    'gameId': 'game_id',
                    'id': 'shift_id',
                    'playerId': 'player_id',
                    'teamId': 'team_id',
                    'teamAbbrev': 'team_abbr'
                })
            .select([pl.col('game_id').cast(pl.Int32),
                     pl.col('team_id').cast(pl.Utf8),
                     pl.col('player_id').cast(pl.Utf8),
                     pl.col('player_name').str.to_uppercase().cast(pl.Utf8),
                     pl.col('team_abbr').cast(pl.Utf8),
                     pl.col('period').cast(pl.Int32),
                     pl.col('period_start_seconds').cast(pl.Int64),
                     pl.col('period_end_seconds').cast(pl.Int64),
                     pl.col('game_start_seconds').cast(pl.Int64),
                     pl.col('game_end_seconds').cast(pl.Int64)
                     ]) #'shift_id', 'typeCode', 'shift_number', 'eventNumber'
        )
        
        shift_raw = (
            # Join and Create team_type
            shift_raw
            .join(game_info_slim.select('game_id', 'home_id', 'away_id').unique(), on='game_id', how='left')
            .filter((pl.col('home_id') == pl.col('team_id')) | (pl.col('away_id') == pl.col('team_id')))
            .filter(pl.col('game_start_seconds') != pl.col('game_end_seconds') )
            .with_columns(pl.when(pl.col('home_id') == pl.col('team_id')).then(pl.lit('home'))
                            .when(pl.col('away_id') == pl.col('team_id')).then(pl.lit('away')).otherwise(pl.lit(None)).alias('team_type'))
            .drop('home_id', 'away_id')
            .unique()
        )
        # Combine Consecutive Shifts
        gb_cols = [col for col in shift_raw.columns if col not in ['period_start_seconds', 'game_start_seconds']]
        shift_raw = (
            shift_raw
            .sort('game_start_seconds')
            .with_columns([
                pl.col('period_start_seconds').max().over(gb_cols).alias('period_start_seconds'),
                pl.col('game_start_seconds').max().over(gb_cols).alias('game_start_seconds')#,
                #pl.col('eventNumber').max().over(gb_cols).alias('eventNumber')
            ])
            #.unique()
            # Separate Goalies
            .join(ROSTER_DF.with_columns([
                (pl.col('player_id').cast(pl.Utf8).alias('player_id')),
                (pl.col('pos_G').cast(pl.Int32).alias('pos_G'))
            ])
            .select('player_id', 'pos_G'), on='player_id', how='left')
            .unique()
        )
        # Concat Player IDs into lists for each group (i.e. event and seconds)
        result_df = (
            shift_raw
            .sort('game_start_seconds')
            .groupby(['game_id', 'period', 'period_start_seconds', 'period_end_seconds', 'team_type', 'pos_G'])
            .agg(
                pl.concat_list('player_id').flatten().unique().alias('player_id_list'),
                pl.concat_list('player_name').flatten().unique().alias('player_name_list')
                )
        )
        # Separate and Create Player On Columns
        game_data = (
             game_info_slim
            .filter(pl.col('game_id') == i)
            .sort('game_seconds', 'event_idx')
        )
        def apply_player_lists_pl(x, ty, pos, shift, output):
            return get_player_lists_pl((x['game_id'], x['period'], x['period_seconds'], ty, pos, shift, output))
        def get_player_lists_pl(x):
            # Outline Variables
            g_id, per, p_secs, ty, pos, shift, output = x
            # Adjust conditions as needed
            conditions = (
                (result_df['game_id'] == g_id) &
                (result_df['period'] == per) &
                (result_df['team_type'] == ty) &
                (result_df['pos_G'] == pos)
            )
            if shift == 'current':
                conditions &= (
                    (result_df['period_start_seconds'] < p_secs) &
                    (result_df['period_end_seconds'] > p_secs)
                )
            elif shift == 'on':
                conditions &= (result_df['period_start_seconds'] == p_secs)
            elif shift == 'off':
                conditions &= (result_df['period_end_seconds'] == p_secs)
            filtered_rows = result_df.filter(conditions)
            if output == 'id':
                result_list = set(filtered_rows['player_id_list'].explode().to_list())
            elif output == 'name':
                result_list = set(filtered_rows['player_name_list'].explode().to_list())
            return ','.join(str(item) for item in result_list)
    
        # List of columns to generate
        columns_to_generate = [
            ('home', 0, 'current', 'id'),
            ('home', 0, 'current', 'name'),
            ('home', 0, 'on', 'id'),
            ('home', 0, 'on', 'name'),
            ('home', 0, 'off', 'id'),
            ('home', 0, 'off', 'name'),
            ('away', 0, 'current', 'id'),
            ('away', 0, 'current', 'name'),
            ('away', 0, 'on', 'id'),
            ('away', 0, 'on', 'name'),
            ('away', 0, 'off', 'id'),
            ('away', 0, 'off', 'name'),
            ('home', 1, 'current', 'id'),
            ('home', 1, 'current', 'name'),
            ('home', 1, 'on', 'id'),
            ('home', 1, 'on', 'name'),
            ('home', 1, 'off', 'id'),
            ('home', 1, 'off', 'name'),
            ('away', 1, 'current', 'id'),
            ('away', 1, 'current', 'name'),
            ('away', 1, 'on', 'id'),
            ('away', 1, 'on', 'name'),
            ('away', 1, 'off', 'id'),
            ('away', 1, 'off', 'name')
        ]

        # Generate columns dynamically
        for prefix, pos, shift, output in columns_to_generate:
            if pos == 1:
                pos_lab = 'goalie'
            elif pos == 0:
                pos_lab = 'skater'
            col_name = f"{prefix}_{pos_lab}_{shift}_{output}"
            game_data = game_data.with_columns([
                pl.struct(["game_id", "period", "period_seconds"]).apply(lambda x: apply_player_lists_pl(x, prefix, pos, shift, output)).alias(col_name)
            ])
        game_start_end = ['GAME_START', 'PERIOD_START', 'GAME_END', 'PERIOD_END']
        game_data =(
             game_data
            .sort('game_id', 'period', 'period_seconds', 'event_idx')
            .filter(~pl.col('event_type').is_in(game_start_end))
            .with_columns([
                pl.col('event_idx').max().over(['game_id', 'period', 'period_seconds']).alias('max_event_idx')
            ])
            .with_columns([
                (pl.col('game_id').cast(pl.Utf8) + '-' + pl.col('period').cast(pl.Utf8) + '-' + pl.col('period_seconds').cast(pl.Utf8)).alias('event_seconds_id'),
                pl.when(pl.col('event_idx') == pl.col('max_event_idx')).then(pl.col('event_type')).otherwise(pl.lit(None)).alias('max_event_type')
            ])
            .with_columns([
                pl.col('event_seconds_id').count().over(['game_id', 'period', 'period_seconds']).alias('count_event_seconds_id')
            ])
        )

        teams = ['home', 'away']
        positions = ['skater', 'goalie']
        outputvals = ['id', 'name']
        for team in teams:
            for position in positions:
                for outputval in outputvals:
                    cur_cols = f"{team}_{position}_current_{outputval}"
                    off_cols = f"{team}_{position}_off_{outputval}"
                    on_cols = f"{team}_{position}_on_{outputval}"
                    label1 = f"{team}_{position}_on_{outputval}"
                    if position == 'goalie':
                        label2 = f"_goalie_{outputval}"
                    else:
                        label2 = f"on_{outputval}"
                    game_data = (
                        game_data
                        .with_columns([
                            pl.when((pl.col(cur_cols) != "") & (pl.col(on_cols)== "") & (pl.col(off_cols) == "")).then(pl.col(cur_cols))
                            .when((pl.col(cur_cols) == "") & (pl.col(on_cols)!= "") & (pl.col(off_cols) == "")).then(pl.col(on_cols))
                            .when((pl.col(cur_cols) == "") & (pl.col(on_cols)== "") & (pl.col(off_cols) != "")).then(pl.col(off_cols))
                            .when((pl.col(cur_cols) == "") & (pl.col(on_cols)!= "") & (pl.col(off_cols) != "") & (pl.col('event_idx') == pl.col('max_event_idx'))).then(pl.col(on_cols))
                            .when((pl.col(cur_cols) == "") & (pl.col(on_cols)!= "") & (pl.col(off_cols) != "") & (pl.col('event_idx') != pl.col('max_event_idx'))).then(pl.col(off_cols))
                            .when((pl.col(cur_cols) != "") & (pl.col(on_cols)!= "") & (pl.col('event_idx') == pl.col('max_event_idx'))).then(pl.concat_str([pl.col(cur_cols),pl.lit(","),pl.col(on_cols)]))
                            .when((pl.col(cur_cols) != "") & (pl.col(off_cols)!= "") & (pl.col('event_idx') != pl.col('max_event_idx'))).then(pl.concat_str([pl.col(cur_cols),pl.lit(","),pl.col(off_cols)]))
                            .when((pl.col(cur_cols) != "") & (pl.col(off_cols) != "") & (pl.col('event_idx') == pl.col('max_event_idx'))).then(pl.col(cur_cols))
                            .otherwise(pl.lit(None))
                            .alias(label1)
                        ])
                        .with_columns([pl.col(label1).str.split_exact(',', 7)])
                        .unnest(label1)
                        .rename({
                            "field_0" : f"{team}_1_{label2}",
                            "field_1" : f"{team}_2_{label2}",
                            "field_2" : f"{team}_3_{label2}",
                            "field_3" : f"{team}_4_{label2}",
                            "field_4" : f"{team}_5_{label2}",
                            "field_5" : f"{team}_6_{label2}",
                            "field_6" : f"{team}_7_{label2}",
                            "field_7" : f"{team}_8_{label2}"
                        })
                    )
        keep_cols = ['game_id', 'period', 'game_seconds', 'period_seconds', 'event_idx',
                     'home_1__goalie_id', 'home_1__goalie_name',
                     'home_1_on_id', 'home_2_on_id', 'home_3_on_id', 'home_4_on_id', 'home_5_on_id', 'home_6_on_id',
                     'home_1_on_name', 'home_2_on_name', 'home_3_on_name', 'home_4_on_name', 'home_5_on_name', 'home_6_on_name',
                     'away_1_on_id', 'away_2_on_id', 'away_3_on_id', 'away_4_on_id', 'away_5_on_id', 'away_6_on_id',
                     'away_1_on_name', 'away_2_on_name', 'away_3_on_name', 'away_4_on_name', 'away_5_on_name', 'away_6_on_name',
                     'away_1__goalie_id', 'away_1__goalie_name']
        game_data = (
            game_data
            .select(keep_cols)
            .rename({
                'away_1__goalie_id': 'away_goalie',
                'away_1__goalie_name': 'away_goalie_name',
                'home_1__goalie_id': 'home_goalie',
                'home_1__goalie_name': 'home_goalie_name'
            })
            .sort('game_id', 'period', 'period_seconds', 'event_idx')
        )

        # Combine DataFrames
        result_df = data.join(game_data, on = ['game_id', 'period', 'game_seconds', 'period_seconds', 'event_idx'], how = "left")

    except Exception as e:
        print('Bad ID:', i, 'Error:', e)
        bad_shift_ids.append(i)
        # Build Empty DF For Append
        result_df = (
            data
            .select('game_id', 'period', 'game_seconds', 'period_seconds', 'event_idx')
        )
        columns_with_null = [
        'home_goalie', 'home_goalie_name',
        'home_1_on_id', 'home_2_on_id', 'home_3_on_id', 'home_4_on_id', 'home_5_on_id', 'home_6_on_id',
        'home_1_on_name', 'home_2_on_name', 'home_3_on_name', 'home_4_on_name', 'home_5_on_name', 'home_6_on_name',
        'away_1_on_id', 'away_2_on_id', 'away_3_on_id', 'away_4_on_id', 'away_5_on_id', 'away_6_on_id',
        'away_1_on_name', 'away_2_on_name', 'away_3_on_name', 'away_4_on_name', 'away_5_on_name', 'away_6_on_name',
        'away_goalie', 'away_goalie_name'
            ]
        
        # Add null columns to the existing DataFrame
        for column in columns_with_null:
            result_df = result_df.with_columns(pl.lit(None).alias(column))
        
    
    return result_df

# 6) FUNCTION: Load, Clean, and Union Games Given Season - Saves as Local File (Parquet Format)
def load_games(load_path = 'Data/PBP/API_RAW_PBP_Data_2023.parquet', season_start = 2012, season_end = 2024 , existing=False):
    """This function will load all game play by play data using the functions above to clean the raw API Data from the NHL.
    
    If Existing is True, the function will only load games that are not in the most current PBP_RAW Parquet File"""
    # Get Dates
    max_date_file = open('last_load_date.json', 'r+')
    max_date= json.load(max_date_file)['max_date']
    yday = datetime.today() - timedelta(days=1)
    end_date = yday.strftime('%Y%m%d')
    last_load = datetime.strptime(max_date, "%Y-%m-%d").strftime('%Y%m%d')


    if (existing==True):
        # Print Information
        print("Now Loading Most Recent Play By Play Data From Existing File Path", load_path)
        start_time = time.time()

        # Get Dates For Load
        load_dates = pd.date_range(start=last_load, end=end_date, freq='D')

        f_g_id = []
        for i in load_dates:
            i_str = load_dates[0].strftime('%Y-%m-%d')
            sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
            response = requests.get(sched_link).json().get('gameWeek')[0].get('games')
            #print(response)
            for i in response:
                if i.get('gameType') in [2,3]:
                    f_g_id.append(i.get('id'))

        # Initialize Data Frame List To Store Loaded Data Frames
        df_list = []
        
        for i in f_g_id:
            pbp_link = 'https://api-web.nhle.com/v1/gamecenter/'+str(i)+'/play-by-play'

            pbp_response = requests.get(pbp_link).json()
            
            # PLAYS DATA
            try:
                game_data = pl.DataFrame({
                    'id': pbp_response.get('id'),
                    'season': pbp_response.get('season'),
                    'gameDate': pbp_response.get('gameDate'),
                    'gameType': pbp_response.get('gameType'),
                    'awayTeam.id': pbp_response.get('awayTeam', {}).get('id'),
                    'awayTeam.abbrev': pbp_response.get('awayTeam', {}).get('abbrev'),
                    'homeTeam.id': pbp_response.get('homeTeam', {}).get('id'),
                    'homeTeam.abbrev': pbp_response.get('homeTeam', {}).get('abbrev'),
                    'key': 1
                })
                plays_data_list = pbp_response.get('plays', [])
                plays_raw = (
                    pl.DataFrame(plays_data_list)
                    .rename({"typeCode":"eventTypeCode"})
                    .unnest('details')
                    .unnest('periodDescriptor')
                    .rename({"typeCode":"penaltyTypeCode",
                             "eventTypeCode":"typeCode"})
                    .with_columns(pl.lit(1).cast(pl.Int64).alias('key'))
                )
                result_df = game_data.join(plays_raw, on=pl.col("key"), how="inner").drop("key")

                # APPEND TO DF LIST FOR UNION
                result_df = append_shift_data(reconcile_api_data(align_and_cast_columns(data = result_df, sch = raw_schema)))
                df_list.append(result_df)

            except Exception as e:
                print(i, e)
                continue
        
        for df in df_list:
            data = data.vstack(df)
            
        data = data.sort('season', 'game_id', 'event_idx')

        
        max_date_new = data['game_date'].max()

        # Print Eval Statements
        end_time = time.time()
        elap_time = round(((end_time - start_time)/60),2)
        rows_loaded = data.filter(pl.col('game_id').is_in(f_g_id)).height
        start_date = data['game_date'].min()
        
        # COmbine With Existing
        data = pl.read_parquet('Data/PBP/API_RAW_PBP_Data_2023.parquet').vstack(data)
        
        print("Successfully Loaded",str(rows_loaded),"Rows from", str(n_games), "played between", str(start_date), "-", str(end_date), "in", str(elap_time), "Minutes")

        # Save
        g_ids = data['game_id'].unique().to_list()
        with open('game_ids.pkl', 'wb') as file:
            pickle.dump(g_ids, file)
        
        json.dump({"max_date": max_date_new}, open('last_load_date.json', 'w+'))

        return data
    
    elif(existing==False):
        ##### BEGIN GAME ID LOAD #####

        # Get Game IDs If List of Games Do Not Exist
        g_id_file_name  = 'game_ids.pkl'
        if os.path.exists(g_id_file_name):
            with open(g_id_file_name, "rb") as file:
                game_ids = pickle.load(file)
            game_ids.remove(2015020497)
        else:
            print("Collecting and Aggregating All Game ID's")
            id_start = time.time()
            st_date = str(season_start)+'0901'
            game_ids = []
            for i in pd.date_range(start=st_date, end=end_date, freq='D'):
                i_str = load_dates[0].strftime('%Y-%m-%d')
                sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
                response = requests.get(sched_link).json().get('gameWeek')[0].get('games')
                #print(response)
            for i in response:
                if i.get('gameType') in [2,3]:
                    game_ids.append(i.get('id'))

            game_ids.remove(2015020497)
            game_ids = list(set(game_ids))

            # Save
            with open('game_ids.pkl', 'wb') as file:
                pickle.dump(game_ids, file)

            id_end = time.time()
            id_elap = round((id_end - id_start)/60, 2)

            print("Successfully Loaded", str(len(game_ids)), "Game ID's From NHL Schedule in", str(id_elap), 'minutes')
        
        ##### END GAME ID LOAD #####
        

        start_time = time.time()
        n_games = len(game_ids)

        # Initialize Data Frame List To Store Loaded Data Frames
        season_range = list(range(season_start, season_end))
        bad_ids = []
        total_len = []
        shift_len = []
        print(f"Now Loading ALL Play By Play Data From NHL API ({season_start}-{season_end} Seasons) {len([game_id for game_id in game_ids if str(game_id).startswith(str(season_start))])} Games")
        for s in season_range:
            season_start_time = time.time()
            szn_ids = [game_id for game_id in game_ids if str(game_id).startswith(str(s))]
            szn_df_list = []
            szn_bad_ids = []
            
            for i in szn_ids:
                shift_start = time.time()
                # Create Try For Bad Links
                try:
                    # Get Raw Data
                    result_df = ping_nhl_api(i = i)

                    # All Functions
                    result_df = append_shift_data(reconcile_api_data(align_and_cast_columns(data = result_df, sch = raw_schema)))

                    # Append Single Game Data To List For Union
                    szn_df_list.append(result_df)

                except ValueError as e:
                    szn_bad_ids.append(i)
                    print(f"Error In Loading NHL API for GameID: {i} | {e}")
                    continue
                
                # Print Intermitent Update
                shift_end = time.time()
                shift_elap = shift_end - shift_start
                shift_len.append(shift_elap)
                average_shift_time = statistics.mean(shift_len)
                hour_pace = ((average_shift_time*n_games)/3600)

                if str(i)[-3:] == "500":
                    print(f"500 GAME UPDATE: Game {i} took {round(shift_elap,2)} | Each game is taking ~{round(average_shift_time,2)} Seconds | For {n_games} Games It will Take {round(hour_pace,2)} Hours")
                
            # Combine Season DataFrame Into Full DF List
            data = szn_df_list[0]
            for df in szn_df_list[1:]:
                data = data.vstack(df)
            data = data.sort('game_id', 'period', 'event_idx')

            # Save File After Combination
            save_season_path = f"Data/PBP/API_RAW_PBP_Data_{s}.parquet"
            data.write_parquet(
                save_season_path,
                use_pyarrow=True
            )

            # Print Season Metrics
            season_lab = f"{s}-{s+1}"
            season_end_time = time.time()
            season_elapsed_time = round((season_end_time - season_start_time)/60,2)
            bad_games = len(szn_bad_ids)
            games_loaded = len(szn_ids) - bad_games
            total_len.append(games_loaded)
            all_games_loaded = sum(total_len)
            games_remaining = n_games - all_games_loaded
            gpm = ((all_games_loaded)/(season_end_time - start_time)*60)
            szn_gpm = ((games_loaded)/(season_end_time - season_start_time)*60)
            est_time_remaining = games_remaining / gpm
            time_stamp = datetime.fromtimestamp(season_end_time).strftime('%Y-%m-%d %H:%M:%S')
            print(f"Successfully Loaded And Saved {games_loaded} Games From {season_lab} Season in {season_elapsed_time} Minutes ({round(szn_gpm, 2)} GPM) | {games_remaining} Games To Load -- Est. Load Time: {round(est_time_remaining/60,2)} Hours ({round(gpm, 2)} GPM) | Path: {save_season_path} | Completed at {time_stamp}")


            # Save Final Load Dates and Items
            if s == 2023:
                # Save And Print Last Load Date
                max_date_new = data['game_date'].max()
                json.dump({"max_date": max_date_new}, open('last_load_date.json', 'w+'))

                # Save Bad IDs for Filter In Future Use
                sv_bad_ids = list(chain(*bad_ids))
                with open('bad_ids.pkl', 'wb') as file:
                    pickle.dump(sv_bad_ids, file)
                print(len(sv_bad_ids), "Bad IDs - Failed To Load - No Data")
                print("Bad IDs:", sv_bad_ids)
    else:
        print("Wrong Inputs - Please Try Again")


## LOADING GAMES ##
        
# 1) Load All Games:
load_games(load_path='Data/PBP/API_RAW_PBP_Data.parquet', existing=False, season_start=2011, season_end = 2020)

# 2) Update Current PBP
def update_pbp_file(current_season = 2023):
    "This function will update the current season PBP with games occuring between the last load and yesterday's date"
    start_time = time.time()
    # Initialize Existing Data Frame + Stats
    df_list = []
    df_list.append(pl.read_parquet(f'Data/PBP/API_RAW_PBP_Data_{current_season}.parquet'))
    exist_games = len(df_list[0]['game_id'].unique())
    exist_rows = df_list[0].height

    # Initialize Load Dates
    last_load = (datetime.strptime(df_list[0]['game_date'].max(), "%Y-%m-%d") + timedelta(days = 1)).strftime('%Y%m%d')
    yday = datetime.today() - timedelta(days=1)
    end_date = yday.strftime('%Y%m%d')
    load_dates = pd.date_range(start=last_load, end=end_date, freq='D')

    print(f"Existing DataFrame has {exist_rows} from {exist_games} Games")

    # Load IDs
    f_g_id = []
    for i in load_dates:
        i_str = i.strftime('%Y-%m-%d')
        sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
        response = requests.get(sched_link).json().get('gameWeek')[0].get('games')
        for i in response:
            if i.get('gameType') in [2,3]:
                f_g_id.append(i.get('id'))

    print(f"Now Loading {len(f_g_id)} New Games From {last_load} to {end_date}")

    for i in f_g_id:
        try:
            # Get Raw Data
            result_df = ping_nhl_api(i = i)
            # Loand And Clean
            result_df = append_shift_data(reconcile_api_data(align_and_cast_columns(data = result_df, sch = raw_schema)))
            # APPEND TO DF LIST FOR UNION
            df_list.append(result_df)

        except ValueError as e:
            print(f"Error In Loading NHL API for GameID: {i} | {e}")
            continue

    data = df_list[0]
    for df in df_list[1:]:
        data = data.vstack(df)

    data = data.sort('season', 'game_id', 'period', 'event_idx').unique()
        
    # Save File After Combination
    save_season_path = f"Data/PBP/API_RAW_PBP_Data_{current_season}.parquet"
    data.write_parquet(
        save_season_path,
        use_pyarrow=True
    )

    # Print Eval Statements
    end_time = time.time()
    elap_time = round(((end_time - start_time)/60),2)
    rows_loaded = data.filter(pl.col('game_id').is_in(f_g_id)).height

    print("Successfully Loaded",str(rows_loaded),"Rows from", str(len(f_g_id)), "Games played between", str(last_load), "-", str(end_date), "in", str(elap_time), "Minutes")

    return data

def load_all_games(load_path = 'Data/PBP/API_RAW_PBP_Data_', season_start = 2012, season_end = 2024):
    ##### BEGIN GAME ID LOAD #####
    max_date_file = open('last_load_date.json', 'r+')
    max_date= json.load(max_date_file)['max_date']
    yday = datetime.today() - timedelta(days=1)
    end_date = yday.strftime('%Y%m%d')
    last_load = datetime.strptime(max_date, "%Y-%m-%d").strftime('%Y%m%d')
    # Get Game IDs If List of Games Do Not Exist
    g_id_file_name  = 'game_ids.pkl'
    if os.path.exists(g_id_file_name):
        with open(g_id_file_name, "rb") as file:
            game_ids = pickle.load(file)
        game_ids.remove(2015020497)
    else:
        print("Collecting and Aggregating All Game ID's")
        id_start = time.time()
        st_date = str(season_start)+'0901'
        game_ids = []
        for i in pd.date_range(start=st_date, end=end_date, freq='D'):
            i_str = load_dates[0].strftime('%Y-%m-%d')
            sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
            response = requests.get(sched_link).json().get('gameWeek')[0].get('games')
            #print(response)
        for i in response:
            if i.get('gameType') in [2,3]:
                game_ids.append(i.get('id'))

        game_ids.remove(2015020497)
        game_ids = list(set(game_ids))

        # Save
        with open('game_ids.pkl', 'wb') as file:
            pickle.dump(game_ids, file)

        id_end = time.time()
        id_elap = round((id_end - id_start)/60, 2)

        print("Successfully Loaded", str(len(game_ids)), "Game ID's From NHL Schedule in", str(id_elap), 'minutes')

    start_time = time.time()
    n_games = len(game_ids)

    # Initialize Data Frame List To Store Loaded Data Frames
    season_range = list(range(season_start, season_end))
    bad_ids = []
    total_len = []
    shift_len = []
    print(f"Now Loading ALL Play By Play Data From NHL API ({season_start}-{season_end} Seasons) {len([game_id for game_id in game_ids if str(game_id).startswith(str(season_start))])} Games")
    for s in season_range:
        season_start_time = time.time()
        szn_ids = [game_id for game_id in game_ids if str(game_id).startswith(str(s))]
        szn_df_list = []
        szn_bad_ids = []
        
        for i in szn_ids:
            shift_start = time.time()
            # Create Try For Bad Links
            try:
                # Get Raw Data
                result_df = ping_nhl_api(i = i)
                # All Functions
                result_df = append_shift_data(reconcile_api_data(align_and_cast_columns(data = result_df, sch = raw_schema)))

                # Append Single Game Data To List For Union
                szn_df_list.append(result_df)

            except ValueError as e:
                szn_bad_ids.append(i)
                print(f"Error In Loading NHL API for GameID: {i} | {e}")
                continue
                
            # Print Intermitent Update
            shift_end = time.time()
            shift_elap = shift_end - shift_start
            shift_len.append(shift_elap)
            average_shift_time = statistics.mean(shift_len)
            hour_pace = ((average_shift_time*n_games)/3600)

            if str(i)[-3:] == "500":
                print(f"500 GAME UPDATE: Game {i} took {round(shift_elap,2)} | Each game is taking ~{round(average_shift_time,2)} Seconds | For {n_games} Games It will Take {round(hour_pace,2)} Hours")

        # Combine Season DataFrame Into Full DF List
        data = szn_df_list[0]
        for df in szn_df_list[1:]:
            data = data.vstack(df)
        data = data.sort('game_id', 'period', 'event_idx')

        # Save File After Combination
        save_season_path = f"Data/PBP/API_RAW_PBP_Data_{s}.parquet"
        data.write_parquet(
            save_season_path,
            use_pyarrow=True
        )

        # Print Season Metrics
        season_lab = f"{s}-{s+1}"
        season_end_time = time.time()
        season_elapsed_time = round((season_end_time - season_start_time)/60,2)
        bad_games = len(szn_bad_ids)
        games_loaded = len(szn_ids) - bad_games
        total_len.append(games_loaded)
        all_games_loaded = sum(total_len)
        games_remaining = n_games - all_games_loaded
        gpm = ((all_games_loaded)/(season_end_time - start_time)*60)
        szn_gpm = ((games_loaded)/(season_end_time - season_start_time)*60)
        est_time_remaining = games_remaining / gpm
        time_stamp = datetime.fromtimestamp(season_end_time).strftime('%Y-%m-%d %H:%M:%S')
        print(f"Successfully Loaded And Saved {games_loaded} Games From {season_lab} Season in {season_elapsed_time} Minutes ({round(szn_gpm, 2)} GPM) | {games_remaining} Games To Load -- Est. Load Time: {round(est_time_remaining/60,2)} Hours ({round(gpm, 2)} GPM) | Path: {save_season_path} | Completed at {time_stamp}")

    # Save Final Load Dates and Items
        if s == 2023:
            # Save And Print Last Load Date
            max_date_new = data['game_date'].max()
            json.dump({"max_date": max_date_new}, open('last_load_date.json', 'w+'))

            # Save Bad IDs for Filter In Future Use
            sv_bad_ids = list(chain(*bad_ids))
            with open('bad_ids.pkl', 'wb') as file:
                pickle.dump(sv_bad_ids, file)
            print(len(sv_bad_ids), "Bad IDs - Failed To Load - No Data")
            print("Bad IDs:", sv_bad_ids)
#PBP_23 = update_pbp_file()
#PBP_23.sort('game_id', descending=True).head()