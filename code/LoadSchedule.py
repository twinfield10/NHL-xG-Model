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
from itertools import product

# Save
import pickle
import json
import os
import pathlib

#### PREVIOUS SEASONS ###
#season_list = [2002,2003,2004,2006,2007,2008,2009,2010,
#               2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,
#               2021,2022,2023,2024]
#
## Loop And Load Seasons
#for i in season_list:
#    csv_url = "https://raw.githubusercontent.com/sportsdataverse/fastRhockey-data/main/nhl/schedules/csv/nhl_schedule_"+str(i)+".csv"
#    df = pl.read_csv(csv_url)
#    save_url = "Data/Schedule/NHL_Schedule_"+str(i)+".parquet"
#    df.write_parquet(save_url)
#    szn_start = i - 1
#    print(f"{szn_start}-{i} NHL Season Schedule Saved | Path: {save_url}")


## Function To Load Current Schedule
def load_schedule(start = '2023-10-10', end = '2023-12-25'): #datetime.today()
    """This function will take a start date and end date and load any NHL Game IDs From The NHL Schedule between those dates

        INPUTS:
        start and end are dates stored in Y%m%d% format ('2023-12-27')
    """

    # 1) Initialize Variables (Start Date, End Date, List of IDs, Existing DF)
    start_date = start
    end_date = end
    game_dfs = []
    #exist_df = pl.read_parquet('Data/Schedule/NHL_Schedule_2024.parquet')

    # 2) Loop Over Date Range To Get API Response For Schedule
    for i in pd.date_range(start=start_date, end=end_date, freq='D'):
        i_str = i.strftime('%Y-%m-%d')
        sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
        response = requests.get(sched_link).json().get('gameWeek')[0].get('games')
 
        for i, value in enumerate(response):
            if (value.get('gameType') in [2,3]) & (value.get('gameScheduleState') == 'OK'):
                data = pl.DataFrame({
                    'game_id': value.get('id'),
                    'season': value.get('season'),
                    'game_type_code': value.get('gameType'),
                    'venue_name': value.get('venue').get('default'),
                    'neutral_site': value.get('neutralSite'),
                    'start_time_utc': value.get('startTimeUTC'),
                    'east_offset': value.get('easternUTCOffset'),
                    'local_offset': value.get('venueUTCOffset'),
                    'local_timezone': value.get('venueTimezone'),
                    'game_state': value.get('gameState'),
                    'game_schedule_state': value.get('gameScheduleState'),
                    'away_team_id': value.get('awayTeam').get('id'),
                    'away_abbreviation': value.get('awayTeam').get('abbrev'),
                    'away_team_place': value.get('awayTeam').get('placeName').get('default'),
                    'away_logo': value.get('awayTeam').get('logo'),
                    'away_logo_dark': value.get('awayTeam').get('darkLogo'),
                    'away_score': value.get('awayTeam').get('score'),
                    'home_team_id': value.get('homeTeam').get('id'),
                    'home_abbreviation': value.get('homeTeam').get('abbrev'),
                    'home_team_place': value.get('homeTeam').get('placeName').get('default'),
                    'home_logo': value.get('homeTeam').get('logo'),
                    'home_logo_dark': value.get('homeTeam').get('darkLogo'),
                    'home_score': value.get('homeTeam').get('score'),
                    'period': value.get('periodDescriptor').get('number'),
                    'period_type': value.get('periodDescriptor').get('periodType'),
                    'last_period_type': value.get('gameOutcome').get('lastPeriodType'),
                    'gamecenter_link': value.get('gameCenterLink'),
                })
            if not data.is_empty():
                result_df = (
                    data
                    .with_columns([
                        pl.col('game_id').cast(pl.Int32),
                        pl.col('season').cast(pl.Int32),
                        pl.when(pl.col('game_type_code') == 2).then(pl.lit('R'))
                          .when(pl.col('game_type_code') == 3).then(pl.lit('P'))
                          .alias('season_type'),
                        pl.col("start_time_utc").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"),
                        pl.col("east_offset").str.extract_all(r"\d+"),
                        pl.col("local_offset").str.extract_all(r"\d+"),

                    ])
                    .with_columns([
                        (pl.col('start_time_utc') - pl.duration(hours = pl.col('east_offset').list.get(0))).alias('east_start_time'),
                        (pl.col('start_time_utc') - pl.duration(hours = pl.col('local_offset').list.get(0))).alias('local_start_time')
                    ])
                    .select([
                        'game_id', 'season', 'season_type', 'east_start_time', 'local_start_time',
                        'venue_name', 'neutral_site',
                        'game_state', 'game_schedule_state',
                        'away_team_id', 'away_abbreviation', 'away_team_place', 'away_score',
                        'home_team_id', 'home_abbreviation', 'home_team_place', 'home_score',
                        'period', 'last_period_type',
                        'gamecenter_link', 'home_logo', 'home_logo_dark', 'away_logo', 'away_logo_dark'

                    ])
                    .sort('game_id', 'season', 'east_start_time')
                )

                game_dfs.append(data)

    # Build + Manipulate Final DataFrame
    #game_dfs = [df for df in game_dfs if not df.is_empty()]
    result_df = game_dfs[0]
    for df in game_dfs[1:]:
        result_df = result_df.vstack(df)

    return result_df

load_schedule().write_parquet('Data/Schedule/NEW_20232024_Schedule.parquet')