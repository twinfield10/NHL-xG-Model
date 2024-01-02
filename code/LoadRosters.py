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



# Load And Save Roster Game IDs
with open('game_ids.pkl', "rb") as file:
    all_g_ids = pickle.load(file)

st_yr = str(max(all_g_ids))[:4]


id_start = time.time()
st_date = st_yr +'1201'
yday = datetime.today() - timedelta(days=1)
end_date = yday.strftime('%Y%m%d')
game_ids_new = []
for i in pd.date_range(start=st_date, end=end_date, freq='D'):
    i_str = i.strftime('%Y-%m-%d')
    sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
    response = requests.get(sched_link).json()
    # Parse the JSON content of the response
    raw_data = pd.json_normalize(response)
    sched_data = pd.json_normalize(raw_data['gameWeek'][0])
    sched_data = pd.json_normalize(sched_data['games'][0])
    if len(sched_data) == 0:
        pass
    else:
        sched_data = sched_data[sched_data['gameType'].isin([2,3])]
        game_ids_new.append(sched_data['id'].tolist())
# Create Lists (Game ID and Dates Loaded):
game_ids = list(set(all_g_ids + list(chain(*game_ids_new))))
# Save
with open('game_ids.pkl', 'wb') as file:
    pickle.dump(game_ids, file)
id_end = time.time()
id_elap = round((id_end - id_start)/60, 2)
print("Successfully Loaded", str(len(game_ids)), "Game ID's From NHL Schedule in", str(id_elap), 'minutes')

def load_rosters(path = 'Data/NHL_Rosters_2014_2024.csv'):
    """Function To load Rosters. If Roster Data Exists, then the table will simply be updatad, rather than re-created every time"""

    # Load Constants:
    bad_link = ['https://api-web.nhle.com/v1/roster/ANA/20132014',
                     'https://api-web.nhle.com/v1/roster/ANA/20142015',
                     'https://api-web.nhle.com/v1/roster/ANA/20152016',
                     'https://api-web.nhle.com/v1/roster/ANA/20162017',
                     'https://api-web.nhle.com/v1/roster/ANA/20172018',
                     'https://api-web.nhle.com/v1/roster/ANA/20182019',
                     'https://api-web.nhle.com/v1/roster/ANA/20192020',
                     'https://api-web.nhle.com/v1/roster/ANA/20202021',
                     'https://api-web.nhle.com/v1/roster/ANA/20212022',
                     'https://api-web.nhle.com/v1/roster/ANA/20222023',
                     'https://api-web.nhle.com/v1/roster/ANA/20232024',
                     'https://api-web.nhle.com/v1/roster/ARI/20122013',
                     'https://api-web.nhle.com/v1/roster/ARI/20132014',
                     'https://api-web.nhle.com/v1/roster/PHX/20142015',
                     'https://api-web.nhle.com/v1/roster/PHX/20152016',
                     'https://api-web.nhle.com/v1/roster/PHX/20162017',
                     'https://api-web.nhle.com/v1/roster/PHX/20172018',
                     'https://api-web.nhle.com/v1/roster/PHX/20182019',
                     'https://api-web.nhle.com/v1/roster/PHX/20192020',
                     'https://api-web.nhle.com/v1/roster/PHX/20202021',
                     'https://api-web.nhle.com/v1/roster/PHX/20212022',
                     'https://api-web.nhle.com/v1/roster/PHX/20222023',
                     'https://api-web.nhle.com/v1/roster/PHX/20232024',
                     'https://api-web.nhle.com/v1/roster/SEA/20122013',
                     'https://api-web.nhle.com/v1/roster/SEA/20132014',
                     'https://api-web.nhle.com/v1/roster/SEA/20142015',
                     'https://api-web.nhle.com/v1/roster/SEA/20152016',
                     'https://api-web.nhle.com/v1/roster/SEA/20162017',
                     'https://api-web.nhle.com/v1/roster/SEA/20172018',
                     'https://api-web.nhle.com/v1/roster/SEA/20182019',
                     'https://api-web.nhle.com/v1/roster/SEA/20192020',
                     'https://api-web.nhle.com/v1/roster/SEA/20202021',
                     'https://api-web.nhle.com/v1/roster/VGK/20122013',
                     'https://api-web.nhle.com/v1/roster/VGK/20132014',
                     'https://api-web.nhle.com/v1/roster/VGK/20142015',
                     'https://api-web.nhle.com/v1/roster/VGK/20152016',
                     'https://api-web.nhle.com/v1/roster/VGK/20162017']

    # Define Historical Load:
    def historical_roster_load():
        """This function will aim to load all rosters from past seasons"""
        print("Now Loading Historical Rosters (2012/13 - 2023/24)")
        start_time = time.time()
        # Constant Team Abbr And Season
        tms_list = [['ANA', 'ARI', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 'COL', 'DAL',
                    'DET', 'EDM', 'FLA', 'LAK','MIN', 'MTL', 'NJD', 'NSH', 'NYI', 'NYR',
                    'OTT', 'PHI', 'PHX', 'PIT', 'SEA', 'SJS', 'STL', 'TBL', 'TOR', 'VAN', 'VGK', 'WPG', 'WSH']]

        szn_list = ['20122013','20132014', '20142015', '20152016', '20162017', '20172018', '20182019', '20192020', '20202021', '20212022', '20222023', '20232024']

        # Generate all combinations of teams and seasons
        combinations = list(product(tms_list[0], szn_list))

        # Create a DataFrame
        df = pd.DataFrame(combinations, columns=['teams', 'year'])
        df = df.explode('teams').drop_duplicates()
        df['link'] = "https://api-web.nhle.com/v1/roster/"+df['teams']+'/'+df['year']
        df.dropna(inplace=True)

        ## Begin Roster Loading
        rosters = []
        for link in df['link']:
            response = requests.get(link)
            szn_lab = link[-4:]
            if response.status_code == 200:
                # Parse the JSON content of the response
                data = response.json()
                # Normalize the nested structure into a flat DataFrame
                df_forwards = pd.json_normalize(data['forwards'])
                df_defensemen = pd.json_normalize(data['defensemen'])
                df_goalies = pd.json_normalize(data['goalies'])
                # Add a position category to each DataFrame
                df_forwards['position'] = 'Forward'
                df_defensemen['position'] = 'Defenseman'
                df_goalies['position'] = 'Goalie'
                # Concatenate the DataFrames for forwards, defensemen, and goalies
                final_df = pd.concat([df_forwards, df_defensemen, df_goalies], ignore_index=True)
                final_df['season'] = szn_lab
                final_df = final_df[['season', 'id', "firstName.default", "lastName.default", 'shootsCatches', 'position']]
                final_df.columns = ['season', 'player_id', 'first_name', 'last_name', 'hand', 'pos']
                # Append Roster DF To 
                rosters.append(final_df)
            elif(link in bad_link):
                pass
            else:
                # If the request was not successful, print the status code and any error message
                print(f"Error Bad Link: {link}")

        # Build Manipulate Rosters DF
        rosters_df = pd.concat(rosters, ignore_index=True)
        rosters_df = rosters_df.drop_duplicates()
        rosters_df['pos_F'] = (rosters_df['pos'] == 'Forward').astype('int32')
        rosters_df['pos_D'] = (rosters_df['pos'] == 'Defenseman').astype('int32')
        rosters_df['pos_G'] = (rosters_df['pos'] == 'Goalie').astype('int32')
        rosters_df['hand_R'] = (rosters_df['hand'] == 'R').astype('int32')
        rosters_df['hand_L'] = (rosters_df['hand'] == 'L').astype('int32')
        rosters_df = rosters_df[['player_id', 'first_name', 'last_name', 'pos_F', 'pos_D', 'pos_G', 'hand_R', 'hand_L']]
        rosters_df.drop_duplicates(inplace=True)

        # Print Efficiency Metrics
        end_time = time.time()
        elap_time = round((end_time - start_time)/60, 2)
        rows = rosters_df.shape[0]
        print(f"Historical Rosters Loading Complete in {elap_time} Minutes | {rows} Distinct Players Loaded From 2012/13 to 2023/24 Season")
        return rosters_df
    
    def current_roster_load(prev_rosters, current_yr = 2023):
        """This function will load all rosters possible from the current season and find players using individual game logs rather than the current roster"""
        print("Now Loading Current Season Games To Fill Missing Players")
        # Get Dates For Load   
        max_date = "2022-12-01"
        yday = datetime.today() - timedelta(days=1)
        end_date = yday.strftime('%Y%m%d')
        last_load = datetime.strptime(max_date, "%Y-%m-%d").strftime('%Y%m%d')
        load_dates = pd.date_range(start=last_load, end=end_date, freq='D')

        game_ids_new = []
        for i in load_dates:
            i_str = load_dates[0].strftime('%Y-%m-%d')
            sched_link = "https://api-web.nhle.com/v1/schedule/"+i_str
            response = requests.get(sched_link).json().get('gameWeek')[0].get('games')
            #print(response)
            for i in response:
                if i.get('gameType') in [2,3]:
                    game_ids_new.append(i.get('id'))

        roster_df_list = []
        for i in game_ids_new:
            pbp_link = 'https://api-web.nhle.com/v1/gamecenter/'+str(i)+'/play-by-play'

            pbp_response = requests.get(pbp_link).json()
            pbp_data = pd.json_normalize(pbp_response)
            pbp_data = pbp_data[pbp_data['gameType'].isin([2,3])]

            ## GAME DATA
            game_data = pbp_data[['id', 'season', 'gameDate', 'gameType', 'awayTeam.id', 'awayTeam.abbrev', 'homeTeam.id', 'homeTeam.abbrev']]

            ## ROSTER DATA
            roster_spots = pd.json_normalize(pbp_data['rosterSpots'])

            ## Create an empty DataFrame to store the normalized plays
            RS_normalized = pd.DataFrame()

            ## Iterate over each row in plays_1 and normalize the JSON data
            for _, row in roster_spots.iterrows():
                # Normalize the JSON data in the current row
                normalized_row = pd.json_normalize(row)

                # Concatenate the normalized row to the result DataFrame
                RS_normalized = pd.concat([RS_normalized, normalized_row], ignore_index=True)

            RS_df = pd.merge(game_data.assign(key=1), RS_normalized.assign(key=1), on='key').drop('key', axis=1)
            RS_df = RS_df[~RS_df['playerId'].isin(prev_rosters['player_id'])]
            RS_df = RS_df[['playerId', 'firstName.default', 'lastName.default', 'positionCode']]
            RS_df = RS_df.rename(columns = {
                 "playerId": "player_id",
                 "firstName.default": "first_name",
                 "lastName.default": "last_name"
            })
            RS_df['pos_F'] = (RS_df['positionCode'].isin(['C', 'R', 'L'])).astype('int32')
            RS_df['pos_D'] = (RS_df['positionCode'] == 'D').astype('int32')
            RS_df['pos_G'] = (RS_df['positionCode'] == 'G').astype('int32')

            # Append if Data
            if not RS_df.empty:
                roster_df_list.append(RS_df)
                missing_plyr_df = pd.concat(roster_df_list, ignore_index=True).drop_duplicates()

                # Pull and Join Handiness
                plyr_id_list =  missing_plyr_df['player_id'].drop_duplicates().values.tolist()
                plyr_df_list = []
                for i in plyr_id_list:
                    plyr_link = 'https://api-web.nhle.com/v1/player/'+str(i)+'/landing'

                    plyr_link_response = requests.get(plyr_link)
                    plyr_link_data = pd.json_normalize(plyr_link_response.json())
                    plyr_link_data = plyr_link_data[['playerId', 'shootsCatches']]
                    plyr_link_data['hand_R'] = (plyr_link_data['shootsCatches'] == 'R').astype('int32')
                    plyr_link_data['hand_L'] = (plyr_link_data['shootsCatches'] == 'L').astype('int32')
                    plyr_link_data = plyr_link_data.rename(columns = {"playerId": "player_id"})

                    plyr_df_list.append(plyr_link_data[['player_id', 'hand_R', 'hand_L']])

                hand_plyr_data = pd.concat(plyr_df_list, ignore_index=True).drop_duplicates()

                data = missing_plyr_df.merge(hand_plyr_data, on='player_id', how='left')
            else:
                data = pd.DataFrame({})

        return data

    # Load Roster Data (either from CSV or API)
    start_time = time.time()

    if os.path.exists(path):
        print("Roster Data Exists - Checking For New Players")
        roster_data = pd.read_csv(path)
    else:
        # Apply Historical Load Function
        roster_data = historical_roster_load()

    # Apply Current Load Function
    missing_plyr_df = current_roster_load(prev_rosters=roster_data)

    # Concat if Not Empty:
    if missing_plyr_df.empty:
        end_time = time.time()
        elap_time = round((end_time - start_time)/60, 2)

        # Eval Statements #
        print(f"Current Rosters Loading Complete in {elap_time} Minutes")
        print("All Players Up To Date")
        print(roster_data.head(5))

    elif not missing_plyr_df.empty:
        roster_data = pd.concat([roster_data, missing_plyr_df], ignore_index=True).drop(columns='positionCode')
        end_time = time.time()
        elap_time = round((end_time - start_time)/60, 2)

        # Eval Statements #
        print(f"Current Rosters Loading Complete in {elap_time} Minutes")
        print("Total Missing Players:", len(missing_plyr_df['player_id'].unique()))
        print("Missing Players:", [f"{row['first_name']} {row['last_name']}" for index, row in missing_plyr_df.iterrows()])
        print(roster_data.head(5))
    
    ## Save as a CSV
    roster_data.to_csv(path, index=False)

load_rosters()