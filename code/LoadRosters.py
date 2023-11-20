import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
from itertools import product
import requests
from datetime import datetime, timedelta

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
             'https://api-web.nhle.com/v1/roster/SEA/20132014',
             'https://api-web.nhle.com/v1/roster/SEA/20142015',
             'https://api-web.nhle.com/v1/roster/SEA/20152016',
             'https://api-web.nhle.com/v1/roster/SEA/20162017',
             'https://api-web.nhle.com/v1/roster/SEA/20172018',
             'https://api-web.nhle.com/v1/roster/SEA/20182019',
             'https://api-web.nhle.com/v1/roster/SEA/20192020',
             'https://api-web.nhle.com/v1/roster/SEA/20202021',
             'https://api-web.nhle.com/v1/roster/VGK/20132014',
             'https://api-web.nhle.com/v1/roster/VGK/20142015',
             'https://api-web.nhle.com/v1/roster/VGK/20152016',
             'https://api-web.nhle.com/v1/roster/VGK/20162017']


def generate_schedule_links():

    tms_list = [['ANA', 'ARI', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 'COL', 'DAL',
                 'DET', 'EDM', 'FLA', 'LAK','MIN', 'MTL', 'NJD', 'NSH', 'NYI', 'NYR',
                 'OTT', 'PHI', 'PHX', 'PIT', 'SEA', 'SJS', 'STL', 'TBL', 'TOR', 'VAN', 'VGK', 'WPG', 'WSH']]

    szn_list = ['20132014', '20142015', '20152016', '20162017', '20172018', '20182019', '20192020', '20202021', '20212022', '20222023', '20232024']

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
        elif(link not in bad_link):
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
    
    return rosters_df

roster_data = generate_schedule_links()
print(roster_data.head(10))

## Save as a CSV
roster_data.to_csv('NHL_Rosters_2014_2024.csv', index=False)