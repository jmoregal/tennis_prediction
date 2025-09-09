import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def load_data(path):
    return pd.read_csv(path) 

def preprocess_data(df):

    df = df.dropna()

    # Convert column Date to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Encode surface types as integers
    surface_mapping = {'Hard': 0, 'Clay': 1, 'Grass': 2, 'Carpet': 3}
    df['Surface'] = df['Surface'].map(surface_mapping)
 
    # Winner Rank, Loser Rank and Rank Diff
    df['Winner_Rank'] = np.where(df['Winner'] == df['Player_1'], df['Rank_1'], df['Rank_2'])
    df['Loser_Rank'] = np.where(df['Winner'] == df['Player_1'], df['Rank_2'], df['Rank_1'])
    df['Rank_Diff'] = df['Loser_Rank'] - df['Winner_Rank']
    
    return df

def h2h(df):

    df['h2h_Wins_Player_1'] = 0
    df['h2h_Wins_Player_2'] = 0

    h2h_dict = {}

    for index, row in df.iterrows():
        pair = tuple(sorted([row['Player_1'], row['Player_2']]))
        if pair not in h2h_dict:
            h2h_dict[pair] = {'A': 0, 'B': 0}
        
        df.at[index, 'h2h_Wins_Player_1'] = h2h_dict[pair]['A']
        df.at[index, 'h2h_Wins_Player_2'] = h2h_dict[pair]['B']

        if row['Winner'] == row['Player_1']:
            h2h_dict[pair]['A'] += 1
        else:
            h2h_dict[pair]['B'] += 1
    return df

def surface_ratio(df):
    "Compute win ratio on each surface for both players"
    df['surface_ratio_Player_1'] = 0.5
    df['surface_ratio_Player_2'] = 0.5

    surface_stats = {}
    for idx, row in df.iterrows():
        playerA, playerB, surface, winner = row['Player_1'], row['Player_2'], row['Surface'], row['Winner']

        for player in [playerA, playerB]:
            if (player, surface) not in surface_stats:
                surface_stats[(player, surface)] = {'wins': 0, 'total': 0}

        df.at[idx, 'surface_ratio_Player_1'] = surface_stats[(playerA, surface)]['wins'] / (surface_stats[(playerA, surface)]['total']+1e-5)
        df.at[idx, 'surface_ratio_Player_2'] = surface_stats[(playerB, surface)]['wins'] / (surface_stats[(playerB, surface)]['total']+1e-5)

        surface_stats[(playerA, surface)]['total'] += 1
        surface_stats[(playerB, surface)]['total'] += 1

        if winner == playerA:
            surface_stats[(playerA, surface)]['wins'] += 1
        else:
            surface_stats[(playerB, surface)]['wins'] += 1

    return df

def final_preprocessing(str_path):
    df = load_data(str_path)
    df = preprocess_data(df)
    df = h2h(df)
    df = surface_ratio(df)
    return df



