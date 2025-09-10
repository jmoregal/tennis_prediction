import pandas as pd
import numpy as np

# --------------------
# 1. Carga y limpieza
# --------------------
def load_data(path: str) -> pd.DataFrame:
    """Carga dataset desde CSV."""
    return pd.read_csv(path)

def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia valores nulos básicos."""
    df = df.dropna(subset=['Winner', 'Surface', 'Rank_1', 'Rank_2'])
    df['Surface'] = df['Surface'].fillna("Unknown")
    return df

# --------------------
# 2. Categorización
# --------------------
def encode_Surface(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte superficie a categorías numéricas."""
    mapping = {"Hard": 0, "Clay": 1, "Grass": 2, "Carpet": 3, "Unknown": -1}
    df['Surface_encoded'] = df['Surface'].map(mapping)
    return df

# --------------------
# 3. Features sin leakage
# --------------------
def add_rank_diff(df: pd.DataFrame) -> pd.DataFrame:
    """Diferencia de ranking (A - B). Negativo = A mejor rankeado."""
    df['rank_diff'] = df['Rank_1'] - df['Rank_2']
    return df

def add_h2h(df: pd.DataFrame) -> pd.DataFrame:
    """Histórico H2H antes del partido."""
    df['h2h_A_wins'] = 0
    df['h2h_B_wins'] = 0
    
    h2h = {}
    for idx, row in df.iterrows():
        pair = tuple(sorted([row['Player_1'], row['Player_2']]))
        if pair not in h2h:
            h2h[pair] = {'A':0, 'B':0}
        
        # asignar valores previos
        df.at[idx, 'h2h_A_wins'] = h2h[pair]['A']
        df.at[idx, 'h2h_B_wins'] = h2h[pair]['B']
        
        # actualizar tras el resultado
        if row['Winner'] == row['Player_1']:
            h2h[pair]['A'] += 1
        else:
            h2h[pair]['B'] += 1
    return df

def add_Surface_winrate(df: pd.DataFrame) -> pd.DataFrame:
    """Winrate histórico de cada jugador por superficie antes del partido."""
    df['A_Surface_winrate'] = 0.5
    df['B_Surface_winrate'] = 0.5
    
    winrate = {}
    for idx, row in df.iterrows():
        playerA, playerB, surf, Winner = row['Player_1'], row['Player_2'], row['Surface'], row['Winner']
        
        for player in [playerA, playerB]:
            if (player, surf) not in winrate:
                winrate[(player, surf)] = {'wins':0, 'matches':0}
        
        # asignar antes de actualizar
        df.at[idx, 'A_Surface_winrate'] = winrate[(playerA, surf)]['wins'] / (winrate[(playerA, surf)]['matches']+1e-5)
        df.at[idx, 'B_Surface_winrate'] = winrate[(playerB, surf)]['wins'] / (winrate[(playerB, surf)]['matches']+1e-5)
        
        # actualizar tras el partido
        winrate[(playerA, surf)]['matches'] += 1
        winrate[(playerB, surf)]['matches'] += 1
        if Winner == playerA:
            winrate[(playerA, surf)]['wins'] += 1
        else:
            winrate[(playerB, surf)]['wins'] += 1
    
    return df

# --------------------
# 4. Pipeline final
# --------------------
def preprocess(path: str) -> pd.DataFrame:
    df = load_data(path)
    df = clean_nulls(df)
    df = encode_Surface(df)
    df = add_rank_diff(df)
    df = add_h2h(df)
    df = add_Surface_winrate(df)
    
    # Definir target: 1 si gana A, 0 si gana B
    df['target'] = (df['Winner'] == df['Player_1']).astype(int)
    return df
