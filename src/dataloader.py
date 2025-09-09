import requests
import pandas as pd
import os
from datetime import datetime

# Obtiene API key desde variable de entorno
API_KEY = os.getenv("FOOTBALL_DATA_API_KEY")
BASE_URL = "https://api.football-data.org/v4/competitions/ELC/matches"

def fetch_championship_matches(season: int) -> pd.DataFrame:
    """
    Descarga todos los partidos de la Championship para una temporada dada.
    season: año de inicio de la temporada (ej: 2024 -> temporada 2024/25)
    """
    headers = {"X-Auth-Token": API_KEY}
    params = {"season": season}

    response = requests.get(BASE_URL, headers=headers, params=params)
    response.raise_for_status()  # lanza error si la petición falla

    data = response.json()
    matches = data.get("matches", [])

    df = pd.DataFrame([{
        "match_id": m["id"],
        "season": m["season"]["startDate"][:4],
        "utc_date": m["utcDate"],
        "status": m["status"],
        "matchday": m["matchday"],
        "home_team": m["homeTeam"]["name"],
        "away_team": m["awayTeam"]["name"],
        "home_score": m["score"]["fullTime"]["home"],
        "away_score": m["score"]["fullTime"]["away"]
    } for m in matches])

    return df


def save_matches(df: pd.DataFrame, season: int, folder="data/raw/"):
    """Guarda el DataFrame en CSV en la carpeta data/raw/"""
    os.makedirs(folder, exist_ok=True)
    filename = f"championship_matches_{season}.csv"
    filepath = os.path.join(folder, filename)
    df.to_csv(filepath, index=False)
    print(f"✅ Guardado en {filepath}")