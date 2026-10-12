"""
Prepara el lote de la PRUEBA B (falla Critical controlada, seccion 7.2).

NO modifica el archivo original: crea una copia en data/test/ con UNA violacion
deterministica de la regla Critical DQ04 (popularity debe estar entre 0 y 100).

Ejecucion (dentro del contenedor, donde estan pandas y las rutas del proyecto):
    docker compose exec airflow-scheduler python /opt/airflow/src/make_bad_data.py

Luego se dispara el DAG con el parametro spotify_file = spotify_bad_popularity.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import config               # noqa: E402
from src.files import read_table     # noqa: E402

BAD_FILE = "spotify_bad_popularity.csv"
INVALID_POPULARITY = 150             # fuera del contrato 0-100 de Spotify


def main():
    df = read_table(config.RAW_DIR / config.SPOTIFY_DEFAULT_FILE)
    row = df.index[0]
    original = df.loc[row, "popularity"]
    df.loc[row, "popularity"] = INVALID_POPULARITY

    config.TEST_DIR.mkdir(parents=True, exist_ok=True)
    out = config.TEST_DIR / BAD_FILE
    df.to_csv(out, index=False)
    print(f"Lote de prueba creado: {out}")
    print(f"Fila {row} (track_id={df.loc[row, 'track_id']}): popularity {original} -> {INVALID_POPULARITY}")
    print("Regla que debe bloquear: DQ04 (Critical) en validate_spotify_raw")


if __name__ == "__main__":
    main()
