"""
Configuracion central del pipeline (rutas, conexiones y contratos de datos).

Todas las rutas apuntan a carpetas montadas en los contenedores de Airflow
(ver docker-compose.yaml). Fuera de Docker se puede cambiar la raiz con la
variable de entorno PROJECT_ROOT (util para pruebas locales).
"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/opt/airflow"))

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"            # archivos fuente originales (solo lectura)
TEST_DIR = DATA_DIR / "test"          # lotes de prueba (falla controlada)
WORK_DIR = DATA_DIR / "work"          # archivos intermedios por ejecucion (run_id)
SQL_DIR = PROJECT_ROOT / "sql"
GX_DIR = PROJECT_ROOT / "gx"          # suites y resultados de Great Expectations

SPOTIFY_DEFAULT_FILE = "spotify_dataset.csv"

# ---------------------------------------------------------------------------
# Conexiones (valores tomados de .env; nunca se escriben en el codigo)
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

SOURCE_DB = os.getenv("GRAMMY_SOURCE_DB", "grammy_source")   # fuente operativa Grammy
SOURCE_TABLE = "grammy_awards"
DW_DB = os.getenv("DW_DB", "music_dw")                        # Data Warehouse

# ---------------------------------------------------------------------------
# Contratos de las fuentes: columnas requeridas por R1-R4
# ---------------------------------------------------------------------------
AUDIO_FEATURES = [
    "danceability", "energy", "speechiness", "acousticness",
    "instrumentalness", "liveness", "valence",
]

SPOTIFY_REQUIRED_COLUMNS = [
    "track_id", "artists", "album_name", "track_name", "popularity",
    "duration_ms", "explicit", "tempo", "loudness", "time_signature",
    "track_genre", *AUDIO_FEATURES,
]

GRAMMY_REQUIRED_COLUMNS = ["year", "title", "category", "nominee", "artist", "winner"]

# Primera ceremonia de los Premios Grammy (contrato de la fuente para DQ10)
FIRST_GRAMMY_YEAR = 1958

# Maximo de registros por (year, category) en un lote de GANADORES.
# Un premio tiene un ganador; los empates reales llegan a 2 (evidencia 1958-2018).
MAX_WINNERS_PER_CATEGORY = 2
