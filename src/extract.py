"""
EXTRACT — adquisicion de las dos fuentes.

Regla de diseno (seccion 6.7 del enunciado): la extraccion NO limpia nada.
Copia los datos tal como llegan a un archivo raw del lote (run_id), para que la
validacion raw evalue la fuente real y para conservar evidencia investigable.
"""

import logging
from pathlib import Path

import pandas as pd

from src import config, db
from src.files import read_table

log = logging.getLogger(__name__)


def _batch_dir(run_id):
    """Carpeta de trabajo propia de cada ejecucion: data/work/<run_id>/."""
    safe = str(run_id).replace(":", "_").replace("+", "_")
    path = config.WORK_DIR / safe / "raw"
    path.mkdir(parents=True, exist_ok=True)
    return path


def extract_spotify(run_id, source_file=config.SPOTIFY_DEFAULT_FILE):
    """Lee el CSV de Spotify y guarda una copia raw del lote.

    source_file se busca primero en data/raw/ y luego en data/test/
    (los lotes de prueba para la falla controlada viven en data/test/).
    """
    candidates = [config.RAW_DIR / source_file, config.TEST_DIR / source_file]
    source_path = next((p for p in candidates if p.exists()), None)
    if source_path is None:
        raise FileNotFoundError(f"No se encontro el archivo fuente de Spotify: {source_file}")

    df = read_table(source_path)
    out = _batch_dir(run_id) / "spotify_raw.csv"
    df.to_csv(out, index=False)

    meta = {
        "source": str(source_path),
        "path": str(out),
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
    }
    log.info("Spotify extraido: %s", meta)
    return meta


def extract_grammys(run_id):
    """Extrae los datos de Grammy desde la BASE DE DATOS de origen (no desde el CSV)."""
    engine = db.sqlalchemy_engine(config.SOURCE_DB)
    query = f"""
        SELECT id AS source_id, year, title, published_at, updated_at,
               category, nominee, artist, workers, img, winner
        FROM {config.SOURCE_TABLE}
        ORDER BY id
    """
    try:
        df = pd.read_sql(query, engine)
    finally:
        engine.dispose()

    out = _batch_dir(run_id) / "grammy_raw.csv"
    df.to_csv(out, index=False)

    meta = {
        "source": f"postgresql://{config.DB_HOST}/{config.SOURCE_DB}.{config.SOURCE_TABLE}",
        "path": str(out),
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
    }
    log.info("Grammy extraido desde la base de datos de origen: %s", meta)
    return meta


def read_raw(path):
    return read_table(Path(path))
