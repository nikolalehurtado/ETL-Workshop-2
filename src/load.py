"""
LOAD — carga final del ETL al Data Warehouse dimensional (music_dw).

Estrategia de repetibilidad (seccion 7.3): TRUNCATE-AND-LOAD TRANSACCIONAL.

    BEGIN
      crear estructura si no existe (dw_schema.sql, idempotente)
      TRUNCATE de hechos y dimensiones
      COPY de dimensiones y luego hechos (las FK se validan en la carga)
      verificar conteo de filas por tabla == conteo del archivo preparado
      registrar la carga en etl_load_audit
    COMMIT   (si cualquier paso falla -> ROLLBACK: el DW queda como estaba)

Re-ejecutar con el mismo lote produce exactamente el mismo contenido:
las llaves subrogadas son deterministas y la tabla se reemplaza completa,
nunca se agregan filas duplicadas.

Esto NO es la preparacion de la fuente Grammy (sql/source_setup.sql): aqui se
escriben datos ya validados en el modelo dimensional.
"""

import io
import json
import logging

import pandas as pd

from src import config, db
from src.files import read_table

log = logging.getLogger(__name__)

# Orden de carga: dimensiones antes que hechos (integridad referencial)
LOAD_ORDER = ["dim_artist", "dim_track", "dim_genre", "dim_category", "dim_year",
              "fact_track_artist", "fact_grammy_award"]

# Columnas que son medidas decimales aunque un valor parezca entero
FLOAT_COLUMNS = {"danceability", "energy", "speechiness", "acousticness", "instrumentalness",
                 "liveness", "valence", "tempo", "loudness", "duration_min"}


def ensure_database():
    """Crea la base music_dw si no existe (CREATE DATABASE no admite transaccion)."""
    conn = db.psycopg_connection("postgres", autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config.DW_DB,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{config.DW_DB}"')
                log.info("Base de datos %s creada.", config.DW_DB)
    finally:
        conn.close()


def _copy_table(cur, table, csv_path):
    df = read_table(csv_path)
    # El CSV no conserva enteros con nulos (1986 -> 1986.0): se restauran como enteros
    for col in df.select_dtypes("float").columns:
        values = df[col].dropna()
        if len(values) and (values % 1 == 0).all() and col not in FLOAT_COLUMNS:
            df[col] = df[col].astype("Int64")
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    columns = ", ".join(f'"{c}"' for c in df.columns)
    cur.copy_expert(f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)", buffer)
    return len(df)


def load_dw(prepared_tables, run_id):
    ensure_database()
    schema_sql = (config.SQL_DIR / "dw_schema.sql").read_text(encoding="utf-8")

    conn = db.psycopg_connection(config.DW_DB)
    expected, loaded = {}, {}
    try:
        with conn:                      # una sola transaccion: commit o rollback completo
            with conn.cursor() as cur:
                cur.execute(schema_sql)
                cur.execute("SELECT " + ", ".join(
                    f"(SELECT COUNT(*) FROM {t})" for t in LOAD_ORDER))
                before = dict(zip(LOAD_ORDER, cur.fetchone()))

                cur.execute("TRUNCATE " + ", ".join(LOAD_ORDER) + " RESTART IDENTITY CASCADE")
                for table in LOAD_ORDER:
                    expected[table] = _copy_table(cur, table, prepared_tables[table])

                for table in LOAD_ORDER:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    loaded[table] = cur.fetchone()[0]
                mismatches = {t: (expected[t], loaded[t]) for t in LOAD_ORDER if expected[t] != loaded[t]}
                if mismatches:
                    raise RuntimeError(f"Conteos no conciliados tras la carga: {mismatches}")

                cur.execute(
                    "INSERT INTO etl_load_audit (run_id, strategy, row_counts) VALUES (%s, %s, %s)",
                    (str(run_id), "transactional truncate-and-load", json.dumps(loaded)),
                )
    finally:
        conn.close()

    for table in LOAD_ORDER:
        log.info("  %-20s antes=%8s  despues=%8s", table, before[table], loaded[table])
    log.info("Carga confirmada (COMMIT) en %s.", config.DW_DB)
    return {"before": before, "after": loaded}
