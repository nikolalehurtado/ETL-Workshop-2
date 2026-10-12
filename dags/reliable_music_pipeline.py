"""
Reliable Music Pipeline — ETL Workshop-2 (UAO)

Airflow 3.1.8 (TaskFlow API, airflow.sdk) · Great Expectations Core 1.23.1

Integra Spotify (CSV) y Premios Grammy (base de datos PostgreSQL de origen) en
un Data Warehouse dimensional (music_dw) que responde R1-R4 (docs/requirements.md).

    extract_spotify -> validate_spotify_raw ----\
                                                 >-> transform_and_integrate -> validate_prepared -> load_dw -> publish_kpi_summary
    extract_grammys -> validate_grammys_raw ----/

Principios aplicados:
- El DAG solo ORQUESTA; la logica vive en src/ (extract, validation, transform, load).
- Las tareas intercambian RUTAS y metadatos compactos, nunca DataFrames completos.
- Critical en una compuerta => la tarea falla y lo posterior no se ejecuta.
- Reintentos SOLO en tareas con fallas transitorias posibles (base de datos).
- Cada ejecucion trabaja en data/work/<run_id>/ y deja su evidencia en gx/.
"""

import json
import logging
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

log = logging.getLogger(__name__)

# Politica de reintentos (docs/failure_policy en el README, seccion 11)
NO_RETRY = {"retries": 0}
DB_RETRY = {                       # fallas transitorias de red / base de datos
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=3),
}


def _run_id():
    return get_current_context()["run_id"]


def _db_call(func, *args):
    """Ejecuta una operacion de base de datos aplicando la politica de reintentos selectiva.

    Falla transitoria (conexion/red)  -> se relanza: Airflow la reintenta (DB_RETRY).
    Falla determinista (datos/esquema) -> AirflowFailException: falla de inmediato,
                                          sin gastar reintentos que repetirian el error.
    """
    from airflow.exceptions import AirflowFailException
    from src.db import is_transient
    try:
        return func(*args)
    except Exception as exc:
        if is_transient(exc):
            log.warning("Falla TRANSITORIA de base de datos (se reintentara): %s", exc)
            raise
        raise AirflowFailException(f"Falla DETERMINISTA, no se reintenta: {exc}") from exc


@dag(
    dag_id="reliable_music_pipeline",
    description="Spotify + Grammy -> GX raw -> transform/integrate -> GX prepared -> music_dw",
    schedule=None,                         # ejecuciones manuales para inspeccionar cada DAG Run
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,                     # dos cargas simultaneas al DW no son seguras
    default_args={"execution_timeout": timedelta(minutes=20)},
    params={
        "spotify_file": Param(
            "spotify_dataset.csv",
            type="string",
            description="Archivo de Spotify en data/raw/ o data/test/. "
                        "Para la Prueba B usar spotify_bad_popularity.csv",
        ),
    },
    tags=["etl", "workshop-2", "gx", "spotify", "grammy"],
)
def reliable_music_pipeline():

    # ------------------------------------------------------------------ EXTRACT
    @task(**NO_RETRY)
    def extract_spotify():
        """Archivo local: si falta o esta corrupto, reintentar no lo arregla."""
        from src.extract import extract_spotify as run
        context = get_current_context()
        return run(_run_id(), context["params"]["spotify_file"])

    @task(**DB_RETRY)
    def extract_grammys():
        """Lee la fuente Grammy desde PostgreSQL (una caida temporal si justifica reintento)."""
        from src.extract import extract_grammys as run
        return _db_call(run, _run_id())

    # --------------------------------------------------------- RAW VALIDATION
    @task(**NO_RETRY)
    def validate_spotify_raw(meta):
        """¿Pueden los datos de Spotify entrar a la transformacion?"""
        from src.extract import read_raw
        from src.validation import enforce_policy, run_suite
        df = read_raw(meta["path"])
        summary, max_failure, result_file = run_suite(df, "spotify_raw_suite", _run_id())
        decision = enforce_policy(summary, max_failure, gate="validate_spotify_raw")
        return {**decision, "result_file": result_file, "rows": len(df)}

    @task(**NO_RETRY)
    def validate_grammys_raw(meta):
        """¿Pueden los datos de Grammy entrar a la transformacion?"""
        from src.extract import read_raw
        from src.validation import enforce_policy, run_suite
        df = read_raw(meta["path"])
        summary, max_failure, result_file = run_suite(df, "grammy_raw_suite", _run_id())
        decision = enforce_policy(summary, max_failure, gate="validate_grammys_raw")
        return {**decision, "result_file": result_file, "rows": len(df)}

    # --------------------------------------------------------------- TRANSFORM
    @task(**NO_RETRY)
    def transform_and_integrate(spotify_meta, grammy_meta):
        """Limpieza, armonizacion, integracion por artista y tablas del modelo estrella."""
        from src.transform import transform_and_integrate as run
        return run(spotify_meta["path"], grammy_meta["path"])

    # ---------------------------------------------------- PREPARED VALIDATION
    @task(**NO_RETRY)
    def validate_prepared(prepared):
        """¿La transformacion produjo datos aptos para cargar? Suite distinta por tabla."""
        from great_expectations.expectations.metadata_types import FailureSeverity
        from src.files import read_table
        from src.validation import enforce_policy, failed_rules, run_suite

        checks = {
            "fact_track_artist": "prepared_fact_track_artist_suite",
            "fact_grammy_award": "prepared_fact_grammy_award_suite",
            "dim_artist": "prepared_dim_artist_suite",
        }
        decisions, critical = {}, {}
        for table, suite in checks.items():
            df = read_table(prepared["tables"][table])
            summary, max_failure, result_file = run_suite(df, suite, _run_id())
            if max_failure == FailureSeverity.CRITICAL:
                critical[suite] = failed_rules(summary, "critical")
            else:
                decisions[suite] = {**enforce_policy(summary, max_failure, gate=suite),
                                    "result_file": result_file}
        if critical:
            raise ValueError(f"Validacion preparada CRITICAL: {critical}. La carga al DW queda bloqueada.")
        return decisions

    # -------------------------------------------------------------------- LOAD
    @task(**DB_RETRY)
    def load_dw(prepared):
        """Truncate-and-load transaccional: un reintento no duplica (rollback + reemplazo)."""
        from src.load import load_dw as run
        return _db_call(run, prepared["tables"], _run_id())

    # --------------------------------------------------------------- ANALYTICS
    @task(**DB_RETRY)
    def publish_kpi_summary(load_result):
        """Consulta las vistas KPI del DW y guarda un resumen del lote como evidencia."""
        from pathlib import Path
        import pandas as pd
        from src import config, db

        engine = db.sqlalchemy_engine(config.DW_DB)
        views = ["kpi_r1_popularity_by_winner", "kpi_r2_genre_winners",
                 "kpi_r3_audio_profile", "kpi_r4_popularity_by_decade"]
        summary = {}
        try:
            for view in views:
                df = pd.read_sql(f"SELECT * FROM {view}", engine)
                summary[view] = df.head(15).to_dict(orient="records")
                log.info("%s\n%s", view, df.head(10).to_string(index=False))
        finally:
            engine.dispose()

        safe = str(_run_id()).replace(":", "_").replace("+", "_")
        out = Path(config.WORK_DIR) / safe / "kpi_summary.json"
        out.write_text(json.dumps({"load": load_result, "kpis": summary}, indent=2, default=str),
                       encoding="utf-8")
        return str(out)

    # ----------------------------------------------------------- DEPENDENCIAS
    spotify_raw = extract_spotify()
    grammy_raw = extract_grammys()

    spotify_gate = validate_spotify_raw(spotify_raw)
    grammy_gate = validate_grammys_raw(grammy_raw)

    prepared = transform_and_integrate(spotify_raw, grammy_raw)
    # Compuerta raw: la transformacion espera a que AMBAS ramas superen la politica
    [spotify_gate, grammy_gate] >> prepared

    prepared_gate = validate_prepared(prepared)
    loaded = load_dw(prepared)
    # Compuerta preparada: no se carga si la validacion preparada no tuvo exito
    prepared_gate >> loaded

    publish_kpi_summary(loaded)


reliable_music_pipeline()
