"""
VALIDATION — compuertas de calidad con Great Expectations (GX Core 1.23.1).

Cada Expectation lleva en `meta` el ID de la regla de calidad que implementa
(DQxx, ver docs/quality_rules.md) y una severidad:
    critical -> bloquea el camino posterior
    warning  -> se registra y el pipeline continua segun la politica documentada
    info     -> contexto de monitoreo; nunca bloquea

Diseno GX (seccion 6.6 del enunciado):
    Data Source / Asset / Batch Definition -> el DataFrame del lote
    Expectation Suite                       -> un grupo coherente de reglas por capa/tabla
    Validation Definition                   -> asocia el batch con la suite
    Ejecucion controlada                    -> run_suite(): ejecuta, guarda el resultado
                                               en JSON y devuelve la severidad maxima

Las suites se definen en codigo (versionadas en Git) y en cada ejecucion se
exportan a gx/expectations/. Los resultados se conservan en
gx/validation_results/<run_id>/<suite>.json (evidencia legible por maquina).
"""

import json
import logging
import os
from datetime import datetime, timezone

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")  # sin telemetria

import great_expectations as gx                           # noqa: E402
from great_expectations.data_context.types.base import ProgressBarsConfig  # noqa: E402
from great_expectations.expectations.metadata_types import FailureSeverity  # noqa: E402

from src import config                                    # noqa: E402

log = logging.getLogger(__name__)
E = gx.expectations


# ---------------------------------------------------------------------------
# Definicion de suites: lista de (rule_id, severidad, Expectation)
# ---------------------------------------------------------------------------
def _spotify_raw():
    rules = [
        ("DQ01", "critical", E.ExpectTableColumnsToMatchSet(
            column_set=config.SPOTIFY_REQUIRED_COLUMNS, exact_match=False)),
        ("DQ02", "critical", E.ExpectColumnValuesToNotBeNull(column="track_id")),
        ("DQ03", "critical", E.ExpectColumnValuesToNotBeNull(column="artists", mostly=0.999)),
        ("DQ04", "critical", E.ExpectColumnValuesToBeBetween(
            column="popularity", min_value=0, max_value=100)),
        ("DQ06", "critical", E.ExpectColumnValuesToNotBeNull(column="track_genre")),
        ("DQ07", "warning", E.ExpectCompoundColumnsToBeUnique(
            column_list=["track_id", "track_genre"], mostly=0.99)),
        ("DQ08", "warning", E.ExpectColumnValuesToBeBetween(
            column="tempo", min_value=0, strict_min=True, mostly=0.995)),
        ("DQ22", "info", E.ExpectColumnMeanToBeBetween(
            column="popularity", min_value=25, max_value=45)),
    ]
    rules += [
        ("DQ05", "critical", E.ExpectColumnValuesToBeBetween(column=c, min_value=0, max_value=1))
        for c in config.AUDIO_FEATURES
    ]
    return rules


def _grammy_raw():
    current_year = datetime.now(timezone.utc).year
    return [
        ("DQ09", "critical", E.ExpectTableColumnsToMatchSet(
            column_set=config.GRAMMY_REQUIRED_COLUMNS, exact_match=False)),
        ("DQ10", "critical", E.ExpectColumnValuesToNotBeNull(column="year")),
        ("DQ10", "critical", E.ExpectColumnValuesToBeBetween(
            column="year", min_value=config.FIRST_GRAMMY_YEAR, max_value=current_year)),
        ("DQ11", "critical", E.ExpectColumnValuesToNotBeNull(column="category")),
        ("DQ12", "warning", E.ExpectColumnValuesToNotBeNull(column="artist", mostly=0.55)),
        ("DQ13", "warning", E.ExpectCompoundColumnsToBeUnique(
            column_list=["year", "category"], mostly=0.98)),
    ]


def _prepared_fact_track_artist():
    rules = [
        ("DQ14", "critical", E.ExpectTableColumnsToMatchSet(column_set=[
            "track_key", "artist_key", "genre_key", "artist_position", "popularity",
            "tempo", "loudness", *config.AUDIO_FEATURES], exact_match=False)),
        ("DQ15", "critical", E.ExpectCompoundColumnsToBeUnique(
            column_list=["track_key", "artist_key", "genre_key"])),
        ("DQ16", "critical", E.ExpectColumnValuesToNotBeNull(column="track_key")),
        ("DQ16", "critical", E.ExpectColumnValuesToNotBeNull(column="artist_key")),
        ("DQ16", "critical", E.ExpectColumnValuesToNotBeNull(column="genre_key")),
        ("DQ17", "critical", E.ExpectColumnValuesToBeBetween(
            column="popularity", min_value=0, max_value=100)),
        ("DQ17", "critical", E.ExpectColumnValuesToNotBeNull(column="popularity")),
        ("DQ18", "critical", E.ExpectColumnValuesToBeBetween(
            column="tempo", min_value=0, strict_min=True)),   # los nulos se ignoran
    ]
    rules += [
        ("DQ17", "critical", E.ExpectColumnValuesToBeBetween(column=c, min_value=0, max_value=1))
        for c in config.AUDIO_FEATURES
    ]
    return rules


def _prepared_fact_grammy_award():
    return [
        ("DQ16", "critical", E.ExpectColumnValuesToNotBeNull(column="artist_key")),
        ("DQ16", "critical", E.ExpectColumnValuesToNotBeNull(column="category_key")),
        ("DQ16", "critical", E.ExpectColumnValuesToNotBeNull(column="year_key")),
        ("DQ19", "critical", E.ExpectColumnValuesToBeBetween(
            column="records_in_year_category", min_value=1,
            max_value=config.MAX_WINNERS_PER_CATEGORY)),
        ("DQ21", "warning", E.ExpectColumnMeanToBeBetween(
            column="matched_spotify", min_value=0.40, max_value=0.70)),
    ]


def _prepared_dim_artist():
    return [
        ("DQ20", "critical", E.ExpectColumnValuesToNotBeNull(column="artist_name_norm")),
        ("DQ20", "critical", E.ExpectColumnValuesToBeUnique(column="artist_name_norm")),
        ("DQ20", "critical", E.ExpectColumnValuesToBeUnique(column="artist_key")),
        ("DQ20", "critical", E.ExpectColumnValuesToBeInSet(
            column="is_grammy_winner", value_set=[True, False])),
    ]


SUITES = {
    "spotify_raw_suite": _spotify_raw,
    "grammy_raw_suite": _grammy_raw,
    "prepared_fact_track_artist_suite": _prepared_fact_track_artist,
    "prepared_fact_grammy_award_suite": _prepared_fact_grammy_award,
    "prepared_dim_artist_suite": _prepared_dim_artist,
}


# ---------------------------------------------------------------------------
# Ejecucion controlada
# ---------------------------------------------------------------------------
def _safe(run_id):
    return str(run_id).replace(":", "_").replace("+", "_")


def _summarize(result):
    """Resumen por regla: lo que se imprime en el log de Airflow."""
    rows = []
    for r in result.results:
        cfg = r.expectation_config
        res = r.result or {}
        rows.append({
            "rule_id": (cfg.meta or {}).get("rule_id"),
            "severity": str(cfg.severity.value if hasattr(cfg.severity, "value") else cfg.severity),
            "expectation": cfg.type,
            "column": cfg.kwargs.get("column") or cfg.kwargs.get("column_list"),
            "success": bool(r.success),
            "unexpected_count": res.get("unexpected_count"),
            "unexpected_percent": (round(res["unexpected_percent"], 4)
                                   if res.get("unexpected_percent") is not None else None),
            "observed_value": (res.get("observed_value")
                               if not isinstance(res.get("observed_value"), list) else None),
            "exception": bool((r.exception_info or {}).get("raised_exception")),
        })
    return sorted(rows, key=lambda x: (x["rule_id"] or "", x["expectation"]))


def run_suite(df, suite_name, run_id):
    """Valida un DataFrame contra una suite y devuelve (resumen, severidad_maxima_fallida).

    severidad_maxima_fallida: FailureSeverity.CRITICAL, FailureSeverity.WARNING o None.
    """
    context = gx.get_context(mode="ephemeral")
    # Sin barras de progreso: los logs de Airflow quedan legibles
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)
    source = context.data_sources.add_pandas(name=f"{suite_name}_source")
    asset = source.add_dataframe_asset(name=f"{suite_name}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(f"{suite_name}_batch")

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for rule_id, severity, expectation in SUITES[suite_name]():
        expectation.severity = severity
        expectation.meta = {"rule_id": rule_id}
        suite.add_expectation(expectation)

    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{suite_name}_validation", data=batch_definition, suite=suite)
    )
    result = validation_definition.run(
        batch_parameters={"dataframe": df},
        result_format={"result_format": "SUMMARY"},
    )
    max_failure = result.get_max_severity_failure()

    # Evidencia persistente: suite exportada y resultado completo en JSON
    (config.GX_DIR / "expectations").mkdir(parents=True, exist_ok=True)
    (config.GX_DIR / "expectations" / f"{suite_name}.json").write_text(
        json.dumps(suite.to_json_dict(), indent=2, default=str), encoding="utf-8")

    summary = _summarize(result)
    out_dir = config.GX_DIR / "validation_results" / _safe(run_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": str(run_id),
        "suite": suite_name,
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "rows_validated": int(len(df)),
        "success": bool(result.success),
        "max_failure_severity": max_failure.value if max_failure else None,
        "statistics": result.statistics,
        "rules": summary,
        "gx_result": result.to_json_dict(),
    }
    out_file = out_dir / f"{suite_name}.json"
    out_file.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    log.info("GX suite=%s filas=%s success=%s severidad_maxima=%s",
             suite_name, len(df), result.success, max_failure)
    for row in summary:
        estado = "OK  " if row["success"] else "FAIL"
        log.info("  [%s] %s %-8s %-40s col=%s unexpected=%s%% observed=%s",
                 estado, row["rule_id"], row["severity"], row["expectation"], row["column"],
                 row["unexpected_percent"], row["observed_value"])
    log.info("Resultado guardado en %s", out_file)
    return summary, max_failure, str(out_file)


def failed_rules(summary, severity):
    return sorted({r["rule_id"] for r in summary if not r["success"] and r["severity"] == severity})


def enforce_policy(summary, max_failure, gate):
    """Politica de compuerta: Critical bloquea; Warning se registra y continua."""
    if max_failure == FailureSeverity.CRITICAL:
        raise ValueError(
            f"[{gate}] Validacion con severidad CRITICAL. Reglas violadas: "
            f"{failed_rules(summary, 'critical')}. El procesamiento posterior queda bloqueado."
        )
    if max_failure == FailureSeverity.WARNING:
        log.warning("[%s] Reglas WARNING violadas: %s. La politica documentada permite continuar.",
                    gate, failed_rules(summary, "warning"))
    if failed_rules(summary, "info"):
        log.info("[%s] Reglas INFO fuera de rango (solo monitoreo): %s",
                 gate, failed_rules(summary, "info"))
    log.info("[%s] Compuerta superada segun la politica.", gate)
    return {
        "gate": gate,
        "max_failure_severity": max_failure.value if max_failure else None,
        "warnings": failed_rules(summary, "warning"),
        "info": failed_rules(summary, "info"),
    }
