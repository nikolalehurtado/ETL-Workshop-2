-- =============================================================================
-- rerun_check.sql — Evidencia de re-ejecucion segura (seccion 7.3)
-- Ejecutar ANTES y DESPUES de re-ejecutar el DAG con el mismo lote:
--   docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d music_dw -f /sql/rerun_check.sql'
-- Si la carga es idempotente, ambas salidas deben ser identicas (salvo etl_load_audit,
-- que registra una carga mas).
-- =============================================================================
SELECT
    (SELECT COUNT(*) FROM dim_artist)                          AS dim_artist,
    (SELECT COUNT(*) FROM dim_track)                           AS dim_track,
    (SELECT COUNT(*) FROM fact_track_artist)                   AS fact_track_artist,
    (SELECT COUNT(*) FROM fact_grammy_award)                   AS fact_grammy_award,
    (SELECT SUM(popularity) FROM fact_track_artist)            AS suma_popularity,
    (SELECT ROUND(AVG(energy)::numeric, 6) FROM fact_track_artist) AS promedio_energy,
    (SELECT SUM(award_count) FROM fact_grammy_award)           AS total_premios,
    (SELECT md5(string_agg(artist_key || '|' || artist_name_norm, ',' ORDER BY artist_key))
       FROM dim_artist)                                        AS hash_llaves_artista;

SELECT audit_id, run_id, loaded_at, strategy,
       row_counts ->> 'fact_track_artist' AS fact_track_artist
FROM etl_load_audit
ORDER BY audit_id;
