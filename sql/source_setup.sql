-- =============================================================================
-- source_setup.sql
-- PREPARACION DE LA FUENTE GRAMMY (no es la Carga final del ETL)
--
-- Crea la base de datos operativa "grammy_source" y carga el CSV original
-- en la tabla "grammy_awards" SIN limpiar ni transformar los datos.
-- Los problemas de calidad se conservan a proposito para que el pipeline
-- los detecte en la validacion raw (seccion 6.7 del enunciado).
--
-- Es re-ejecutable: borra y recarga la tabla, por lo que nunca duplica filas.
-- Ejecucion:
--   docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d postgres -f /sql/source_setup.sql'
-- =============================================================================

-- Detener el script ante el primer error (no seguir con datos incompletos)
\set ON_ERROR_STOP on

-- 1. Crear la base de datos solo si no existe
--    (PostgreSQL no soporta CREATE DATABASE IF NOT EXISTS; \gexec ejecuta
--     la sentencia generada solo cuando la consulta devuelve una fila)
SELECT 'CREATE DATABASE grammy_source'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'grammy_source')
\gexec

-- 2. Conectarse a la base de datos fuente
\connect grammy_source

-- 3. Definicion de la tabla fuente (refleja las 10 columnas del CSV)
DROP TABLE IF EXISTS grammy_awards;

CREATE TABLE grammy_awards (
    id            BIGSERIAL PRIMARY KEY,   -- llave tecnica: el CSV no trae identificador
    year          INTEGER,
    title         TEXT,
    published_at  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    category      TEXT,
    nominee       TEXT,
    artist        TEXT,
    workers       TEXT,
    img           TEXT,
    winner        BOOLEAN
);

-- 4. Carga del CSV original tal como fue entregado
COPY grammy_awards (year, title, published_at, updated_at, category,
                    nominee, artist, workers, img, winner)
FROM '/data/raw/the_grammy_awards.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- 5. Conciliacion: el resultado debe ser igual al numero de registros del CSV (4810)
SELECT COUNT(*) AS filas_cargadas FROM grammy_awards;