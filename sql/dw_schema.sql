-- =============================================================================
-- dw_schema.sql — Esquema estrella del Data Warehouse "music_dw"
--
-- Dos procesos de negocio que comparten la dimension CONFORMADA dim_artist:
--   fact_track_artist   grano: una cancion de Spotify x un artista acreditado x un genero
--   fact_grammy_award   grano: un premio Grammy ganado por un artista, en una categoria y un año
--
-- El script es idempotente (IF NOT EXISTS): load_dw lo ejecuta en cada corrida
-- antes de cargar, y nunca borra la estructura existente.
-- =============================================================================

-- ------------------------------ Dimensiones ---------------------------------
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key         INTEGER      PRIMARY KEY,           -- llave subrogada
    artist_name_norm   TEXT         NOT NULL UNIQUE,       -- llave de negocio (nombre normalizado)
    artist_name        TEXT         NOT NULL,              -- nombre para mostrar
    is_grammy_winner   BOOLEAN      NOT NULL,
    grammy_awards      INTEGER      NOT NULL CHECK (grammy_awards >= 0),
    first_award_year   INTEGER,
    last_award_year    INTEGER,
    last_award_decade  INTEGER,
    in_spotify         BOOLEAN      NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_track (
    track_key          INTEGER      PRIMARY KEY,
    track_id           TEXT         NOT NULL UNIQUE,       -- llave de negocio (Spotify)
    track_name         TEXT,
    album_name         TEXT,
    artists_credited   TEXT,
    duration_ms        INTEGER,
    duration_min       NUMERIC(8,2),
    explicit           BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_genre (
    genre_key          INTEGER      PRIMARY KEY,
    genre_name         TEXT         NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_key       INTEGER      PRIMARY KEY,
    category_name      TEXT         NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_key           INTEGER      PRIMARY KEY,           -- el propio año (llave natural estable)
    year               INTEGER      NOT NULL UNIQUE,
    decade             INTEGER      NOT NULL,
    decade_label       TEXT         NOT NULL,
    ceremony_title     TEXT
);

-- -------------------------------- Hechos ------------------------------------
CREATE TABLE IF NOT EXISTS fact_track_artist (
    track_key          INTEGER      NOT NULL REFERENCES dim_track (track_key),
    artist_key         INTEGER      NOT NULL REFERENCES dim_artist (artist_key),
    genre_key          INTEGER      NOT NULL REFERENCES dim_genre (genre_key),
    artist_position    SMALLINT     NOT NULL,              -- 1 = artista principal
    popularity         SMALLINT     NOT NULL CHECK (popularity BETWEEN 0 AND 100),
    danceability       REAL         CHECK (danceability BETWEEN 0 AND 1),
    energy             REAL         CHECK (energy BETWEEN 0 AND 1),
    speechiness        REAL         CHECK (speechiness BETWEEN 0 AND 1),
    acousticness       REAL         CHECK (acousticness BETWEEN 0 AND 1),
    instrumentalness   REAL         CHECK (instrumentalness BETWEEN 0 AND 1),
    liveness           REAL         CHECK (liveness BETWEEN 0 AND 1),
    valence            REAL         CHECK (valence BETWEEN 0 AND 1),
    tempo              REAL         CHECK (tempo > 0),
    loudness           REAL,
    key                SMALLINT,
    mode               SMALLINT,
    time_signature     SMALLINT,
    PRIMARY KEY (track_key, artist_key, genre_key)
);

CREATE TABLE IF NOT EXISTS fact_grammy_award (
    source_id                 BIGINT    PRIMARY KEY,      -- id de la fila en grammy_source (trazabilidad)
    year_key                  INTEGER   NOT NULL REFERENCES dim_year (year_key),
    category_key              INTEGER   NOT NULL REFERENCES dim_category (category_key),
    artist_key                INTEGER   NOT NULL REFERENCES dim_artist (artist_key),
    nominee                   TEXT,                       -- obra premiada (dimension degenerada)
    match_method              TEXT      NOT NULL,         -- full_name | primary_artist | unmatched
    matched_spotify           SMALLINT  NOT NULL CHECK (matched_spotify IN (0, 1)),
    records_in_year_category  SMALLINT  NOT NULL,
    award_count               SMALLINT  NOT NULL DEFAULT 1
);

-- ----------------------- Auditoria de cargas (no se trunca) ------------------
CREATE TABLE IF NOT EXISTS etl_load_audit (
    audit_id        BIGSERIAL    PRIMARY KEY,
    run_id          TEXT         NOT NULL,
    loaded_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    strategy        TEXT         NOT NULL,
    row_counts      JSONB        NOT NULL
);

-- Indices para las consultas analiticas
CREATE INDEX IF NOT EXISTS ix_fta_artist ON fact_track_artist (artist_key);
CREATE INDEX IF NOT EXISTS ix_fta_genre  ON fact_track_artist (genre_key);
CREATE INDEX IF NOT EXISTS ix_fga_artist ON fact_grammy_award (artist_key);
CREATE INDEX IF NOT EXISTS ix_fga_year   ON fact_grammy_award (year_key);

-- =============================================================================
-- Vistas KPI (una por requerimiento). El dashboard y publish_kpi_summary las
-- consultan: el calculo vive en el DW, no en la herramienta de BI.
-- Las canciones se cuentan una sola vez (DISTINCT) aunque aparezcan en varios
-- generos, para no inflar los promedios (riesgo P04).
-- =============================================================================

-- R1: popularidad de artistas ganadores vs. no ganadores (KPI-1, KPI-2)
CREATE OR REPLACE VIEW kpi_r1_popularity_by_winner AS
WITH artist_track AS (
    SELECT DISTINCT artist_key, track_key, popularity FROM fact_track_artist
), artist_popularity AS (
    SELECT a.artist_key, a.is_grammy_winner, AVG(t.popularity) AS avg_popularity
    FROM artist_track t JOIN dim_artist a USING (artist_key)
    GROUP BY a.artist_key, a.is_grammy_winner
)
SELECT CASE WHEN is_grammy_winner THEN 'Ganador Grammy' ELSE 'Sin Grammy' END AS grupo,
       COUNT(*)                                             AS artistas,
       ROUND(AVG(avg_popularity)::numeric, 2)               AS popularidad_promedio,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)   AS pct_artistas
FROM artist_popularity
GROUP BY is_grammy_winner;

-- R2: generos que concentran artistas ganadores (KPI-3, KPI-4)
CREATE OR REPLACE VIEW kpi_r2_genre_winners AS
WITH genre_artist AS (
    SELECT DISTINCT f.genre_key, f.artist_key FROM fact_track_artist f
)
SELECT g.genre_name                                                   AS genero,
       COUNT(*)                                                       AS artistas,
       COUNT(*) FILTER (WHERE a.is_grammy_winner)                     AS artistas_ganadores,
       ROUND(100.0 * COUNT(*) FILTER (WHERE a.is_grammy_winner) / COUNT(*), 2) AS pct_artistas_ganadores,
       SUM(a.grammy_awards)                                           AS premios_grammy
FROM genre_artist ga
JOIN dim_genre g  USING (genre_key)
JOIN dim_artist a USING (artist_key)
GROUP BY g.genre_name
ORDER BY pct_artistas_ganadores DESC, premios_grammy DESC;

-- R3: perfil sonoro de canciones de artistas ganadores vs. resto (KPI-5)
CREATE OR REPLACE VIEW kpi_r3_audio_profile AS
WITH track_group AS (
    SELECT f.track_key,
           BOOL_OR(a.is_grammy_winner)  AS has_winner_artist,
           AVG(f.danceability)          AS danceability,
           AVG(f.energy)                AS energy,
           AVG(f.valence)               AS valence,
           AVG(f.acousticness)          AS acousticness,
           AVG(f.instrumentalness)      AS instrumentalness,
           AVG(f.speechiness)           AS speechiness
    FROM fact_track_artist f JOIN dim_artist a USING (artist_key)
    GROUP BY f.track_key
)
SELECT CASE WHEN has_winner_artist THEN 'Artista ganador' ELSE 'Sin Grammy' END AS grupo,
       COUNT(*)                                   AS canciones,
       ROUND(AVG(danceability)::numeric, 3)       AS danceability,
       ROUND(AVG(energy)::numeric, 3)             AS energy,
       ROUND(AVG(valence)::numeric, 3)            AS valence,
       ROUND(AVG(acousticness)::numeric, 3)       AS acousticness,
       ROUND(AVG(instrumentalness)::numeric, 3)   AS instrumentalness,
       ROUND(AVG(speechiness)::numeric, 3)        AS speechiness,
       ROUND(100.0 * AVG(t.explicit::int), 2)     AS pct_explicitas
FROM track_group tg
JOIN dim_track t USING (track_key)
GROUP BY has_winner_artist;

-- R4: popularidad actual por decada del ultimo premio (KPI-6, KPI-7)
CREATE OR REPLACE VIEW kpi_r4_popularity_by_decade AS
WITH artist_track AS (
    SELECT DISTINCT artist_key, track_key, popularity FROM fact_track_artist
)
SELECT a.last_award_decade::text || 's'          AS decada_ultimo_premio,
       a.last_award_decade                       AS decada,
       COUNT(DISTINCT a.artist_key)              AS artistas_ganadores,
       ROUND(AVG(t.popularity)::numeric, 2)      AS popularidad_promedio
FROM artist_track t
JOIN dim_artist a USING (artist_key)
WHERE a.is_grammy_winner
GROUP BY a.last_award_decade
ORDER BY a.last_award_decade;
