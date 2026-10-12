"""
TRANSFORM — una sola fase de ingenieria: limpieza, estandarizacion,
armonizacion, integracion Spotify <-> Grammy y construccion de las tablas
del modelo dimensional (ver docs/transformation_decisions.md).

Cada regla tiene un ID (T01...) que se registra en el reporte del lote
(integration_report.json) con las filas afectadas, para que la
transformacion sea auditable.

Importante: ninguna regla existe para "hacer pasar" una validacion. Cada una
se justifica por evidencia del perfilado o por un requerimiento (R1-R4).
"""

import json
import logging
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

from src import config
from src.files import read_table

log = logging.getLogger(__name__)

# Textos que aparecen como "artista" pero no identifican a nadie (evidencia del perfilado)
PLACEHOLDER_ARTISTS = {"n/a", "na", "unknown", "various artists", "none", "null"}

PRIMARY_ARTIST_SPLIT = re.compile(r" featuring | feat\. | & | with |, ", flags=re.IGNORECASE)


# ---------------------------------------------------------------------------
# Utilidades de estandarizacion
# ---------------------------------------------------------------------------
def normalize_name(value):
    """Llave de emparejamiento de artistas (T07).

    minusculas, sin tildes, sin signos de puntuacion y sin espacios repetidos.
    Ej.: 'Beyoncé' -> 'beyonce', 'Guns N' Roses' -> 'guns n roses'.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-z0-9 ]", " ", text.lower())
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def collapse_spaces(series):
    return series.astype("string").str.replace(r"\s+", " ", regex=True).str.strip()


def _most_frequent(series):
    """Escritura mas frecuente (desempate alfabetico, para que sea determinista)."""
    counts = series.value_counts()
    top = counts[counts == counts.max()].index
    return sorted(top)[0]


# ---------------------------------------------------------------------------
# Spotify
# ---------------------------------------------------------------------------
def prepare_spotify(raw, report):
    df = raw.copy()
    n0 = len(df)

    # T01 - Eliminar la columna sin nombre (indice de exportacion, evidencia P01)
    unnamed = [c for c in df.columns if str(c).startswith("Unnamed")]
    df = df.drop(columns=unnamed)

    # T02 - Excluir filas sin artista: no se pueden asociar a ningun artista (P02)
    no_artist = df["artists"].isna() | (df["artists"].astype("string").str.strip() == "")
    df = df[~no_artist]

    # T03 - Eliminar duplicados exactos (P03); se conserva la primera aparicion
    before = len(df)
    df = df.drop_duplicates()
    exact_dups = before - len(df)

    # T04 - Armonizar popularity: una canción tiene UNA popularidad (P05).
    #        Se usa el promedio redondeado de sus filas.
    pop_variants = int((df.groupby("track_id")["popularity"].nunique() > 1).sum())
    df["popularity"] = (df.groupby("track_id")["popularity"].transform("mean").round().astype(int))

    # T05 - Valores fisicamente imposibles -> nulo (P06). No se inventa un valor.
    impossible = {}
    for col in ["tempo", "time_signature", "duration_ms"]:
        mask = df[col] == 0
        impossible[col] = int(mask.sum())
        df.loc[mask, col] = np.nan
    for col in ["time_signature", "duration_ms"]:
        df[col] = df[col].astype("Int64")   # enteros que admiten nulo

    # T06 - Separar artistas multiples (';') en una fila por artista acreditado (P11)
    df["artist_name"] = df["artists"].astype("string").str.split(";")
    df = df.explode("artist_name")
    df["artist_name"] = collapse_spaces(df["artist_name"])
    # Marcadores que no son artistas reales (nulos disfrazados) se descartan del credito
    placeholder = df["artist_name"].str.lower().isin(PLACEHOLDER_ARTISTS)
    placeholders_removed = int(placeholder.sum())
    df = df[~placeholder]
    df["artist_position"] = df.groupby(level=0).cumcount() + 1
    df = df.reset_index(drop=True)

    # T07 - Llave de emparejamiento normalizada
    df["artist_name_norm"] = df["artist_name"].map(normalize_name)
    df = df[df["artist_name_norm"].notna()]

    # Si un artista aparece dos veces en la misma canción/genero (p. ej. dos escrituras
    # que normalizan igual), se conserva la primera posicion.
    before = len(df)
    df = df.sort_values("artist_position").drop_duplicates(
        ["track_id", "track_genre", "artist_name_norm"], keep="first")
    repeated_artist = before - len(df)

    report["spotify"] = {
        "rows_raw": n0,
        "T01_columns_dropped": unnamed,
        "T02_rows_without_artist_excluded": int(no_artist.sum()),
        "T03_exact_duplicates_removed": int(exact_dups),
        "T04_tracks_with_popularity_harmonized": pop_variants,
        "T05_impossible_values_set_to_null": impossible,
        "T06_placeholder_artist_credits_removed": placeholders_removed,
        "T06_rows_after_artist_split": int(len(df) + repeated_artist),
        "T06_repeated_artist_in_same_track_removed": int(repeated_artist),
        "rows_prepared_track_artist_genre": int(len(df)),
        "distinct_tracks": int(df["track_id"].nunique()),
        "distinct_artists": int(df["artist_name_norm"].nunique()),
    }
    return df


# ---------------------------------------------------------------------------
# Grammy
# ---------------------------------------------------------------------------
def prepare_grammy(raw, spotify_artists, report, quarantine_path):
    df = raw.copy()
    n0 = len(df)

    # T08 - Estandarizar textos: espacios repetidos (P13) y categorias con distinta
    #        capitalizacion -> escritura mas frecuente (P12)
    df["title"] = collapse_spaces(df["title"])
    df["category"] = collapse_spaces(df["category"])
    key = df["category"].str.lower()
    canonical = df.groupby(key)["category"].agg(_most_frequent)
    df["category_std"] = key.map(canonical)
    category_variants = int(df["category"].nunique() - df["category_std"].nunique())

    # T09 - Cuarentena de lotes donde 'winner' no distingue ganadores (P09, DQ13):
    #        combinaciones (year, category) con mas de MAX_WINNERS_PER_CATEGORY registros.
    df["records_in_year_category"] = df.groupby(["year", "category_std"])["category_std"].transform("size")
    quarantine = df["records_in_year_category"] > config.MAX_WINNERS_PER_CATEGORY
    df[quarantine].to_csv(quarantine_path, index=False)
    quarantined_years = sorted(int(y) for y in df.loc[quarantine, "year"].unique())
    df = df[~quarantine].copy()

    # T10 - En categorias de 'New Artist' el artista viene en 'nominee' (P10)
    new_artist = df["category_std"].str.contains("new artist", case=False) & df["artist"].isna()
    df["artist_name"] = df["artist"].where(~new_artist, df["nominee"])
    df["artist_name"] = collapse_spaces(df["artist_name"])

    # T11 - Sin artista no hay a quien atribuir el premio (obras, notas, ingenieria)
    no_artist = df["artist_name"].isna()
    df = df[~no_artist].copy()

    # T12 - Emparejamiento con Spotify (contrato de integracion, P11/P15/P16):
    #   1) nombre completo normalizado;
    #   2) si no existe en Spotify, artista principal (antes de Featuring/&/With/,).
    df["name_full_norm"] = df["artist_name"].map(normalize_name)
    df["name_primary_norm"] = df["artist_name"].map(
        lambda x: normalize_name(PRIMARY_ARTIST_SPLIT.split(x)[0]))
    full_hit = df["name_full_norm"].isin(spotify_artists)
    primary_hit = ~full_hit & df["name_primary_norm"].isin(spotify_artists)
    df["match_method"] = np.select([full_hit, primary_hit], ["full_name", "primary_artist"], "unmatched")
    df["artist_name_norm"] = np.where(primary_hit, df["name_primary_norm"], df["name_full_norm"])
    df["matched_spotify"] = (df["match_method"] != "unmatched").astype(int)

    report["grammy"] = {
        "rows_raw": n0,
        "T08_category_variants_unified": category_variants,
        "T09_rows_quarantined": int(quarantine.sum()),
        "T09_years_affected": quarantined_years,
        "T09_quarantine_file": str(quarantine_path),
        "T10_artist_taken_from_nominee": int(new_artist.sum()),
        "T11_rows_without_artist_excluded": int(no_artist.sum()),
        "rows_prepared_awards": int(len(df)),
        "T12_match_full_name": int(full_hit.sum()),
        "T12_match_primary_artist": int(primary_hit.sum()),
        "T12_unmatched": int((df["match_method"] == "unmatched").sum()),
        "match_rate_awards": round(float(df["matched_spotify"].mean()), 4) if len(df) else None,
    }
    return df


# ---------------------------------------------------------------------------
# Modelo dimensional
# ---------------------------------------------------------------------------
def _surrogate(values, key_name):
    """Llaves subrogadas deterministas: 1..n sobre la llave de negocio ordenada.

    Mismo input -> mismas llaves en cada re-ejecucion (apoya el rerun seguro).
    """
    ordered = sorted(pd.Series(values).dropna().unique())
    return pd.DataFrame({key_name: range(1, len(ordered) + 1), "_bk": ordered})


def build_dimensional_model(sp, gr):
    tables = {}

    # dim_genre
    g = _surrogate(sp["track_genre"], "genre_key").rename(columns={"_bk": "genre_name"})
    tables["dim_genre"] = g

    # dim_track (atributos descriptivos de la cancion)
    t = _surrogate(sp["track_id"], "track_key").rename(columns={"_bk": "track_id"})
    attrs = (sp.sort_values("artist_position")
               .drop_duplicates("track_id")[["track_id", "track_name", "album_name",
                                             "duration_ms", "explicit", "artists"]]
               .rename(columns={"artists": "artists_credited"}))
    t = t.merge(attrs, on="track_id", how="left")
    t["duration_min"] = (t["duration_ms"] / 60000).round(2)
    tables["dim_track"] = t

    # dim_year (granularidad anual de la ceremonia)
    y = gr.groupby("year").agg(ceremony_title=("title", "first")).reset_index()
    y["decade"] = (y["year"] // 10 * 10).astype(int)
    y["decade_label"] = y["decade"].astype(str) + "s"
    y.insert(0, "year_key", y["year"].astype(int))
    tables["dim_year"] = y

    # dim_category
    c = _surrogate(gr["category_std"], "category_key").rename(columns={"_bk": "category_name"})
    tables["dim_category"] = c

    # dim_artist (dimension CONFORMADA: une artistas de ambas fuentes)
    sp_names = sp.groupby("artist_name_norm")["artist_name"].agg(_most_frequent)
    gr_names = gr.groupby("artist_name_norm")["artist_name"].agg(_most_frequent)
    names = pd.concat([sp_names, gr_names[~gr_names.index.isin(sp_names.index)]])
    a = _surrogate(names.index, "artist_key").rename(columns={"_bk": "artist_name_norm"})
    a["artist_name"] = a["artist_name_norm"].map(names)
    awards = gr.groupby("artist_name_norm").agg(
        grammy_awards=("year", "size"),
        first_award_year=("year", "min"),
        last_award_year=("year", "max"),
    )
    a = a.merge(awards, left_on="artist_name_norm", right_index=True, how="left")
    a["grammy_awards"] = a["grammy_awards"].fillna(0).astype(int)
    a["is_grammy_winner"] = a["grammy_awards"] > 0
    a["in_spotify"] = a["artist_name_norm"].isin(sp_names.index)
    a["last_award_decade"] = (a["last_award_year"] // 10 * 10).astype("Int64")
    a["first_award_year"] = a["first_award_year"].astype("Int64")
    a["last_award_year"] = a["last_award_year"].astype("Int64")
    tables["dim_artist"] = a

    # fact_track_artist  (grano: cancion x artista acreditado x genero)
    f = (sp.merge(t[["track_key", "track_id"]], on="track_id")
           .merge(a[["artist_key", "artist_name_norm"]], on="artist_name_norm")
           .merge(g, left_on="track_genre", right_on="genre_name"))
    measures = ["popularity", *config.AUDIO_FEATURES, "tempo", "loudness",
                "key", "mode", "time_signature"]
    tables["fact_track_artist"] = f[["track_key", "artist_key", "genre_key",
                                     "artist_position", *measures]].sort_values(
        ["track_key", "artist_key", "genre_key"]).reset_index(drop=True)

    # fact_grammy_award  (grano: un premio ganado por un artista en una categoria y año)
    fa = (gr.merge(a[["artist_key", "artist_name_norm"]], on="artist_name_norm")
            .merge(c, left_on="category_std", right_on="category_name"))
    fa["year_key"] = fa["year"].astype(int)
    fa["award_count"] = 1
    tables["fact_grammy_award"] = fa[["source_id", "year_key", "category_key", "artist_key",
                                      "nominee", "match_method", "matched_spotify",
                                      "records_in_year_category", "award_count"]].sort_values(
        "source_id").reset_index(drop=True)
    return tables


# ---------------------------------------------------------------------------
# Punto de entrada de la tarea transform_and_integrate
# ---------------------------------------------------------------------------
def transform_and_integrate(spotify_raw_path, grammy_raw_path):
    batch_dir = Path(spotify_raw_path).parent.parent
    out_dir = batch_dir / "prepared"
    out_dir.mkdir(parents=True, exist_ok=True)
    report = {}

    spotify = read_table(spotify_raw_path)
    grammy = read_table(grammy_raw_path)

    sp = prepare_spotify(spotify, report)
    gr = prepare_grammy(grammy, set(sp["artist_name_norm"]), report,
                        quarantine_path=out_dir / "grammy_quarantine.csv")
    tables = build_dimensional_model(sp, gr)

    paths = {}
    for name, table in tables.items():
        path = out_dir / f"{name}.csv"
        table.to_csv(path, index=False)
        paths[name] = str(path)

    dim_artist = tables["dim_artist"]
    report["integration"] = {
        "artists_total": int(len(dim_artist)),
        "artists_in_spotify": int(dim_artist["in_spotify"].sum()),
        "grammy_winner_artists": int(dim_artist["is_grammy_winner"].sum()),
        "grammy_winners_found_in_spotify": int((dim_artist["is_grammy_winner"] & dim_artist["in_spotify"]).sum()),
        "cardinality": "muchos a uno: varios premios y varias canciones por artista (dim_artist)",
    }
    report["row_counts"] = {name: int(len(t)) for name, t in tables.items()}
    report_path = out_dir / "integration_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    log.info("Reporte de transformacion e integracion:\n%s", json.dumps(report, indent=2, ensure_ascii=False))
    return {"tables": paths, "report": str(report_path), "row_counts": report["row_counts"]}
