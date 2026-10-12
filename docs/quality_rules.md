# Riesgos y reglas de calidad de datos

> Secciones 6.3, 6.5 y 6.6 del enunciado. La evidencia viene de
> `notebooks/data_profiling.ipynb` (tablas en `docs/evidence/profiling/`).
> Las reglas están implementadas en `src/validation.py`: cada Expectation lleva
> `meta.rule_id` = ID de la regla y su severidad.

## 1. Evidencia del perfilado → riesgo → requerimiento

| ID | Dataset / atributo | Evidencia del perfilado | Riesgo potencial | Req. |
| --- | --- | --- | --- | --- |
| P01 | Spotify / `Unnamed: 0` | Columna sin nombre = consecutivo 0..n-1 | Se confunde con un identificador | — |
| P02 | Spotify / `artists`, `track_name`, `album_name` | 1 fila con los tres campos vacíos | Canción no asociable a ningún artista | R1–R4 |
| P03 | Spotify / fila completa | 450 duplicados exactos | Inflan conteos y promedios | R1–R3 |
| P04 | Spotify / `track_id` | 89,741 ids en 114,000 filas; 16,299 canciones en varios géneros | `track_id` no es llave; duplicar canciones en promedios | R1–R3 |
| P05 | Spotify / `popularity` | 720 canciones con popularidad distinta según la fila | Una canción con dos popularidades | R1, R4 |
| P06 | Spotify / `tempo`, `time_signature` | 157 y 163 filas en 0 (físicamente imposible) | Distorsionan el perfil sonoro | R3 |
| P07 | Spotify / características de audio | Hoy todas en [0, 1] | Un cambio de la fuente invalidaría el perfil sonoro | R3 |
| P08 | Spotify / `popularity` | 14.05 % de filas con 0 | Valor válido de alto peso en promedios | R1, R4 |
| P09 | Grammy / `winner`, `year` | 1958–2018: máx. 2 registros por (año, categoría). 2019: 433 registros, 84 categorías con 5–8 "ganadores" | En 2019 `winner` no distingue ganadores de nominados | R1, R2, R4 |
| P10 | Grammy / `artist` | 38.25 % vacío, concentrado en categorías de obra/persona; en *New Artist* el artista está en `nominee` | Baja cobertura del cruce | R1, R2, R4 |
| P11 | Grammy / `artist` | Artistas múltiples (` & `, ` Featuring `, `, `) | No cruzan o generan coincidencias falsas | R1, R2, R4 |
| P12 | Grammy / `category` | 638 categorías; 630 ignorando mayúsculas | La misma categoría contada como distinta | R2 |
| P13 | Grammy / `title` | Doble espacio en el 100 % | Estandarización (bajo impacto) | — |
| P14 | Grammy / `year` | 1958–2019, sin vacíos, coincide con `title` | Contrato a vigilar para las décadas de R4 | R4 |
| P15 | Cruce Spotify ↔ Grammy | 29.7 % de filas Grammy cruzan solo normalizando (escenario 3) | Cobertura limitada; un cambio brusco = falla de integración | R1–R4 |
| P16 | Cruce Spotify ↔ Grammy | 87 nombres normalizados agrupan varias escrituras | Coincidencias ambiguas | R1, R2, R4 |
| P17 | Spotify / `artists` | Créditos con texto `N/A`, `Unknown`, `Various Artists` (4 créditos) | Nulos disfrazados que crearían "artistas" falsos | R1, R2 |

## 2. Reglas de calidad

Severidad y política (sección 6.5 del enunciado):

| Severidad | Significado | Respuesta del pipeline |
| --- | --- | --- |
| **Critical** | Continuar es inseguro | La tarea de validación **falla** y todo lo posterior queda bloqueado |
| **Warning** | Requiere visibilidad, no invalida el lote | Se registra en el log y en el JSON del resultado; el pipeline continúa y la transformación aplica el tratamiento documentado |
| **Info** | Contexto de monitoreo / tendencia | Se registra; nunca bloquea |

| Rule ID | Dataset / Capa | Atributo(s) | Dimensión | Regla de calidad | Métrica / Umbral | Severidad | Req. | Justificación |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ01 | Spotify / raw | columnas requeridas | Validez (contrato) | Existen todas las columnas que usan R1–R4 | 100 % presentes | Critical | R1–R4 | Contrato de la fuente: sin una columna requerida ningún KPI se puede calcular |
| DQ02 | Spotify / raw | `track_id` | Completitud | `track_id` no vacío | 100 % | Critical | R1–R3 | Es la llave de negocio de la canción (`dim_track`) |
| DQ03 | Spotify / raw | `artists` | Completitud | `artists` no vacío | ≥ 99.9 % | Critical | R1–R4 | P02. Sin artista no hay integración. Se tolera ≤ 0.1 % (114 filas) porque esas filas se excluyen sin sesgar KPIs; más que eso indicaría un archivo dañado |
| DQ04 | Spotify / raw | `popularity` | Validez | Entre 0 y 100 | 100 % | Critical | R1, R4 | Contrato de la API de Spotify. Un valor fuera de rango corrompe directamente KPI-1 y KPI-6. **Regla usada en la Prueba B** |
| DQ05 | Spotify / raw | 7 características de audio | Validez | Entre 0 y 1 | 100 % | Critical | R3 | P07. Contrato de la API de Spotify; KPI-5 promedia estas columnas |
| DQ06 | Spotify / raw | `track_genre` | Completitud | No vacío | 100 % | Critical | R2 | R2 agrupa por género |
| DQ07 | Spotify / raw | (`track_id`, `track_genre`) | Unicidad | Combinación única | ≥ 99 % | Warning | R1–R3 | P03/P04. La granularidad real es canción × género. Hoy 0.78 % de filas están duplicadas: se tolera hasta 1 % porque T03 las elimina; más indicaría una exportación repetida |
| DQ08 | Spotify / raw | `tempo` | Validez | `tempo` > 0 | ≥ 99.5 % | Warning | R3 | P06. Hoy 0.14 % en 0; T05 los convierte en nulo. Más de 0.5 % afectaría el perfil sonoro |
| DQ09 | Grammy / raw | columnas requeridas | Validez (contrato) | Existen `year`, `title`, `category`, `nominee`, `artist`, `winner` | 100 % | Critical | R1–R4 | Contrato de la tabla `grammy_source.grammy_awards` |
| DQ10 | Grammy / raw | `year` | Validez / Completitud | No vacío y entre 1958 y el año actual | 100 % | Critical | R4 | P14. 1958 = primera ceremonia; no existen ceremonias futuras. R4 construye décadas con este campo |
| DQ11 | Grammy / raw | `category` | Completitud | No vacía | 100 % | Critical | R2 | El premio no tiene sentido sin categoría (`dim_category`) |
| DQ12 | Grammy / raw | `artist` | Completitud | No vacío | ≥ 55 % | Warning | R1, R2, R4 | P10. El vacío es estructural (≈ 38 % son categorías de obra/persona). Bajar de 55 % indicaría un cambio de la fuente, no la estructura conocida |
| DQ13 | Grammy / raw | (`year`, `category`) | Unicidad / Consistencia | Un registro por (año, categoría) en un lote de ganadores | ≥ 98 % | Warning | R1, R2, R4 | P09. Los empates reales ocupan 1.42 % de las filas (1958–2018); por eso 98 %. **Hoy falla (89.7 %) por el lote 2019** → T09 pone esas filas en cuarentena |
| DQ14 | Preparada / `fact_track_artist` | esquema | Validez | Existen llaves y medidas esperadas | 100 % | Critical | R1–R3 | Invariante: la carga espera estas columnas |
| DQ15 | Preparada / `fact_track_artist` | (`track_key`, `artist_key`, `genre_key`) | Unicidad | Combinación única (= granularidad declarada) | 100 % | Critical | R1–R3 | Invariante post-transformación: el grano debe cumplirse exactamente |
| DQ16 | Preparada / hechos | llaves foráneas | Integridad referencial | Ninguna llave vacía | 100 % | Critical | R1–R4 | Una fila sin llave rompería las FK del DW |
| DQ17 | Preparada / `fact_track_artist` | `popularity`, audio | Validez | Rangos 0–100 y 0–1, sin nulos en `popularity` | 100 % | Critical | R1, R3, R4 | Invariante: T04 no puede producir valores fuera de rango |
| DQ18 | Preparada / `fact_track_artist` | `tempo` | Validez | `tempo` > 0 cuando existe | 100 % | Critical | R3 | Invariante de T05 (los 0 pasaron a nulo) |
| DQ19 | Preparada / `fact_grammy_award` | `records_in_year_category` | Consistencia | Entre 1 y 2 registros por (año, categoría) | 100 % | Critical | R1, R2, R4 | Invariante de T09: ningún lote en cuarentena llega al DW |
| DQ20 | Preparada / `dim_artist` | `artist_name_norm`, `artist_key`, `is_grammy_winner` | Unicidad / Validez | Llave de negocio y subrogada únicas; bandera booleana | 100 % | Critical | R1–R4 | La dimensión conformada es el punto de integración: un duplicado duplicaría hechos |
| DQ21 | Preparada / `fact_grammy_award` | `matched_spotify` | Consistencia (integración) | % de premios cuyo artista se encontró en Spotify | entre 40 % y 70 % | Warning | R1–R4 | P15. Línea base 53 %. Menos de 40 % indicaría que la normalización o la extracción se rompió; más de 70 % sugeriría coincidencias falsas |
| DQ22 | Spotify / raw | `popularity` | Monitoreo | Popularidad media del lote | entre 25 y 45 | Info | R1, R4 | P08. Línea base 33.2. Una desviación fuerte es una señal de tendencia o de un lote diferente; no bloquea |

**Cobertura:** 22 reglas: 13 raw (8 Spotify, 5 Grammy) y 9 preparadas. No se fuerza el mismo número
por dataset: Spotify alimenta más medidas (R1, R3, R4), por eso tiene más reglas.

## 3. Diseño en Great Expectations (GX Core 1.23.1)

| Elemento GX | Implementación en el proyecto |
| --- | --- |
| **Data Source / Asset / Batch Definition** | `add_pandas` → `add_dataframe_asset` → `add_batch_definition_whole_dataframe`. Cada lote es el DataFrame del archivo raw o preparado de **esa ejecución** (`data/work/<run_id>/`) |
| **Expectation Suites** | `spotify_raw_suite` · `grammy_raw_suite` · `prepared_fact_track_artist_suite` · `prepared_fact_grammy_award_suite` · `prepared_dim_artist_suite`. Se definen en código (`src/validation.py`, versionado en Git) y se exportan en cada ejecución a `gx/expectations/<suite>.json` |
| **Validation Definition** | `<suite>_validation`: asocia la batch definition con su suite |
| **Ejecución controlada** | `run_suite()` ejecuta la Validation Definition, guarda el resultado y devuelve la severidad máxima (`get_max_severity_failure()`). `enforce_policy()` aplica la política: Critical → excepción (la tarea de Airflow falla), Warning/Info → log y continúa. Es el mismo patrón de la Actividad 3 del curso |
| **Resultados** | `gx/validation_results/<run_id>/<suite>.json`: éxito global, estadísticas, **resumen por regla** (rule_id, severidad, % inesperado, valor observado) y el resultado completo de GX |

### Mapeo Expectation → Rule ID

| Suite | Expectation (columna) | Rule ID |
| --- | --- | --- |
| spotify_raw_suite | `ExpectTableColumnsToMatchSet` (columnas requeridas, `exact_match=False`) | DQ01 |
| spotify_raw_suite | `ExpectColumnValuesToNotBeNull` (`track_id`) | DQ02 |
| spotify_raw_suite | `ExpectColumnValuesToNotBeNull` (`artists`, `mostly=0.999`) | DQ03 |
| spotify_raw_suite | `ExpectColumnValuesToBeBetween` (`popularity`, 0–100) | DQ04 |
| spotify_raw_suite | `ExpectColumnValuesToBeBetween` × 7 (características de audio, 0–1) | DQ05 |
| spotify_raw_suite | `ExpectColumnValuesToNotBeNull` (`track_genre`) | DQ06 |
| spotify_raw_suite | `ExpectCompoundColumnsToBeUnique` (`track_id`, `track_genre`, `mostly=0.99`) | DQ07 |
| spotify_raw_suite | `ExpectColumnValuesToBeBetween` (`tempo` > 0, `mostly=0.995`) | DQ08 |
| spotify_raw_suite | `ExpectColumnMeanToBeBetween` (`popularity`, 25–45) | DQ22 |
| grammy_raw_suite | `ExpectTableColumnsToMatchSet` (columnas requeridas) | DQ09 |
| grammy_raw_suite | `ExpectColumnValuesToNotBeNull` + `ExpectColumnValuesToBeBetween` (`year`) | DQ10 |
| grammy_raw_suite | `ExpectColumnValuesToNotBeNull` (`category`) | DQ11 |
| grammy_raw_suite | `ExpectColumnValuesToNotBeNull` (`artist`, `mostly=0.55`) | DQ12 |
| grammy_raw_suite | `ExpectCompoundColumnsToBeUnique` (`year`, `category`, `mostly=0.98`) | DQ13 |
| prepared_fact_track_artist_suite | `ExpectTableColumnsToMatchSet` | DQ14 |
| prepared_fact_track_artist_suite | `ExpectCompoundColumnsToBeUnique` (llaves del grano) | DQ15 |
| prepared_fact_track_artist_suite / prepared_fact_grammy_award_suite | `ExpectColumnValuesToNotBeNull` (llaves foráneas) | DQ16 |
| prepared_fact_track_artist_suite | `ExpectColumnValuesToBeBetween` / `NotBeNull` (`popularity`, audio) | DQ17 |
| prepared_fact_track_artist_suite | `ExpectColumnValuesToBeBetween` (`tempo` > 0) | DQ18 |
| prepared_fact_grammy_award_suite | `ExpectColumnValuesToBeBetween` (`records_in_year_category`, 1–2) | DQ19 |
| prepared_dim_artist_suite | `NotBeNull` / `ToBeUnique` / `ToBeInSet` | DQ20 |
| prepared_fact_grammy_award_suite | `ExpectColumnMeanToBeBetween` (`matched_spotify`, 0.40–0.70) | DQ21 |

## 4. Resultado esperado con los datos entregados

| Suite | Resultado | Detalle |
| --- | --- | --- |
| spotify_raw_suite | ✅ Pasa | Sin fallas Critical ni Warning |
| grammy_raw_suite | ⚠️ Pasa con Warning | **DQ13** falla (89.7 % < 98 %) por el lote 2019 → la política permite continuar y T09 aplica la cuarentena |
| suites preparadas | ✅ Pasan | DQ21 observado: 53 % de premios con artista encontrado en Spotify |
| spotify_raw_suite con `spotify_bad_popularity.csv` | ❌ Falla Critical | **DQ04**: 1 fila con `popularity = 150` → Prueba B |
