# Registro de decisiones de transformación e integración

> Sección 6.8 del enunciado. Código: `src/transform.py`. Cada ejecución guarda
> en `data/work/<run_id>/prepared/integration_report.json` cuántas filas afectó
> cada regla (las cifras de abajo corresponden al lote entregado).
>
> **Ninguna regla existe para hacer pasar una validación.** Cada una se
> justifica por evidencia del perfilado (P01–P17) o por un requerimiento (R1–R4),
> y es defendible aunque la validación no existiera.

## Reglas de transformación

| ID | Regla y justificación | Campos / registros afectados | Antes → después | Manejo de excepciones | Impacto analítico |
| --- | --- | --- | --- | --- | --- |
| T01 | Eliminar la columna sin nombre: es un índice de exportación (P01) | `Unnamed: 0` | 21 → 20 columnas | — | Evita confundirla con una llave |
| T02 | Excluir canciones sin artista: no se pueden atribuir (P02) | 1 fila | 114,000 → 113,999 | Se cuenta en el reporte | R1–R4: toda medida necesita un artista |
| T03 | Eliminar duplicados exactos, conservando la primera aparición (P03) | 450 filas | — | — | R1–R3: no inflar promedios ni conteos |
| T04 | Armonizar `popularity`: una canción tiene una sola popularidad; se usa el promedio redondeado de sus filas (P05) | 720 canciones | Ej.: *Amake Amar Moto Thakte Dao*: 47 en 4 géneros y 46 en 2 → 47 en todas sus filas | — | R1, R4: el promedio por artista no depende de en qué género cayó la canción |
| T05 | Valores físicamente imposibles → nulo, sin inventar un reemplazo (P06) | `tempo` 157, `time_signature` 163 | `0` → `NULL` | Los nulos se ignoran en los promedios | R3: el perfil sonoro no se distorsiona |
| T06 | Separar artistas múltiples (`;`) en un crédito por artista con su posición (1 = principal); descartar marcadores que no son artistas (`N/A`, `Unknown`, `Various Artists`) (P11, P17) | 30,075 filas con varios artistas; 4 créditos marcador | `"Artista A;Artista B"` → 2 filas | Si dos escrituras normalizan igual en la misma canción, se conserva la primera (4 casos) | R1, R2: cada artista acreditado recibe la canción |
| T07 | Llave de emparejamiento: minúsculas, sin tildes, sin puntuación, sin espacios repetidos | artistas de ambas fuentes | `"Beyoncé"` → `beyonce` | Si queda vacía, la fila se descarta | Integración R1–R4 |
| T08 | Estandarizar texto: espacios repetidos y categorías con distinta capitalización → escritura más frecuente (P12, P13) | `title`, `category` | 8 variantes unificadas (ej. `"Best Spoken Word Album For Children"` / `"... for Children"`) | Desempate alfabético (determinista) | R2: una categoría se cuenta una vez |
| T09 | **Cuarentena** de (año, categoría) con más de 2 registros: en ese lote `winner` no distingue ganadores (P09, DQ13) | 433 filas, año 2019 | Salen del modelo y se guardan en `prepared/grammy_quarantine.csv` | No se adivina el ganador | R1, R2, R4: no se cuentan nominados como ganadores |
| T10 | En categorías *New Artist* el artista está en `nominee` (P10) | 57 filas | `artist` vacío → nombre desde `nominee` | — | Recupera ganadores de *Best New Artist* |
| T11 | Excluir premios sin artista atribuible (obras, notas, ingeniería) | 1,606 filas | — | Se cuentan en el reporte | El premio no se puede asignar a un artista |
| T12 | Emparejamiento con Spotify en 2 pasos (ver contrato) | 2,771 premios | 1,234 por nombre completo + 238 por artista principal | 1,299 sin coincidencia: se conservan (`matched_spotify = 0`) | R1–R4 |
| T13 | Construcción del modelo estrella con llaves subrogadas **deterministas** (1..n sobre la llave de negocio ordenada) | todas las tablas | — | — | Rerun seguro: mismo lote → mismas llaves |

## Contrato de integración

| Ítem | Decisión y evidencia |
| --- | --- |
| **Llave(s) de integración** | Nombre de artista normalizado (T07). Spotify: cada crédito de `artists` separado por `;`. Grammy: `artist` (o `nominee` en *New Artist*). Es el único atributo común; no existe un identificador compartido |
| **Cardinalidad** | **Muchos a uno hacia `dim_artist`** desde ambos lados: un artista tiene muchas canciones (`fact_track_artist`) y muchos premios (`fact_grammy_award`). Spotify ↔ Grammy queda **muchos a muchos** a través de la dimensión conformada. Se verifica con DQ15 (grano único) y DQ20 (artista único) |
| **Preprocesamiento** | T06 (separar créditos), T07 (normalizar), T10 (artista desde nominee), T08 (espacios) |
| **Estrategia de emparejamiento** | 1) nombre completo normalizado; 2) solo si no existe en Spotify, el **artista principal** (texto antes de ` Featuring `, ` feat. `, ` & `, ` With `, `, `). Ej.: *"Tony Bennett & Lady Gaga"* → `tony bennett`. El método queda en `match_method` |
| **Registros sin coincidencia** | Se **miden y conservan**: 1,299 premios (46.9 %) con `matched_spotify = 0`, cuyo artista entra a `dim_artist` con `in_spotify = false`. DQ21 vigila que la tasa de coincidencia quede entre 40 % y 70 % (hoy 53.1 %) |
| **Coincidencias duplicadas / ambiguas** | Escrituras distintas que normalizan igual se tratan como el mismo artista (87 casos en Spotify, P16); `dim_artist` tiene `artist_name_norm` UNIQUE (DQ20 + restricción en el DW), por lo que un artista nunca se duplica |
| **Supuestos y limitaciones** | (1) Mismo nombre normalizado = mismo artista (posibles homónimos). (2) En colaboraciones solo se acredita al artista principal cuando el nombre completo no existe: los colaboradores no reciben el premio. (3) El paso 2 puede partir nombres de grupo con `&` (*"Simon & Garfunkel"*), pero solo se usa si el nombre completo no está en Spotify. (4) Spotify es una muestra de 1,000 canciones por género: un ganador ausente de la muestra no es un error de integración |

## Resultado de la integración (lote entregado)

| Medida | Valor |
| --- | --- |
| Artistas en `dim_artist` | 30,249 |
| Artistas presentes en Spotify | 29,450 |
| Artistas ganadores del Grammy (1958–2018) | 1,348 |
| Ganadores encontrados en Spotify | 549 (40.7 %) |
| Premios cargados (`fact_grammy_award`) | 2,771 |
| Filas `fact_track_artist` | 156,739 |
