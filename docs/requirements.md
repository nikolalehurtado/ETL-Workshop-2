# Requerimientos analíticos y alcance de la solución

> Sección 6.1 del enunciado. Los IDs **R1–R4** se usan en todo el repositorio
> (reglas de calidad, Expectations de GX, transformaciones, modelo dimensional y
> dashboard) para mantener la trazabilidad de extremo a extremo.

## Declaración de alcance

El producto de datos responde **cómo se relaciona el reconocimiento de la
industria (premios Grammy ganados, 1958–2018) con el desempeño y el perfil
sonoro actual de los artistas en Spotify**.

- **Unidad de integración:** el **artista**, identificado por su nombre normalizado.
  Las dos fuentes no comparten ningún identificador; el nombre es el único
  atributo presente en ambas.
- **Spotify aporta** el desempeño actual (`popularity`), el género (`track_genre`)
  y el perfil sonoro (características de audio) de cada canción.
- **Grammy aporta** el reconocimiento: qué artistas ganaron, en qué años y en
  qué categorías.
- **Ninguna de las dos fuentes por sí sola responde estas preguntas.** Spotify no
  sabe qué artistas fueron premiados ni cuándo; Grammy no sabe qué tan
  escuchados son hoy esos artistas ni cómo suena su música.

### Decisiones de alcance basadas en evidencia del perfilado

| Decisión | Evidencia (`notebooks/data_profiling.ipynb`) | Consecuencia |
| --- | --- | --- |
| **El dataset de Grammy representa ganadores.** | De 1958 a 2018 hay en promedio **1 registro por (año, categoría)**; solo 31 combinaciones tienen 2 registros, coherentes con empates reales. | Los KPIs usan **premios ganados**. |
| **El lote 2019 no es confiable para identificar ganadores.** | En 2019 hay **433 registros** (frente a 86 en 2018) y 84 categorías con 5 a 8 registros cada una, **todos con `winner = True`**. Ej.: *Record Of The Year 2019* lista a las 8 nominadas como ganadoras, aunque solo ganó "Bad Guy". | Los registros de 2019 se **excluyen de los KPIs de ganadores** y se reportan como hallazgo de calidad. No se "corrigen" adivinando el ganador. |
| **La popularidad es una foto actual.** | Spotify no tiene fechas ni histórico. | `popularity` se interpreta como desempeño actual; el tiempo solo proviene de Grammy (`year`). |
| **El cruce es por nombre de artista, no por canción.** | No hay identificador compartido; en muchas categorías `nominee` es un álbum, una obra o una persona. | Las canciones de Spotify heredan la condición "artista ganador". |

## Tabla de requerimientos

| ID | Requerimiento analítico (pregunta / decisión) | Datos requeridos | Fuente(s) | KPI(s) esperado(s) | Nivel de detalle requerido |
| --- | --- | --- | --- | --- | --- |
| **R1** | **¿Los artistas ganadores del Grammy tienen mayor popularidad actual en Spotify que los que no han ganado?** Evalúa si el reconocimiento de la industria se traduce en audiencia. | Spotify: `artists`, `track_id`, `popularity` · Grammy: `artist` (y `nominee` en *Best New Artist*), `year`, `winner` | Spotify + Grammy | **KPI-1:** popularidad promedio de artistas ganadores vs. no ganadores. **KPI-2:** % de artistas de Spotify con al menos un Grammy. | Artista (agregado desde canción) |
| **R2** | **¿Qué géneros de Spotify concentran a los artistas ganadores del Grammy?** Orienta en qué géneros el reconocimiento es más frecuente. | Spotify: `track_genre`, `artists` · Grammy: `artist`, `category` | Spotify + Grammy | **KPI-3:** % de artistas ganadores por género. **KPI-4:** número de premios por género. | Género × artista |
| **R3** | **¿En qué se diferencia el perfil sonoro de las canciones de artistas ganadores frente al resto?** | Spotify: `danceability`, `energy`, `valence`, `acousticness`, `instrumentalness`, `speechiness`, `explicit` · Grammy: `artist` | Spotify + Grammy | **KPI-5:** promedio de cada característica de audio por grupo (ganador / no ganador). | Canción |
| **R4** | **¿Los artistas premiados más recientemente conservan hoy más popularidad que los premiados en décadas anteriores?** Mide la vigencia del reconocimiento. | Spotify: `popularity`, `artists` · Grammy: `artist`, `year` | Spotify + Grammy | **KPI-6:** popularidad promedio por década del último premio del artista. **KPI-7:** número de artistas premiados por década. | Artista × década del premio |

## Por qué es necesario combinar ambas fuentes

```
            SPOTIFY                                  GRAMMY
  "¿cómo suena y qué tan escuchado es?"    "¿fue premiado y cuándo?"
   popularity, género, audio features       year, category, artist
                 \                                   /
                  \______ nombre de artista _______/
                         (normalizado)
                              |
          R1 popularidad   R2 géneros   R3 perfil sonoro   R4 vigencia
```

Cada KPI necesita **una medida de Spotify** cruzada con **la condición de
ganador (y su año) de Grammy**. Sin el cruce, ningún KPI se puede calcular.

## Trazabilidad hacia el resto del diseño

| Requerimiento | Riesgos de calidad que lo amenazan (IDs en `docs/quality_rules.md`) |
| --- | --- |
| R1 | P02 fila sin artista · P03 duplicados exactos · P05 popularidad inconsistente · P09 lote 2019 · P10 `artist` vacío · P11 artistas múltiples · P15 cobertura del cruce · P16 coincidencias ambiguas |
| R2 | P04 canción en varios géneros · P10 · P11 · P12 categorías con variantes de mayúsculas |
| R3 | P03 · P06 valores imposibles · P07 rango de características de audio |
| R4 | P05 · P08 popularidad 0 · P09 · P14 contrato de `year` |

## Limitaciones declaradas

- **Supuesto:** dos artistas con el mismo nombre normalizado se consideran el mismo artista.
- La cobertura del cruce es limitada: Spotify es una muestra (114,000 filas, 1,000 por cada
  uno de sus 114 géneros), no el catálogo completo, y Grammy incluye categorías clásicas,
  técnicas o de *liner notes* cuyos ganadores rara vez aparecen en esa muestra.
- El análisis de ganadores cubre 1958–2018. 2019 queda fuera por la evidencia de calidad.
- La popularidad de Spotify es una foto del momento de extracción del dataset.
