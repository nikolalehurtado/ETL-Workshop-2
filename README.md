# ETL Workshop-2 — Pipeline batch confiable: Spotify + Premios Grammy

**Curso:** ETL (G01) — Ingeniería de Datos e Inteligencia Artificial, Universidad Autónoma de Occidente
**Autora:** _(nombre completo)_

Pipeline batch que integra datos de Spotify (CSV) y de los Premios Grammy (base de datos relacional), validados con Great Expectations, orquestados con Apache Airflow 3.1.8 y cargados en un Data Warehouse dimensional en PostgreSQL.

---

## Tabla de contenido

1. [Problema y objetivo analítico](#1-problema-y-objetivo-analítico)
2. [Requerimientos analíticos](#2-requerimientos-analíticos)
3. [Fuentes de datos](#3-fuentes-de-datos)
4. [Arquitectura del pipeline](#4-arquitectura-del-pipeline)
5. [Hallazgos del perfilado](#5-hallazgos-del-perfilado)
6. [Riesgos y reglas de calidad](#6-riesgos-y-reglas-de-calidad)
7. [Diseño de validación con Great Expectations](#7-diseño-de-validación-con-great-expectations)
8. [Estrategia de transformación e integración](#8-estrategia-de-transformación-e-integración)
9. [Modelo dimensional](#9-modelo-dimensional)
10. [Diseño del DAG de Airflow](#10-diseño-del-dag-de-airflow)
11. [Política de fallas y reintentos](#11-política-de-fallas-y-reintentos)
12. [Evidencia de ejecución exitosa](#12-evidencia-de-ejecución-exitosa)
13. [Evidencia de falla controlada](#13-evidencia-de-falla-controlada)
14. [Estrategia de repetibilidad](#14-estrategia-de-repetibilidad)
15. [Dashboard y salidas analíticas](#15-dashboard-y-salidas-analíticas)
16. [Instalación y ejecución](#16-instalación-y-ejecución)
17. [Supuestos, decisiones y limitaciones](#17-supuestos-decisiones-y-limitaciones)
18. [Registro de evidencias](#18-registro-de-evidencias)

---

## 1. Problema y objetivo analítico

🚧 *Pendiente — se define junto con los requerimientos analíticos.*

## 2. Requerimientos analíticos

🚧 *Pendiente.*

## 3. Fuentes de datos

| Fuente | Forma entregada | Fuente de extracción del pipeline | Ubicación en el proyecto |
| --- | --- | --- | --- |
| Spotify | CSV | Archivo CSV | `data/raw/spotify_dataset.csv` |
| Premios Grammy | CSV | Base de datos PostgreSQL (fuente operativa) | Original: `data/raw/the_grammy_awards.csv` |

- Los archivos originales se conservan sin modificaciones en `data/raw/`.
- El CSV de Grammy se carga en una base de datos de origen como **preparación de la fuente**. Esto **no** es la etapa final de Carga (Load) del ETL. El pipeline extrae los datos de Grammy desde esa base de datos, no desde el CSV.

🚧 *Pendiente: definición de la tabla fuente y conciliación de filas.*

## 4. Arquitectura del pipeline

Flujo planeado (sección 5 del enunciado):

```
SPOTIFY CSV                              GRAMMY DATABASE (PostgreSQL)
     │                                              │
extract_spotify                              extract_grammys
     │                                              │
validate_spotify_raw                       validate_grammys_raw
     └────────────────────┬─────────────────────────┘
                          │
               transform_and_integrate
                          │
                  validate_prepared
                          │
                       load_dw
                          │
              DATA WAREHOUSE (PostgreSQL)
                          │
                   KPIs / DASHBOARD
```

🚧 *Pendiente: diagrama definitivo y descripción de cada tarea.*

## 5. Hallazgos del perfilado

🚧 *Pendiente.*

## 6. Riesgos y reglas de calidad

🚧 *Pendiente.*

## 7. Diseño de validación con Great Expectations

🚧 *Pendiente.*

## 8. Estrategia de transformación e integración

🚧 *Pendiente.*

## 9. Modelo dimensional

🚧 *Pendiente.*

## 10. Diseño del DAG de Airflow

🚧 *Pendiente.*

## 11. Política de fallas y reintentos

🚧 *Pendiente.*

## 12. Evidencia de ejecución exitosa

🚧 *Pendiente.*

## 13. Evidencia de falla controlada

🚧 *Pendiente.*

## 14. Estrategia de repetibilidad

🚧 *Pendiente.*

## 15. Dashboard y salidas analíticas

🚧 *Pendiente.*

## 16. Instalación y ejecución

### Requisitos

| Herramienta | Versión usada |
| --- | --- |
| Docker Desktop (backend WSL2) | Docker 29.4.0 · Compose v5.1.2 |
| Git | — |
| Memoria RAM | Mínimo 8 GB recomendado (probado con 7.2 GB, ver sección 17) |

### Pasos

**1. Clonar el repositorio** (en una ruta sin espacios y fuera de carpetas sincronizadas como OneDrive):

```powershell
git clone https://github.com/nikolalehurtado/ETL-Workshop-2.git
cd ETL-Workshop-2
```

**2. Crear el archivo de secretos** a partir de la plantilla y reemplazar los valores:

```powershell
Copy-Item .env.example .env
```

> Las contraseñas no deben contener los caracteres `@`, `:` ni `/`, porque forman parte de la cadena de conexión a PostgreSQL.

**3. Inicializar Airflow** (crea la base de metadatos, el usuario administrador y `config/airflow.cfg`):

```powershell
docker compose up airflow-init
```

Debe terminar con `airflow-init-1 exited with code 0`.

**4. Encender los servicios:**

```powershell
docker compose up -d
docker compose ps
```

Los servicios `postgres`, `airflow-apiserver`, `airflow-scheduler` y `airflow-dag-processor` deben aparecer en estado `(healthy)`.

**5. Entrar a la interfaz web:** http://localhost:8080, con el usuario y la contraseña definidos en `.env` (`_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`).

**6. Verificar la versión de Airflow:**

```powershell
docker compose exec airflow-scheduler airflow version
```

### Apagar el entorno

```powershell
docker compose down      # apaga los contenedores y conserva los datos
docker compose down -v   # apaga y BORRA la base de datos (reinicio desde cero)
```

### Estructura del repositorio

```
ETL-Workshop-2/
├── dags/                  # DAG de Airflow (solo orquestación)
├── src/                   # Lógica reutilizable: extract, transform, validation, load
├── gx/                    # Configuración y resultados de Great Expectations
├── notebooks/             # Perfilado de datos
├── sql/                   # Preparación de la fuente Grammy y esquema del DW
├── data/raw/              # Archivos fuente originales (sin modificar)
├── docs/                  # Arquitectura, reglas de calidad y evidencias
├── config/  logs/  plugins/   # Carpetas montadas por Airflow
├── docker-compose.yaml
├── .env.example           # Plantilla de variables (sin credenciales reales)
└── README.md
```

## 17. Supuestos, decisiones y limitaciones

### Decisiones de entorno

El `docker-compose.yaml` parte del archivo oficial de Apache Airflow 3.1.8 y fue adaptado para desarrollo local con recursos limitados:

| Decisión | Original oficial | Adaptación | Justificación |
| --- | --- | --- | --- |
| Executor | CeleryExecutor | **LocalExecutor** | Celery distribuye tareas entre varias máquinas. Con un solo equipo, LocalExecutor ejecuta las tareas dentro del scheduler y elimina contenedores innecesarios. |
| Redis, worker y Flower | Incluidos | **Eliminados** | Solo los usa CeleryExecutor. |
| Triggerer | Incluido | **Eliminado** | Solo ejecuta tareas diferibles (deferrable). El DAG es batch con tareas TaskFlow normales. Por eso la interfaz muestra "Triggerer" en rojo: es el resultado esperado de esta decisión, no una falla. |
| DAGs de ejemplo | Activados | **Desactivados** | Reducen el consumo de memoria y el ruido en la interfaz. |
| Credenciales | Escritas en el archivo | **Leídas desde `.env`** | Los secretos no se suben al repositorio (sección 6.2 del enunciado). Si falta una variable, Docker se detiene con un mensaje explícito. |
| Healthcheck del dag-processor | timeout 10 s | **timeout 30 s, intervalo 60 s** | Con 7.2 GB de RAM, el comando de chequeo tarda entre 8 y 12 s solo en cargar Airflow y generaba falsos `unhealthy`. El diagnóstico (`docker inspect`) mostró `Found one alive job` dentro de chequeos que excedían el tiempo, lo que confirma que el servicio funcionaba. |
| Paralelismo y workers del API | 32 y 4 | **4 y 1** | Ahorro de memoria. |
| Carpetas montadas | dags, logs, config, plugins | **+ data, src, gx** | El DAG necesita leer los archivos fuente, importar la lógica de `src/` y usar Great Expectations. |
| Puerto de PostgreSQL | No expuesto | **5432** | Permite conectar la herramienta de BI y los notebooks desde el equipo anfitrión. |

### Limitaciones conocidas

- El entorno se probó en un equipo con 7.2 GB de RAM, por debajo de los 8 GB recomendados. El procesamiento de datos podría requerir cerrar otras aplicaciones mientras corre el pipeline.

## 18. Registro de evidencias

| ID | Ejecución / Tarea | Artefacto o ruta | Qué demuestra | Política / Regla relacionada |
| --- | --- | --- | --- | --- |
| E01 | Arranque del entorno | `docs/evidence/E01_airflow_ui_salud.png` | Airflow 3.1.8 arranca con Docker Compose y sus componentes (base de metadatos, scheduler, dag-processor) reportan estado saludable | Requisito 6.2 — entorno reproducible |