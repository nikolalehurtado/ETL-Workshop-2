"""Conexiones a PostgreSQL (fuente Grammy y Data Warehouse)."""

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from src import config


def _check_credentials():
    if not config.DB_USER or not config.DB_PASSWORD:
        raise RuntimeError(
            "Faltan POSTGRES_USER o POSTGRES_PASSWORD en el entorno (.env). "
            "No se puede conectar a la base de datos."
        )


def sqlalchemy_engine(database):
    """Engine de SQLAlchemy para lecturas con pandas."""
    _check_credentials()
    url = URL.create(
        "postgresql+psycopg2",
        username=config.DB_USER,
        password=config.DB_PASSWORD,
        host=config.DB_HOST,
        port=config.DB_PORT,
        database=database,
    )
    return create_engine(url, pool_pre_ping=True)


def psycopg_connection(database, autocommit=False):
    """Conexion psycopg2 directa (necesaria para COPY y transacciones explicitas)."""
    _check_credentials()
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        dbname=database,
        connect_timeout=10,
    )
    conn.autocommit = autocommit
    return conn


def is_transient(exc):
    """¿La falla puede desaparecer en otro intento sin cambiar datos ni codigo?

    Transitorias (se reintentan): base de datos caida o reiniciando, red interrumpida,
    conexion rechazada o cortada  -> psycopg2.OperationalError / InterfaceError.
    Deterministas (NO se reintentan): tabla inexistente, tipo invalido, violacion de
    llave o restriccion, conteo no conciliado -> el mismo lote fallaria igual.
    """
    original = getattr(exc, "orig", None) or exc          # SQLAlchemy envuelve el error real
    return isinstance(original, (psycopg2.OperationalError, psycopg2.InterfaceError))
