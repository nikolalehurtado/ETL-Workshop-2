"""Lectura uniforme de los archivos del pipeline."""

import pandas as pd


def read_table(path):
    """Lee un CSV tratando como nulo SOLO la celda vacia.

    Por defecto pandas convierte textos como "N/A", "NA", "None" o "null" en nulos.
    Aqui esos textos pueden ser datos reales (nombres de artistas o canciones), y
    decidir si son nulos es una regla de transformacion explicita (T06), no un
    efecto silencioso de la lectura.
    """
    return pd.read_csv(path, keep_default_na=False, na_values=[""])
