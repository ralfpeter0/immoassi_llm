import pandas as pd
import os

BASE_PATH = "dataout"


def load_konto_mapping():
    path = os.path.join(BASE_PATH, "konto_mapping.csv")
    return pd.read_csv(path)


def get_mietkonten():
    """
    Harte Definition für dein System:
    Nur echte Miete (keine Nebenkosten!)
    """
    return [8105, 8400]