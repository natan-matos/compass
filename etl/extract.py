"""
extract.py
Responsabilidade: ler os arquivos CSV brutos e devolvê-los como DataFrames.
Nenhuma transformação ocorre aqui.
"""

import logging
import os
import pandas as pd

logger = logging.getLogger(__name__)


def extract_csv(filepath: str) -> pd.DataFrame:
    """Lê um CSV inferindo separador, com encoding latin-1."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

    df = pd.read_csv(filepath, sep=None, engine="python", encoding="latin-1")
    logger.info(f"[extract] {os.path.basename(filepath)} → {len(df)} linhas, {len(df.columns)} colunas")
    return df


def extract_all(file_pessoa: str, file_agencia: str, file_transacoes: str) -> dict:
    """
    Extrai os 3 arquivos CSV e retorna um dicionário com os DataFrames brutos.

    Returns:
        {
            "pessoa":     DataFrame,
            "agencia":    DataFrame,
            "transacoes": DataFrame,
        }
    """
    logger.info("[extract] Iniciando extração dos arquivos CSV...")

    raw = {
        "pessoa":     extract_csv(file_pessoa),
        "agencia":    extract_csv(file_agencia),
        "transacoes": extract_csv(file_transacoes),
    }

    logger.info("[extract] Extração concluída.")
    return raw