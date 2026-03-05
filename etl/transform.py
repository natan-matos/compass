"""
transform.py
Responsabilidade: sanitizar, tipar e validar os DataFrames brutos.
Recebe DataFrames "sujos" e devolve DataFrames prontos para carga.

Premissas documentadas:
  - num_cpf_cnpj está anonimizado (hash); tratado como VARCHAR.
  - vlr_transacao usa vírgula como separador decimal (padrão BR).
  - Modalidades inconsistentes ('Crédito', 'CREDITO', 'Débito', 'DEBITO')
    são normalizadas para 'CREDITO' / 'DEBITO'.
  - Modalidade '0' é inválida → convertida para NULL.
  - Duplicatas em db_pessoa_associado: mantém o registro mais recente
    por num_cpf_cnpj.
  - Campos de texto contêm '?' onde havia caracteres acentuados —
    a informação foi perdida na origem do arquivo, antes da extração.
    Os valores são carregados como estão. Ver README § "Caracteres corrompidos".
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)

_MODALIDADE_MAP = {
    "CRÉDITO": "CREDITO",
    "CREDITO": "CREDITO",
    "DÉBITO":  "DEBITO",
    "DEBITO":  "DEBITO",
}


def transform_pessoa(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[transform] db_pessoa_associado...")
    df = df.copy()

    df["num_cpf_cnpj"]       = df["num_cpf_cnpj"].str.strip()
    df["des_nome_associado"] = df["des_nome_associado"].str.strip().str.upper()
    df["des_faixa_renda"]    = df["des_faixa_renda"].str.strip().str.upper()

    df["dat_associacao"] = pd.to_datetime(
        df["dat_associacao"], format="%d/%m/%Y", errors="coerce"
    )
    df["cod_faixa_renda"] = pd.to_numeric(
        df["cod_faixa_renda"], errors="coerce"
    ).astype("Int64")

    before = len(df)
    df = (
        df.sort_values("dat_associacao", ascending=False)
          .drop_duplicates(subset=["num_cpf_cnpj"], keep="first")
    )
    if dropped := before - len(df):
        logger.warning(f"[transform] pessoa: {dropped} duplicata(s) removida(s)")

    _log_nulls(df, "pessoa")
    logger.info(f"[transform] pessoa: {len(df)} registros prontos.")
    return df


def transform_agencia(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[transform] db_entidade_agencia...")
    df = df.copy()

    df["des_nome_cooperativa"] = df["des_nome_cooperativa"].str.strip().str.upper()
    df["des_nome_agencia"]     = df["des_nome_agencia"].str.strip().str.upper()
    df["cod_cooperativa"]      = df["cod_cooperativa"].astype(str).str.zfill(4)
    df["cod_agencia"]          = pd.to_numeric(df["cod_agencia"], errors="coerce").astype("Int64")

    before = len(df)
    df = df.drop_duplicates(subset=["cod_cooperativa", "cod_agencia"])
    if dropped := before - len(df):
        logger.warning(f"[transform] agencia: {dropped} duplicata(s) removida(s)")

    _log_nulls(df, "agencia")
    logger.info(f"[transform] agencia: {len(df)} registros prontos.")
    return df


def transform_transacoes(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("[transform] db_cartoes_transacoes...")
    df = df.copy()

    df["num_cpf_cnpj"]   = df["num_cpf_cnpj"].str.strip()
    df["nom_modalidade"] = df["nom_modalidade"].str.strip().str.upper().map(_MODALIDADE_MAP)

    if invalidos := df["nom_modalidade"].isna().sum():
        logger.warning(f"[transform] transacoes: {invalidos} registro(s) com modalidade inválida → NULL")

    df["dat_transacao"] = pd.to_datetime(
        df["dat_transacao"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
    )
    df["vlr_transacao"] = pd.to_numeric(
        df["vlr_transacao"].astype(str)
            .str.replace(r"\.", "", regex=True)
            .str.replace(",", ".", regex=False),
        errors="coerce",
    )

    df["cod_cooperativa"] = df["cod_cooperativa"].astype(str).str.zfill(4)
    df["cod_agencia"]     = pd.to_numeric(df["cod_agencia"],  errors="coerce").astype("Int64")
    df["cod_conta"]       = pd.to_numeric(df["cod_conta"],    errors="coerce").astype("Int64")
    df["num_plastico"]    = pd.to_numeric(df["num_plastico"], errors="coerce").astype("Int64")

    df["nom_cidade_estabelecimento"] = df["nom_cidade_estabelecimento"].str.strip().str.upper()

    _log_nulls(df, "transacoes")
    logger.info(f"[transform] transacoes: {len(df)} registros prontos.")
    return df


def transform_all(raw: dict) -> dict:
    logger.info("[transform] Iniciando transformações...")

    pessoa     = transform_pessoa(raw["pessoa"])
    agencia    = transform_agencia(raw["agencia"])
    transacoes = transform_transacoes(raw["transacoes"])

    faixa_renda = (
        pessoa[["cod_faixa_renda", "des_faixa_renda"]]
        .dropna(subset=["cod_faixa_renda"])
        .drop_duplicates()
        .sort_values("cod_faixa_renda")
    )

    logger.info("[transform] Transformações concluídas.")
    return {"pessoa": pessoa, "agencia": agencia, "transacoes": transacoes, "faixa_renda": faixa_renda}


def _log_nulls(df: pd.DataFrame, nome: str):
    if nulls := df.isnull().sum()[lambda s: s > 0].to_dict():
        logger.warning(f"[transform] [{nome}] NULLs após transform: {nulls}")