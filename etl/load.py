"""
load.py
Responsabilidade: criar o schema no banco e carregar os DataFrames transformados.
Idempotente: pode ser executado múltiplas vezes sem duplicar dados.
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────────────────────────────────────

_DDL = """
-- Lookup: faixa de renda
CREATE TABLE IF NOT EXISTS dim_faixa_renda (
    cod_faixa_renda  SMALLINT     PRIMARY KEY,
    des_faixa_renda  VARCHAR(50)  NOT NULL
);

-- Cooperativas e agências
CREATE TABLE IF NOT EXISTS stg_agencias (
    cod_cooperativa      CHAR(4)       NOT NULL,
    des_nome_cooperativa VARCHAR(100)  NOT NULL,
    cod_agencia          SMALLINT      NOT NULL,
    des_nome_agencia     VARCHAR(100)  NOT NULL,
    PRIMARY KEY (cod_cooperativa, cod_agencia)
);

-- Associados
CREATE TABLE IF NOT EXISTS stg_associados (
    num_cpf_cnpj        VARCHAR(64)   PRIMARY KEY,
    des_nome_associado  VARCHAR(100)  NOT NULL,
    dat_associacao      DATE,
    cod_faixa_renda     SMALLINT      REFERENCES dim_faixa_renda(cod_faixa_renda)
);

-- Transações (staging)
CREATE TABLE IF NOT EXISTS stg_transacoes (
    id_transacao                BIGSERIAL     PRIMARY KEY,
    num_cpf_cnpj                VARCHAR(64)   NOT NULL
                                    REFERENCES stg_associados(num_cpf_cnpj),
    cod_cooperativa             CHAR(4)       NOT NULL,
    cod_agencia                 SMALLINT      NOT NULL,
    cod_conta                   INTEGER       NOT NULL,
    num_plastico                BIGINT        NOT NULL,
    dat_transacao               TIMESTAMP     NOT NULL,
    vlr_transacao               NUMERIC(15,2),
    nom_modalidade              VARCHAR(10)
                                    CHECK (nom_modalidade IN ('CREDITO','DEBITO')),
    nom_cidade_estabelecimento  VARCHAR(100),
    FOREIGN KEY (cod_cooperativa, cod_agencia)
        REFERENCES stg_agencias(cod_cooperativa, cod_agencia)
);

CREATE INDEX IF NOT EXISTS idx_trans_cpf        ON stg_transacoes(num_cpf_cnpj);
CREATE INDEX IF NOT EXISTS idx_trans_dat        ON stg_transacoes(dat_transacao);
CREATE INDEX IF NOT EXISTS idx_trans_modalidade ON stg_transacoes(nom_modalidade);
CREATE INDEX IF NOT EXISTS idx_trans_coop_ag    ON stg_transacoes(cod_cooperativa, cod_agencia);

-- ── Camada Flat ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS flat_associados (
    num_cpf_cnpj                VARCHAR(64)   PRIMARY KEY,
    des_nome_associado          VARCHAR(100),
    dat_associacao              DATE,
    cod_faixa_renda             SMALLINT,
    des_faixa_renda             VARCHAR(50),
    cod_cooperativa_principal   CHAR(4),
    cod_agencia_principal       SMALLINT,
    des_agencia_principal       VARCHAR(100),
    flg_associado_frequente     BOOLEAN,
    flg_ativo_credito           BOOLEAN,
    flg_ativo_debito            BOOLEAN,
    qtd_transacoes_3m           INTEGER,
    vlr_total_3m                NUMERIC(15,2),
    vlr_credito_3m              NUMERIC(15,2),
    vlr_debito_3m               NUMERIC(15,2),
    dat_ultima_transacao        TIMESTAMP,
    dat_geracao_flat            TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_flat_frequente ON flat_associados(flg_associado_frequente);
CREATE INDEX IF NOT EXISTS idx_flat_coop      ON flat_associados(cod_cooperativa_principal);

-- ── Camada Dimensional ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_tempo (
    sk_tempo        SERIAL       PRIMARY KEY,
    dat_completa    DATE         NOT NULL UNIQUE,
    num_ano         SMALLINT     NOT NULL,
    num_semestre    SMALLINT     NOT NULL,
    num_trimestre   SMALLINT     NOT NULL,
    num_mes         SMALLINT     NOT NULL,
    des_mes         VARCHAR(20)  NOT NULL,
    num_semana_ano  SMALLINT     NOT NULL,
    num_dia_mes     SMALLINT     NOT NULL,
    num_dia_semana  SMALLINT     NOT NULL,
    des_dia_semana  VARCHAR(15)  NOT NULL,
    flg_fim_semana  BOOLEAN      NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_associado (
    sk_associado        SERIAL       PRIMARY KEY,
    num_cpf_cnpj        VARCHAR(64)  NOT NULL UNIQUE,
    des_nome_associado  VARCHAR(100) NOT NULL,
    dat_associacao      DATE,
    cod_faixa_renda     SMALLINT,
    des_faixa_renda     VARCHAR(50),
    dat_carga           TIMESTAMP    DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_agencia (
    sk_agencia           SERIAL       PRIMARY KEY,
    cod_cooperativa      CHAR(4)      NOT NULL,
    des_nome_cooperativa VARCHAR(100) NOT NULL,
    cod_agencia          SMALLINT     NOT NULL,
    des_nome_agencia     VARCHAR(100) NOT NULL,
    dat_carga            TIMESTAMP    DEFAULT NOW(),
    UNIQUE (cod_cooperativa, cod_agencia)
);

CREATE TABLE IF NOT EXISTS dim_cartao (
    sk_cartao    SERIAL    PRIMARY KEY,
    num_plastico BIGINT    NOT NULL UNIQUE,
    cod_conta    INTEGER   NOT NULL,
    dat_carga    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_modalidade (
    sk_modalidade  SERIAL      PRIMARY KEY,
    nom_modalidade VARCHAR(10) NOT NULL UNIQUE
);

INSERT INTO dim_modalidade (nom_modalidade)
VALUES ('CREDITO'), ('DEBITO')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_localidade (
    sk_localidade              SERIAL       PRIMARY KEY,
    nom_cidade_estabelecimento VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fct_transacoes (
    sk_transacao       BIGSERIAL  PRIMARY KEY,
    sk_tempo           INTEGER    NOT NULL REFERENCES dim_tempo(sk_tempo),
    sk_associado       INTEGER    NOT NULL REFERENCES dim_associado(sk_associado),
    sk_agencia         INTEGER    NOT NULL REFERENCES dim_agencia(sk_agencia),
    sk_cartao          INTEGER    NOT NULL REFERENCES dim_cartao(sk_cartao),
    sk_modalidade      INTEGER    NOT NULL REFERENCES dim_modalidade(sk_modalidade),
    sk_localidade      INTEGER             REFERENCES dim_localidade(sk_localidade),
    dat_hora_transacao TIMESTAMP  NOT NULL,
    vlr_transacao      NUMERIC(15,2),
    dat_carga          TIMESTAMP  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fct_tempo      ON fct_transacoes(sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fct_associado  ON fct_transacoes(sk_associado);
CREATE INDEX IF NOT EXISTS idx_fct_agencia    ON fct_transacoes(sk_agencia);
CREATE INDEX IF NOT EXISTS idx_fct_modalidade ON fct_transacoes(sk_modalidade);
CREATE INDEX IF NOT EXISTS idx_fct_dat_hora   ON fct_transacoes(dat_hora_transacao);
"""

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_engine(db_url: str) -> Engine:
    engine = create_engine(db_url, echo=False)
    logger.info("[load] Conexão com o banco estabelecida.")
    return engine


def create_schema(engine: Engine) -> None:
    logger.info("[load] Criando/verificando schema...")
    with engine.connect() as conn:
        conn.execute(text(_DDL))
        conn.commit()
    logger.info("[load] Schema OK.")


def _upsert(df: pd.DataFrame, table: str, engine: Engine,
            conflict_cols: list[str], update_cols: list[str] | None = None) -> None:
    """
    Insere registros ignorando conflitos de PK (INSERT ... ON CONFLICT DO NOTHING).
    Se update_cols for fornecido, faz upsert (ON CONFLICT DO UPDATE).
    """
    if df.empty:
        logger.warning(f"[load] {table}: DataFrame vazio, pulando.")
        return

    rows = df.to_dict(orient="records")
    cols = list(df.columns)
    col_list   = ", ".join(cols)
    placeholder = ", ".join(f":{c}" for c in cols)
    conflict    = ", ".join(conflict_cols)

    if update_cols:
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        stmt = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholder}) "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
        )
    else:
        stmt = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholder}) "
            f"ON CONFLICT ({conflict}) DO NOTHING"
        )

    with engine.connect() as conn:
        conn.execute(text(stmt), rows)
        conn.commit()

    logger.info(f"[load] {table}: {len(rows)} registro(s) processado(s).")


# ─────────────────────────────────────────────────────────────────────────────
# Carga principal
# ─────────────────────────────────────────────────────────────────────────────

def load_faixa_renda(df: pd.DataFrame, engine: Engine) -> None:
    _upsert(
        df.astype({"cod_faixa_renda": int}),
        "dim_faixa_renda",
        engine,
        conflict_cols=["cod_faixa_renda"],
        update_cols=["des_faixa_renda"],
    )


def load_agencia(df: pd.DataFrame, engine: Engine) -> None:
    subset = df[["cod_cooperativa", "des_nome_cooperativa", "cod_agencia", "des_nome_agencia"]].copy()
    subset["cod_agencia"] = subset["cod_agencia"].astype(int)
    _upsert(
        subset,
        "stg_agencias",
        engine,
        conflict_cols=["cod_cooperativa", "cod_agencia"],
        update_cols=["des_nome_cooperativa", "des_nome_agencia"],
    )


def load_pessoa(df: pd.DataFrame, engine: Engine) -> None:
    subset = df[["num_cpf_cnpj", "des_nome_associado", "dat_associacao", "cod_faixa_renda"]].copy()
    subset["cod_faixa_renda"] = subset["cod_faixa_renda"].astype("object").where(
        subset["cod_faixa_renda"].notna(), other=None
    )
    _upsert(
        subset,
        "stg_associados",
        engine,
        conflict_cols=["num_cpf_cnpj"],
        update_cols=["des_nome_associado", "dat_associacao", "cod_faixa_renda"],
    )


def load_transacoes(df: pd.DataFrame, engine: Engine) -> None:
    subset = df[[
        "num_cpf_cnpj", "cod_cooperativa", "cod_agencia", "cod_conta",
        "num_plastico", "dat_transacao", "vlr_transacao",
        "nom_modalidade", "nom_cidade_estabelecimento",
    ]].copy()

    # Descarta registros com chaves obrigatórias nulas
    before = len(subset)
    subset = subset.dropna(subset=["num_cpf_cnpj", "cod_cooperativa", "cod_agencia", "dat_transacao"])
    dropped = before - len(subset)
    if dropped:
        logger.warning(f"[load] transacoes: {dropped} registro(s) descartado(s) por chave nula.")

    subset["cod_agencia"]  = subset["cod_agencia"].astype(int)
    subset["cod_conta"]    = subset["cod_conta"].astype(int)
    subset["num_plastico"] = subset["num_plastico"].astype(int)

    # Transações não têm PK natural — carrega via to_sql (sem upsert)
    subset.to_sql(
        "stg_transacoes", engine,
        if_exists="append", index=False,
        method="multi", chunksize=500,
    )
    logger.info(f"[load] stg_transacoes: {len(subset)} registro(s) inserido(s).")


def load_all(transformed: dict, engine: Engine) -> None:
    """
    Carrega todos os DataFrames na ordem correta (respeita FK constraints).

    Args:
        transformed: saída de transform.transform_all()
        engine:      SQLAlchemy engine conectada ao Postgres
    """
    logger.info("[load] Iniciando carga no banco de dados...")

    create_schema(engine)

    # Ordem importa: lookup → agência → pessoa → transações
    load_faixa_renda(transformed["faixa_renda"], engine)
    load_agencia(transformed["agencia"],         engine)
    load_pessoa(transformed["pessoa"],           engine)
    load_transacoes(transformed["transacoes"],   engine)

    logger.info("[load] Carga concluída.")