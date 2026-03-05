"""
pipeline.py
Orquestrador: conecta extract → transform → load e configura logging.
Ponto de entrada da aplicação.
"""

import logging
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

from etl.extract   import extract_all
from etl.transform import transform_all
from etl.load      import build_engine, load_all

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run():
    setup_logging()
    logger = logging.getLogger(__name__)

    load_dotenv()

    db_url = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )

    file_pessoa     = os.getenv("FILE_PESSOA",     "data/db_pessoa_associado.csv")
    file_agencia    = os.getenv("FILE_AGENCIA",    "data/db_entidade_agencia.csv")
    file_transacoes = os.getenv("FILE_TRANSACOES", "data/db_cartoes_transacoes.csv")

    logger.info("=" * 55)
    logger.info("  PIPELINE ETL — Sistema Cooperativo")
    logger.info(f"  Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    start = time.time()

    try:
        # 1. Extract
        raw = extract_all(file_pessoa, file_agencia, file_transacoes)

        # 2. Transform
        transformed = transform_all(raw)

        # 3. Load
        engine = build_engine(db_url)
        load_all(transformed, engine)

    except Exception as exc:
        logger.error(f"Pipeline falhou: {exc}", exc_info=True)
        sys.exit(1)

    elapsed = time.time() - start
    logger.info("=" * 55)
    logger.info(f"  Pipeline finalizada em {elapsed:.1f}s")
    logger.info(f"  Associados : {len(transformed['pessoa'])}")
    logger.info(f"  Agências   : {len(transformed['agencia'])}")
    logger.info(f"  Transações : {len(transformed['transacoes'])}")
    logger.info("=" * 55)


if __name__ == "__main__":
    run()