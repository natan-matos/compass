-- ============================================================
-- DESAFIO 3 - Modelagem Dimensional (Star Schema)
-- Fato x Dimensão a partir dos dados do sistema cooperativo
-- ============================================================
-- Decisões de modelagem:
--   • Granularidade da FATO: 1 linha por transação de cartão
--   • Dimensões SCD Tipo 1 (sobrescreve) — dados não historicizados
--     pois os CSVs de origem não possuem histórico de mudanças
--   • dim_tempo separada para facilitar análises por período
--   • dim_cartao captura o plástico e sua conta vinculada
--   • Chaves surrogate (SERIAL) em todas as dimensões
-- ============================================================


-- ════════════════════════════════════════════════════════════
-- DIMENSÕES
-- ════════════════════════════════════════════════════════════

-- ── dim_tempo ────────────────────────────────────────────────────────────────
-- Pré-populada com todos os dias do período de interesse
CREATE TABLE IF NOT EXISTS dim_tempo (
    sk_tempo        SERIAL       PRIMARY KEY,
    dat_completa    DATE         NOT NULL UNIQUE,
    num_ano         SMALLINT     NOT NULL,
    num_semestre    SMALLINT     NOT NULL,  -- 1 ou 2
    num_trimestre   SMALLINT     NOT NULL,  -- 1 a 4
    num_mes         SMALLINT     NOT NULL,  -- 1 a 12
    des_mes         VARCHAR(20)  NOT NULL,  -- 'Janeiro', 'Fevereiro', ...
    num_semana_ano  SMALLINT     NOT NULL,  -- ISO week 1-53
    num_dia_mes     SMALLINT     NOT NULL,
    num_dia_semana  SMALLINT     NOT NULL,  -- 1=Dom ... 7=Sáb
    des_dia_semana  VARCHAR(15)  NOT NULL,
    flg_fim_semana  BOOLEAN      NOT NULL
);

-- ── dim_associado ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_associado (
    sk_associado        SERIAL       PRIMARY KEY,
    num_cpf_cnpj        VARCHAR(64)  NOT NULL UNIQUE,
    des_nome_associado  VARCHAR(100) NOT NULL,
    dat_associacao      DATE,
    cod_faixa_renda     SMALLINT,
    des_faixa_renda     VARCHAR(50),
    dat_carga           TIMESTAMP    DEFAULT NOW()
);

-- ── dim_agencia ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_agencia (
    sk_agencia           SERIAL      PRIMARY KEY,
    cod_cooperativa      CHAR(4)     NOT NULL,
    des_nome_cooperativa VARCHAR(100) NOT NULL,
    cod_agencia          SMALLINT    NOT NULL,
    des_nome_agencia     VARCHAR(100) NOT NULL,
    dat_carga            TIMESTAMP   DEFAULT NOW(),
    UNIQUE (cod_cooperativa, cod_agencia)
);

-- ── dim_cartao ───────────────────────────────────────────────────────────────
-- Premissa: num_plastico é único; um plástico pode trocar de conta
-- ao longo do tempo (reemissão), mas no dataset atual assumimos
-- que a combinação plástico+conta é estável.
CREATE TABLE IF NOT EXISTS dim_cartao (
    sk_cartao        SERIAL    PRIMARY KEY,
    num_plastico     BIGINT    NOT NULL UNIQUE,
    cod_conta        INTEGER   NOT NULL,
    dat_carga        TIMESTAMP DEFAULT NOW()
);

-- ── dim_modalidade ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_modalidade (
    sk_modalidade   SERIAL      PRIMARY KEY,
    nom_modalidade  VARCHAR(10) NOT NULL UNIQUE  -- 'CREDITO' | 'DEBITO'
);

INSERT INTO dim_modalidade (nom_modalidade)
VALUES ('CREDITO'), ('DEBITO')
ON CONFLICT DO NOTHING;

-- ── dim_localidade ───────────────────────────────────────────────────────────
-- Granularidade: cidade do estabelecimento
CREATE TABLE IF NOT EXISTS dim_localidade (
    sk_localidade               SERIAL       PRIMARY KEY,
    nom_cidade_estabelecimento  VARCHAR(100) NOT NULL UNIQUE
);


-- ════════════════════════════════════════════════════════════
-- TABELA FATO
-- ════════════════════════════════════════════════════════════

-- ── fato_transacao ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fato_transacao (
    sk_transacao    BIGSERIAL    PRIMARY KEY,

    -- Chaves estrangeiras (surrogate keys)
    sk_tempo        INTEGER      NOT NULL REFERENCES dim_tempo(sk_tempo),
    sk_associado    INTEGER      NOT NULL REFERENCES dim_associado(sk_associado),
    sk_agencia      INTEGER      NOT NULL REFERENCES dim_agencia(sk_agencia),
    sk_cartao       INTEGER      NOT NULL REFERENCES dim_cartao(sk_cartao),
    sk_modalidade   INTEGER      NOT NULL REFERENCES dim_modalidade(sk_modalidade),
    sk_localidade   INTEGER               REFERENCES dim_localidade(sk_localidade),

    -- Atributo degenerado (sem dimensão própria — chave natural da transação)
    dat_hora_transacao  TIMESTAMP    NOT NULL,

    -- Medidas (fatos aditivos)
    vlr_transacao       NUMERIC(15,2),

    -- Metadados de carga
    dat_carga           TIMESTAMP    DEFAULT NOW()
);

-- Índices para consultas analíticas frequentes
CREATE INDEX IF NOT EXISTS idx_fato_tempo       ON fato_transacao(sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_associado   ON fato_transacao(sk_associado);
CREATE INDEX IF NOT EXISTS idx_fato_agencia     ON fato_transacao(sk_agencia);
CREATE INDEX IF NOT EXISTS idx_fato_modalidade  ON fato_transacao(sk_modalidade);
CREATE INDEX IF NOT EXISTS idx_fato_dat_hora    ON fato_transacao(dat_hora_transacao);


-- ════════════════════════════════════════════════════════════
-- CARGA DAS DIMENSÕES (a partir das tabelas staging)
-- ════════════════════════════════════════════════════════════

-- ── Popula dim_tempo (ano 2025 completo) ────────────────────────────────────
INSERT INTO dim_tempo (
    dat_completa, num_ano, num_semestre, num_trimestre,
    num_mes, des_mes, num_semana_ano, num_dia_mes,
    num_dia_semana, des_dia_semana, flg_fim_semana
)
SELECT
    d::date                                                         AS dat_completa,
    EXTRACT(YEAR    FROM d)::SMALLINT                              AS num_ano,
    CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END        AS num_semestre,
    EXTRACT(QUARTER FROM d)::SMALLINT                              AS num_trimestre,
    EXTRACT(MONTH   FROM d)::SMALLINT                              AS num_mes,
    TO_CHAR(d, 'TMMonth')                                          AS des_mes,
    EXTRACT(WEEK    FROM d)::SMALLINT                              AS num_semana_ano,
    EXTRACT(DAY     FROM d)::SMALLINT                              AS num_dia_mes,
    EXTRACT(ISODOW  FROM d)::SMALLINT                              AS num_dia_semana,
    TO_CHAR(d, 'TMDay')                                            AS des_dia_semana,
    EXTRACT(ISODOW  FROM d) IN (6, 7)                              AS flg_fim_semana
FROM generate_series('2025-01-01'::date, '2025-12-31'::date, '1 day') d
ON CONFLICT (dat_completa) DO NOTHING;


-- ── Popula dim_associado ────────────────────────────────────────────────────
INSERT INTO dim_associado (
    num_cpf_cnpj, des_nome_associado, dat_associacao,
    cod_faixa_renda, des_faixa_renda
)
SELECT
    pa.num_cpf_cnpj,
    pa.des_nome_associado,
    pa.dat_associacao,
    pa.cod_faixa_renda,
    fr.des_faixa_renda
FROM db_pessoa_associado pa
LEFT JOIN dim_faixa_renda fr ON fr.cod_faixa_renda = pa.cod_faixa_renda
ON CONFLICT (num_cpf_cnpj) DO UPDATE
    SET des_nome_associado = EXCLUDED.des_nome_associado,
        dat_associacao     = EXCLUDED.dat_associacao,
        cod_faixa_renda    = EXCLUDED.cod_faixa_renda,
        des_faixa_renda    = EXCLUDED.des_faixa_renda;


-- ── Popula dim_agencia ──────────────────────────────────────────────────────
INSERT INTO dim_agencia (cod_cooperativa, des_nome_cooperativa, cod_agencia, des_nome_agencia)
SELECT cod_cooperativa, des_nome_cooperativa, cod_agencia, des_nome_agencia
FROM db_entidade_agencia
ON CONFLICT (cod_cooperativa, cod_agencia) DO UPDATE
    SET des_nome_cooperativa = EXCLUDED.des_nome_cooperativa,
        des_nome_agencia     = EXCLUDED.des_nome_agencia;


-- ── Popula dim_cartao ───────────────────────────────────────────────────────
-- Premissa: se um mesmo plástico aparece com contas diferentes, mantém
-- a conta mais recente (última transação).
INSERT INTO dim_cartao (num_plastico, cod_conta)
SELECT DISTINCT ON (num_plastico)
    num_plastico,
    cod_conta
FROM db_cartoes_transacoes
ORDER BY num_plastico, dat_transacao DESC
ON CONFLICT (num_plastico) DO UPDATE
    SET cod_conta = EXCLUDED.cod_conta;


-- ── Popula dim_localidade ───────────────────────────────────────────────────
INSERT INTO dim_localidade (nom_cidade_estabelecimento)
SELECT DISTINCT nom_cidade_estabelecimento
FROM db_cartoes_transacoes
WHERE nom_cidade_estabelecimento IS NOT NULL
ON CONFLICT (nom_cidade_estabelecimento) DO NOTHING;


-- ════════════════════════════════════════════════════════════
-- CARGA DA FATO
-- ════════════════════════════════════════════════════════════

INSERT INTO fato_transacao (
    sk_tempo, sk_associado, sk_agencia, sk_cartao,
    sk_modalidade, sk_localidade, dat_hora_transacao, vlr_transacao
)
SELECT
    dt.sk_tempo,
    da.sk_associado,
    dag.sk_agencia,
    dc.sk_cartao,
    dm.sk_modalidade,
    dl.sk_localidade,
    t.dat_transacao,
    t.vlr_transacao
FROM db_cartoes_transacoes t
JOIN dim_tempo       dt  ON dt.dat_completa  = t.dat_transacao::date
JOIN dim_associado   da  ON da.num_cpf_cnpj  = t.num_cpf_cnpj
JOIN dim_agencia     dag ON dag.cod_cooperativa = t.cod_cooperativa
                        AND dag.cod_agencia     = t.cod_agencia
JOIN dim_cartao      dc  ON dc.num_plastico  = t.num_plastico
JOIN dim_modalidade  dm  ON dm.nom_modalidade = t.nom_modalidade
LEFT JOIN dim_localidade dl ON dl.nom_cidade_estabelecimento = t.nom_cidade_estabelecimento
-- Pula transações com modalidade inválida (NULL)
WHERE t.nom_modalidade IS NOT NULL;


-- ════════════════════════════════════════════════════════════
-- QUERIES ANALÍTICAS DE EXEMPLO
-- ════════════════════════════════════════════════════════════

-- Volume transacionado por cooperativa e mês
SELECT
    ag.cod_cooperativa,
    ag.des_nome_cooperativa,
    t.num_ano,
    t.des_mes,
    COUNT(*)             AS qtd_transacoes,
    SUM(f.vlr_transacao) AS volume_total
FROM fato_transacao f
JOIN dim_agencia dag ON dag.sk_agencia = f.sk_agencia
JOIN dim_agencia ag  ON ag.sk_agencia  = f.sk_agencia
JOIN dim_tempo   t   ON t.sk_tempo     = f.sk_tempo
GROUP BY ag.cod_cooperativa, ag.des_nome_cooperativa, t.num_ano, t.num_mes, t.des_mes
ORDER BY ag.cod_cooperativa, t.num_ano, t.num_mes;

-- Ticket médio por faixa de renda e modalidade
SELECT
    a.des_faixa_renda,
    m.nom_modalidade,
    COUNT(*)                          AS qtd_transacoes,
    ROUND(AVG(f.vlr_transacao), 2)    AS ticket_medio,
    SUM(f.vlr_transacao)              AS volume_total
FROM fato_transacao f
JOIN dim_associado  a ON a.sk_associado  = f.sk_associado
JOIN dim_modalidade m ON m.sk_modalidade = f.sk_modalidade
GROUP BY a.des_faixa_renda, m.nom_modalidade
ORDER BY a.cod_faixa_renda, m.nom_modalidade;