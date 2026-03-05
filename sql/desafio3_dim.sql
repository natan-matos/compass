-- ============================================================
-- DESAFIO 3 - Carga da camada dimensional
-- ============================================================
-- Ordem de execução: dimensões primeiro, fato por último.
-- Todas as inserções são idempotentes (ON CONFLICT DO NOTHING/UPDATE).
-- ============================================================


-- ── dim_tempo (ano 2025) ──────────────────────────────────────────────────────
INSERT INTO dim_tempo (
    dat_completa, num_ano, num_semestre, num_trimestre,
    num_mes, des_mes, num_semana_ano, num_dia_mes,
    num_dia_semana, des_dia_semana, flg_fim_semana
)
SELECT
    d::date,
    EXTRACT(YEAR    FROM d)::SMALLINT,
    CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 1 ELSE 2 END,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH   FROM d)::SMALLINT,
    TO_CHAR(d, 'TMMonth'),
    EXTRACT(WEEK    FROM d)::SMALLINT,
    EXTRACT(DAY     FROM d)::SMALLINT,
    EXTRACT(ISODOW  FROM d)::SMALLINT,
    TO_CHAR(d, 'TMDay'),
    EXTRACT(ISODOW  FROM d) IN (6, 7)
FROM generate_series('2025-01-01'::date, '2025-12-31'::date, '1 day') d
ON CONFLICT (dat_completa) DO NOTHING;


-- ── dim_associado ─────────────────────────────────────────────────────────────
INSERT INTO dim_associado (num_cpf_cnpj, des_nome_associado, dat_associacao, cod_faixa_renda, des_faixa_renda)
SELECT pa.num_cpf_cnpj, pa.des_nome_associado, pa.dat_associacao, pa.cod_faixa_renda, fr.des_faixa_renda
FROM stg_associados pa
LEFT JOIN dim_faixa_renda fr ON fr.cod_faixa_renda = pa.cod_faixa_renda
ON CONFLICT (num_cpf_cnpj) DO UPDATE SET
    des_nome_associado = EXCLUDED.des_nome_associado,
    dat_associacao     = EXCLUDED.dat_associacao,
    cod_faixa_renda    = EXCLUDED.cod_faixa_renda,
    des_faixa_renda    = EXCLUDED.des_faixa_renda;


-- ── dim_agencia ───────────────────────────────────────────────────────────────
INSERT INTO dim_agencia (cod_cooperativa, des_nome_cooperativa, cod_agencia, des_nome_agencia)
SELECT cod_cooperativa, des_nome_cooperativa, cod_agencia, des_nome_agencia
FROM stg_agencias
ON CONFLICT (cod_cooperativa, cod_agencia) DO UPDATE SET
    des_nome_cooperativa = EXCLUDED.des_nome_cooperativa,
    des_nome_agencia     = EXCLUDED.des_nome_agencia;


-- ── dim_cartao ────────────────────────────────────────────────────────────────
-- Em caso de reemissão, mantém a conta mais recente
INSERT INTO dim_cartao (num_plastico, cod_conta)
SELECT DISTINCT ON (num_plastico) num_plastico, cod_conta
FROM stg_transacoes
ORDER BY num_plastico, dat_transacao DESC
ON CONFLICT (num_plastico) DO UPDATE SET cod_conta = EXCLUDED.cod_conta;


-- ── dim_localidade ────────────────────────────────────────────────────────────
INSERT INTO dim_localidade (nom_cidade_estabelecimento)
SELECT DISTINCT nom_cidade_estabelecimento
FROM stg_transacoes
WHERE nom_cidade_estabelecimento IS NOT NULL
ON CONFLICT (nom_cidade_estabelecimento) DO NOTHING;


-- ── fct_transacoes ────────────────────────────────────────────────────────────
INSERT INTO fct_transacoes (sk_tempo, sk_associado, sk_agencia, sk_cartao, sk_modalidade, sk_localidade, dat_hora_transacao, vlr_transacao)
SELECT
    dt.sk_tempo,
    da.sk_associado,
    dag.sk_agencia,
    dc.sk_cartao,
    dm.sk_modalidade,
    dl.sk_localidade,
    t.dat_transacao,
    t.vlr_transacao
FROM stg_transacoes t
JOIN dim_tempo      dt  ON dt.dat_completa   = t.dat_transacao::date
JOIN dim_associado  da  ON da.num_cpf_cnpj   = t.num_cpf_cnpj
JOIN dim_agencia    dag ON dag.cod_cooperativa = t.cod_cooperativa AND dag.cod_agencia = t.cod_agencia
JOIN dim_cartao     dc  ON dc.num_plastico   = t.num_plastico
JOIN dim_modalidade dm  ON dm.nom_modalidade = t.nom_modalidade
LEFT JOIN dim_localidade dl ON dl.nom_cidade_estabelecimento = t.nom_cidade_estabelecimento
WHERE t.nom_modalidade IS NOT NULL;


-- ============================================================
-- QUERIES ANALÍTICAS
-- ============================================================

-- Volume por cooperativa e mês
SELECT
    ag.cod_cooperativa,
    ag.des_nome_cooperativa,
    t.num_ano,
    t.des_mes,
    COUNT(*)             AS qtd_transacoes,
    SUM(f.vlr_transacao) AS volume_total
FROM fct_transacoes f
JOIN dim_agencia ag ON ag.sk_agencia = f.sk_agencia
JOIN dim_tempo   t  ON t.sk_tempo    = f.sk_tempo
GROUP BY ag.cod_cooperativa, ag.des_nome_cooperativa, t.num_ano, t.num_mes, t.des_mes
ORDER BY ag.cod_cooperativa, t.num_mes;


-- Ticket médio por faixa de renda e modalidade
SELECT
    a.des_faixa_renda,
    m.nom_modalidade,
    COUNT(*)                       AS qtd_transacoes,
    ROUND(AVG(f.vlr_transacao), 2) AS ticket_medio,
    SUM(f.vlr_transacao)           AS volume_total
FROM fct_transacoes f
JOIN dim_associado  a ON a.sk_associado  = f.sk_associado
JOIN dim_modalidade m ON m.sk_modalidade = f.sk_modalidade
GROUP BY a.des_faixa_renda, a.cod_faixa_renda, m.nom_modalidade
ORDER BY a.cod_faixa_renda, m.nom_modalidade;