-- ============================================================
-- DESAFIO 2 - Tabela Flat: Indicadores de Atividade do Associado
-- ============================================================
-- Premissas:
--   • "Últimos 3 meses" = os 3 meses-calendário completos anteriores
--     à data de referência. Ex: se hoje é qualquer dia de Jan/2026,
--     os 3 últimos meses são: Out, Nov e Dez de 2025.
--   • Associado Frequente: transacionou pelo menos 1x em CADA um
--     dos 3 meses, em qualquer modalidade (inclui registros com
--     modalidade NULL, pois a transação ocorreu).
--   • Associado Ativo no Crédito: ao menos 1 transação CREDITO
--     nos últimos 3 meses (não exige os 3 meses consecutivos).
--   • Associado Ativo no Débito: idem para DEBITO.
--   • A tabela flat une TODOS os associados cadastrados, mesmo os
--     sem transação (flags ficam FALSE).
-- ============================================================


-- ── 1. CTE: data de referência e janela dos últimos 3 meses ─────────────────
WITH params AS (
    SELECT
        -- Usa o último dia do mês anterior como fim da janela
        DATE_TRUNC('month', CURRENT_DATE)                           AS ref_fim,
        DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 months'    AS ref_inicio
        -- Exemplo em Dez/2025: ref_inicio = 2025-09-01, ref_fim = 2025-12-01
        -- Cobre Set, Out e Nov de 2025
),

-- ── 2. Transações dentro da janela ──────────────────────────────────────────
transacoes_janela AS (
    SELECT
        t.num_cpf_cnpj,
        t.nom_modalidade,
        DATE_TRUNC('month', t.dat_transacao) AS mes_transacao
    FROM db_cartoes_transacoes t
    CROSS JOIN params p
    WHERE t.dat_transacao >= p.ref_inicio
      AND t.dat_transacao <  p.ref_fim
),

-- ── 3. Meses distintos em que o associado transacionou ──────────────────────
meses_por_associado AS (
    SELECT
        num_cpf_cnpj,
        COUNT(DISTINCT mes_transacao) AS qtd_meses_com_transacao
    FROM transacoes_janela
    GROUP BY num_cpf_cnpj
),

-- ── 4. Flags de modalidade ───────────────────────────────────────────────────
flags_modalidade AS (
    SELECT
        num_cpf_cnpj,
        MAX(CASE WHEN nom_modalidade = 'CREDITO' THEN 1 ELSE 0 END) AS flag_credito,
        MAX(CASE WHEN nom_modalidade = 'DEBITO'  THEN 1 ELSE 0 END) AS flag_debito
    FROM transacoes_janela
    GROUP BY num_cpf_cnpj
),

-- ── 5. Resumo financeiro por associado (últimos 3 meses) ─────────────────────
resumo_financeiro AS (
    SELECT
        num_cpf_cnpj,
        COUNT(*)                                         AS qtd_transacoes_3m,
        SUM(vlr_transacao)                               AS vlr_total_3m,
        SUM(CASE WHEN nom_modalidade = 'CREDITO'
                 THEN vlr_transacao ELSE 0 END)          AS vlr_credito_3m,
        SUM(CASE WHEN nom_modalidade = 'DEBITO'
                 THEN vlr_transacao ELSE 0 END)          AS vlr_debito_3m,
        MAX(dat_transacao)                               AS dat_ultima_transacao
    FROM transacoes_janela  -- já filtrada pela janela
    -- (vlr_transacao vem da tabela original via JOIN abaixo)
    -- Nota: precisamos do vlr da tabela base; refazemos o join
    GROUP BY num_cpf_cnpj
),

-- Refaz o resumo financeiro usando a tabela original para ter vlr_transacao
resumo_financeiro_v2 AS (
    SELECT
        t.num_cpf_cnpj,
        COUNT(*)                                                    AS qtd_transacoes_3m,
        SUM(t.vlr_transacao)                                        AS vlr_total_3m,
        SUM(CASE WHEN t.nom_modalidade = 'CREDITO'
                 THEN t.vlr_transacao ELSE 0 END)                   AS vlr_credito_3m,
        SUM(CASE WHEN t.nom_modalidade = 'DEBITO'
                 THEN t.vlr_transacao ELSE 0 END)                   AS vlr_debito_3m,
        MAX(t.dat_transacao)                                        AS dat_ultima_transacao
    FROM db_cartoes_transacoes t
    CROSS JOIN params p
    WHERE t.dat_transacao >= p.ref_inicio
      AND t.dat_transacao <  p.ref_fim
    GROUP BY t.num_cpf_cnpj
)

-- ============================================================
-- CRIAÇÃO DA TABELA FLAT
-- ============================================================
CREATE TABLE IF NOT EXISTS flat_associado_indicadores AS

SELECT
    -- ── Dados cadastrais do associado ────────────────────────────────────────
    pa.num_cpf_cnpj,
    pa.des_nome_associado,
    pa.dat_associacao,
    pa.cod_faixa_renda,
    fr.des_faixa_renda,

    -- ── Agência principal (a que gerou mais transações no período) ───────────
    -- Premissa: associado pode ter contas em cooperativas distintas;
    -- exibimos a cooperativa/agência de maior volume transacional.
    agencia_principal.cod_cooperativa  AS cod_cooperativa_principal,
    agencia_principal.cod_agencia      AS cod_agencia_principal,
    agencia_principal.des_nome_agencia AS des_agencia_principal,

    -- ── Indicadores de atividade ─────────────────────────────────────────────
    CASE
        WHEN COALESCE(ma.qtd_meses_com_transacao, 0) >= 3
        THEN TRUE ELSE FALSE
    END                                                 AS flg_associado_frequente,

    CASE
        WHEN COALESCE(fm.flag_credito, 0) = 1
        THEN TRUE ELSE FALSE
    END                                                 AS flg_ativo_credito,

    CASE
        WHEN COALESCE(fm.flag_debito, 0) = 1
        THEN TRUE ELSE FALSE
    END                                                 AS flg_ativo_debito,

    -- ── Métricas financeiras (últimos 3 meses) ───────────────────────────────
    COALESCE(rf.qtd_transacoes_3m, 0)                  AS qtd_transacoes_3m,
    COALESCE(rf.vlr_total_3m,      0)                  AS vlr_total_3m,
    COALESCE(rf.vlr_credito_3m,    0)                  AS vlr_credito_3m,
    COALESCE(rf.vlr_debito_3m,     0)                  AS vlr_debito_3m,
    rf.dat_ultima_transacao,

    -- ── Metadados da carga ───────────────────────────────────────────────────
    NOW()                                               AS dat_geracao_flat

FROM db_pessoa_associado pa

-- Faixa de renda
LEFT JOIN dim_faixa_renda fr
    ON fr.cod_faixa_renda = pa.cod_faixa_renda

-- Meses com transação
LEFT JOIN meses_por_associado ma
    ON ma.num_cpf_cnpj = pa.num_cpf_cnpj

-- Flags de modalidade
LEFT JOIN flags_modalidade fm
    ON fm.num_cpf_cnpj = pa.num_cpf_cnpj

-- Métricas financeiras
LEFT JOIN resumo_financeiro_v2 rf
    ON rf.num_cpf_cnpj = pa.num_cpf_cnpj

-- Agência principal: subquery com DISTINCT ON (mais transações no período)
LEFT JOIN LATERAL (
    SELECT
        t.cod_cooperativa,
        t.cod_agencia,
        ea.des_nome_agencia,
        COUNT(*) AS qtd
    FROM db_cartoes_transacoes t
    CROSS JOIN params p
    JOIN db_entidade_agencia ea
        ON ea.cod_cooperativa = t.cod_cooperativa
       AND ea.cod_agencia     = t.cod_agencia
    WHERE t.num_cpf_cnpj      = pa.num_cpf_cnpj
      AND t.dat_transacao     >= p.ref_inicio
      AND t.dat_transacao     <  p.ref_fim
    GROUP BY t.cod_cooperativa, t.cod_agencia, ea.des_nome_agencia
    ORDER BY qtd DESC
    LIMIT 1
) agencia_principal ON TRUE
;

-- ── Índices da tabela flat ───────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_flat_cpf          ON flat_associado_indicadores(num_cpf_cnpj);
CREATE INDEX IF NOT EXISTS idx_flat_frequente    ON flat_associado_indicadores(flg_associado_frequente);
CREATE INDEX IF NOT EXISTS idx_flat_coop         ON flat_associado_indicadores(cod_cooperativa_principal);


-- ============================================================
-- QUERY DE VALIDAÇÃO / CONFERÊNCIA
-- ============================================================
SELECT
    COUNT(*)                                            AS total_associados,
    SUM(flg_associado_frequente::int)                   AS frequentes,
    SUM(flg_ativo_credito::int)                         AS ativos_credito,
    SUM(flg_ativo_debito::int)                          AS ativos_debito,
    SUM((flg_ativo_credito AND flg_ativo_debito)::int)  AS ativos_ambos,
    ROUND(AVG(qtd_transacoes_3m),2)                     AS media_transacoes_3m,
    SUM(vlr_total_3m)                                   AS volume_total_3m
FROM flat_associado_indicadores;