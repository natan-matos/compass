-- ============================================================
-- DESAFIO 2 - Carga da tabela flat_associados
-- ============================================================
-- Premissas:
--   "Últimos 3 meses" = os 3 meses-calendário completos anteriores
--   ao mês atual. Ex: executado em Jan/2026 → cobre Out, Nov e Dez/2025.
--
--   Associado Frequente : transacionou ao menos 1x em CADA um dos 3 meses.
--   Ativo no Crédito    : ao menos 1 transação CREDITO nos 3 meses.
--   Ativo no Débito     : ao menos 1 transação DEBITO nos 3 meses.
--   Associados sem transação entram com flags FALSE e métricas zeradas.
-- ============================================================

INSERT INTO flat_associados
WITH params AS (
    SELECT
        DATE_TRUNC('month', CURRENT_DATE)                        AS ref_fim,
        DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 months' AS ref_inicio
),
transacoes_janela AS (
    SELECT
        t.num_cpf_cnpj,
        t.nom_modalidade,
        t.vlr_transacao,
        t.dat_transacao,
        t.cod_cooperativa,
        t.cod_agencia,
        DATE_TRUNC('month', t.dat_transacao) AS mes_transacao
    FROM stg_transacoes t
    CROSS JOIN params p
    WHERE t.dat_transacao >= p.ref_inicio
      AND t.dat_transacao <  p.ref_fim
),
meses_por_associado AS (
    SELECT num_cpf_cnpj, COUNT(DISTINCT mes_transacao) AS qtd_meses
    FROM transacoes_janela
    GROUP BY num_cpf_cnpj
),
resumo AS (
    SELECT
        num_cpf_cnpj,
        COUNT(*)                                                      AS qtd_transacoes_3m,
        SUM(vlr_transacao)                                            AS vlr_total_3m,
        SUM(CASE WHEN nom_modalidade = 'CREDITO' THEN vlr_transacao ELSE 0 END) AS vlr_credito_3m,
        SUM(CASE WHEN nom_modalidade = 'DEBITO'  THEN vlr_transacao ELSE 0 END) AS vlr_debito_3m,
        MAX(CASE WHEN nom_modalidade = 'CREDITO' THEN 1 ELSE 0 END)  AS flag_credito,
        MAX(CASE WHEN nom_modalidade = 'DEBITO'  THEN 1 ELSE 0 END)  AS flag_debito,
        MAX(dat_transacao)                                            AS dat_ultima_transacao
    FROM transacoes_janela
    GROUP BY num_cpf_cnpj
)
SELECT
    pa.num_cpf_cnpj,
    pa.des_nome_associado,
    pa.dat_associacao,
    pa.cod_faixa_renda,
    fr.des_faixa_renda,
    ag.cod_cooperativa  AS cod_cooperativa_principal,
    ag.cod_agencia      AS cod_agencia_principal,
    ag.des_nome_agencia AS des_agencia_principal,
    COALESCE(ma.qtd_meses, 0) >= 3                AS flg_associado_frequente,
    COALESCE(r.flag_credito, 0) = 1               AS flg_ativo_credito,
    COALESCE(r.flag_debito,  0) = 1               AS flg_ativo_debito,
    COALESCE(r.qtd_transacoes_3m, 0)              AS qtd_transacoes_3m,
    COALESCE(r.vlr_total_3m,      0)              AS vlr_total_3m,
    COALESCE(r.vlr_credito_3m,    0)              AS vlr_credito_3m,
    COALESCE(r.vlr_debito_3m,     0)              AS vlr_debito_3m,
    r.dat_ultima_transacao,
    NOW()                                         AS dat_geracao_flat
FROM stg_associados pa
LEFT JOIN dim_faixa_renda fr ON fr.cod_faixa_renda = pa.cod_faixa_renda
LEFT JOIN meses_por_associado ma ON ma.num_cpf_cnpj = pa.num_cpf_cnpj
LEFT JOIN resumo r ON r.num_cpf_cnpj = pa.num_cpf_cnpj
LEFT JOIN LATERAL (
    SELECT t.cod_cooperativa, t.cod_agencia, a.des_nome_agencia
    FROM transacoes_janela t
    JOIN stg_agencias a ON a.cod_cooperativa = t.cod_cooperativa AND a.cod_agencia = t.cod_agencia
    WHERE t.num_cpf_cnpj = pa.num_cpf_cnpj
    GROUP BY t.cod_cooperativa, t.cod_agencia, a.des_nome_agencia
    ORDER BY COUNT(*) DESC
    LIMIT 1
) ag ON TRUE
ON CONFLICT (num_cpf_cnpj) DO UPDATE SET
    flg_associado_frequente   = EXCLUDED.flg_associado_frequente,
    flg_ativo_credito         = EXCLUDED.flg_ativo_credito,
    flg_ativo_debito          = EXCLUDED.flg_ativo_debito,
    qtd_transacoes_3m         = EXCLUDED.qtd_transacoes_3m,
    vlr_total_3m              = EXCLUDED.vlr_total_3m,
    vlr_credito_3m            = EXCLUDED.vlr_credito_3m,
    vlr_debito_3m             = EXCLUDED.vlr_debito_3m,
    dat_ultima_transacao      = EXCLUDED.dat_ultima_transacao,
    dat_geracao_flat          = EXCLUDED.dat_geracao_flat;