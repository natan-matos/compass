# Desafio de Engenharia de Dados — Sistema Cooperativo

## Estrutura do Projeto

```
desafio_engenharia/
├── docker-compose.yml           # Postgres 16 + pgAdmin
├── .env                         # Variáveis de ambiente (não sobe no git)
├── .env.example                 # Template para configuração
├── requirements.txt
├── data/                        # Arquivos CSV de entrada (não sobe no git)
│   ├── db_pessoa_associado.csv
│   ├── db_entidade_agencia.csv
│   └── db_cartoes_transacoes.csv
├── etl/
│   ├── extract.py               # Leitura dos CSVs
│   ├── transform.py             # Sanitização e tipagem
│   ├── load.py                  # DDL + carga no Postgres
│   └── pipeline.py              # Orquestrador (ponto de entrada)
├── sql/
│   ├── desafio2_flat_table.sql  # Tabela flat com indicadores
│   └── desafio3_dimensional.sql # Modelo estrela (Star Schema)
└── diagrama/
    └── er_dimensional.mermaid   # Diagrama ER do modelo dimensional
```

---

## Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e Docker Compose
- Python 3.11+

---

## Como executar

### 1. Configure o ambiente

```bash
cp .env.example .env
# Edite .env se quiser alterar senhas
```

### 2. Suba o Postgres

```bash
docker compose up -d
# Aguarda o healthcheck; Postgres estará pronto em ~10s
```

### 3. Instale as dependências Python

```bash
pip install -r requirements.txt
```

### 4. Execute o pipeline ETL

```bash
python -m etl.pipeline
```

### 5. (Opcional) Acesse o pgAdmin

Abra [http://localhost:8080](http://localhost:8080) com as credenciais do `.env`.
Adicione um servidor apontando para `postgres:5432`.

### Parar o banco

```bash
docker compose down          # mantém os dados
docker compose down -v       # remove os dados também
```

---

## Desafio 1 — Pipeline ETL

O pipeline segue o padrão **Extract → Transform → Load** com separação clara de responsabilidades:

| Módulo | Responsabilidade |
|--------|-----------------|
| `extract.py` | Lê os CSVs brutos (detecção automática de separador, encoding `latin-1`) |
| `transform.py` | Sanitiza strings, normaliza modalidades, converte datas e valores BR, aplica zero-padding |
| `load.py` | Cria schema com PKs/FKs/índices; carga idempotente via `ON CONFLICT` |
| `pipeline.py` | Orquestra os 3 módulos, configura logging, lê variáveis de ambiente |

**Premissas assumidas:**
- `num_cpf_cnpj` está anonimizado (hash) → tratado como `VARCHAR`
- `vlr_transacao` usa vírgula como separador decimal (padrão BR)
- Modalidade `'0'` é dado inválido → carregado como `NULL`
- Duplicatas de associado (mesmo CPF): mantém o registro com `dat_associacao` mais recente
- Campos de texto contêm `?` onde havia caracteres acentuados — a informação foi perdida na origem do arquivo (o `?` é ASCII `0x3F` literal, não um problema de encoding). Os valores são carregados como estão. Uma abordagem de correção via regex + wordlist de nomes do IBGE foi avaliada e descartada por complexidade desnecessária para o escopo deste desafio.

---

## Desafio 2 — Tabela Flat (`sql/desafio2_flat_table.sql`)

Tabela única com todos os associados e seus indicadores de atividade:

| Indicador | Critério |
|-----------|----------|
| `flg_associado_frequente` | Transacionou em **cada um** dos últimos 3 meses-calendário |
| `flg_ativo_credito` | Ao menos 1 transação `CREDITO` nos últimos 3 meses |
| `flg_ativo_debito` | Ao menos 1 transação `DEBITO` nos últimos 3 meses |

> **"Últimos 3 meses"** = os 3 meses-calendário completos anteriores ao mês atual.
> Evita meses parciais que distorceriam o indicador de frequência.

---

## Desafio 3 — Modelo Dimensional (`sql/desafio3_dimensional.sql`)

Star Schema com granularidade de **1 linha por transação de cartão**.

```
             dim_tempo
                 │
dim_localidade ──┤
                 │
dim_modalidade ──┼── fato_transacao ──── dim_associado
                 │
  dim_cartao ────┤
                 │
  dim_agencia ───┘
```

**Decisões de design:**
- Chaves surrogate (`SERIAL`) em todas as dimensões
- SCD Tipo 1 — fonte não contém histórico de alterações cadastrais
- `dat_hora_transacao` como atributo degenerado na fato (cardinalidade muito alta para virar dimensão)
- `dim_faixa_renda` desnormalizada dentro de `dim_associado` para simplificar queries analíticas