# Engenharia de dados com dbt: bronze, silver e gold

Este documento é **conceitual**. Ele descreve o que você, como engenheiro de dados, vai construir com dbt em cima da
camada `raw`. **Não há projeto dbt pronto neste repositório, de propósito**: o exercício é seu. Aqui você encontra
a fronteira de cada camada, o que entra, o que sai e exercícios aplicados ao Northwind.

Como o dado chega na raw está em [infraestrutura.md](infraestrutura.md).

---

## 1. Quem é dono de quê

```
 INGESTÃO (pronto no lab)                 |  ENGENHEIRO (você, com dbt)
                                          |
 Postgres -> Debezium -> Kafka -> raw     |  bronze -----> silver --------> gold
                                          |  (staging)     (intermediate)   (marts)
 garante FIDELIDADE à origem:             |  garante SIGNIFICADO para o negócio:
  - mesmos nomes, tipos e nulabilidade    |   - tipos de negócio (dinheiro = Decimal)
  - mesmos valores de produção            |   - nomes e padrões consistentes
  - histórico completo (append-only)      |   - 1 linha por entidade (dedup)
  - metadados _cdc_* e _kafka_*           |   - deletes tratados, histórico SCD2
                                          |   - fatos e dimensões para consumo
 você NUNCA escreve na raw                |
```

A regra que separa os dois lados:

- **Conversão de transporte** é da ingestão. Exemplo: o Debezium manda `date` como inteiro de dias, e a raw já
  entrega `Date32`. Você nunca precisa saber como o Debezium serializa.
- **Conversão de negócio** é sua. Exemplo: `unit_price` é `real` em produção, e a raw mantém `Float32` porque esse
  é o tipo de produção. Decidir que dinheiro vira `Decimal(10,2)` é uma decisão de modelagem, e ela acontece
  na **bronze**.

---

## 2. Mapa das camadas

| camada | database no ClickHouse | convenção dbt | materialização sugerida | granularidade |
|---|---|---|---|---|
| raw | `raw` | `source` | (já existe) | 1 linha por **evento CDC** |
| bronze | `bronze` | `stg_<tabela>` | `view` | 1 linha por **evento CDC**, limpo e tipado |
| silver | `silver` | `int_<entidade>_*` + snapshots | `table` / `incremental` | 1 linha por **entidade** (estado atual) ou por **versão** (SCD2) |
| gold | `gold` | `fct_*`, `dim_*` | `table` | modelo dimensional (estrela) |

```
 raw.orders ----------> bronze.stg_orders ----+--> silver.int_orders_current ---+--> gold.fct_order_lines
 raw.order_details ---> bronze.stg_order_details -> silver.int_order_details_current
 raw.products --------> bronze.stg_products --+--> silver.int_products_history (SCD2) --> gold.dim_products
 raw.customers -------> bronze.stg_customers -+--> silver.int_customers_current -------> gold.dim_customers
 raw.employees -------> bronze.stg_employees ---------------------------------------> gold.dim_employees
                                                                         (gerada) --> gold.dim_date
```

---

## 3. raw como `source`

No dbt, a raw é declarada como **source**, ou seja, dado que o dbt lê mas não constrói. O nome do schema do source
é o database `raw`. Vale declarar também:

- **testes de source**: `not_null` nas colunas de PK e `accepted_values` em `_cdc_op` (`r`, `c`, `u`, `d`);
- **freshness**: com `_ingested_at` como `loaded_at_field`, o `dbt source freshness` mostra se a ingestão parou.

> Lembre: a raw tem **várias linhas por PK** (uma por evento). `unique` na PK da raw **vai falhar**, e é assim mesmo.

---

## 4. Bronze (staging): `stg_<tabela>`

**Objetivo:** uma interface limpa e estável sobre cada tabela raw. É o **único** lugar que lê `source()`.
Os modelos seguintes leem só `ref('stg_...')`.

| | |
|---|---|
| **Entra** | `raw.<tabela>`, todas as linhas (todos os eventos) |
| **Sai** | `bronze.stg_<tabela>`, **mesma granularidade** (1 linha por evento CDC), com tipos de negócio, nomes padronizados e metadados de CDC renomeados |
| **Não faz** | joins, dedup, agregações ou regras de negócio complexas |
| **Database** | `bronze` |

O que fazer nessa camada:

- **Tipagem de negócio**: valores monetários `Float32` → `Decimal(10, 2)` (`unit_price`, `freight`);
  `discount` (fração) → `Decimal(4, 2)`; `discontinued` (`Int32` 0/1) → `Bool`.
- **Renomear** para um padrão do projeto (ex.: `company_name` → `customer_name`, `ship_via` → `shipper_id`).
- **Padronizar** textos: espaços, maiúsculas e valores equivalentes (veja o exercício de `country`).
- **Metadados de CDC com nomes de negócio**: `_cdc_op = 'd'` → `is_deleted`;
  `_cdc_source_ts_ms` → `changed_at` (`DateTime64(3)`); manter `_cdc_lsn` para ordenação.
- **Colunas binárias** (`photo`, `picture`): decidir se ficam ou saem da bronze.

Exercícios:

1. Crie `stg_orders`, `stg_order_details`, `stg_products`, `stg_customers` e `stg_employees`.
2. Investigue `customers.country` e `orders.ship_country` (`SELECT DISTINCT`). Os clientes criados pelo gerador usam
   `Brazil`, `USA` etc. Há variações a padronizar?
3. `order_details.discount` vem como `Float32`: o que acontece com `0.05` ao converter para `Decimal`? Compare
   `toDecimal32(discount, 2)` com `toDecimal32(round(discount, 2), 2)`.
4. Adicione testes: `not_null` nas PKs e `accepted_values` em `is_deleted`.

---

## 5. Silver (intermediate): estado atual e histórico

**Objetivo:** transformar **eventos** em **entidades**. É aqui que o CDC vira tabela utilizável.

| | |
|---|---|
| **Entra** | modelos `stg_*` (eventos) |
| **Sai** | (a) **estado atual**: 1 linha por PK, sem as deletadas; (b) **histórico (SCD2)**: 1 linha por versão, com validade |
| **Database** | `silver` |

### 5.1 Deduplicação: "qual é a versão mais recente?"

Para cada PK, a versão vigente é a do **maior `_cdc_lsn`** (posição no WAL). Use `_kafka_offset` como desempate.
No ClickHouse, isso costuma sair com `argMax(coluna, _cdc_lsn)` agrupando pela PK, com
`ORDER BY ... LIMIT 1 BY <pk>`, ou com `row_number()` em window function. Vale comparar as três.

Cuidados:

- **Duplicatas at-least-once**: o mesmo evento pode aparecer duas vezes na raw (mesmo `_kafka_offset`).
  A dedup por PK + LSN já resolve isso.
- **Snapshot**: todos os eventos `r` compartilham o mesmo LSN do snapshot. Qualquer `c/u/d` posterior tem LSN maior.
- **Deletes**: se a versão mais recente é `is_deleted`, a entidade **não existe mais** no estado atual. Decida
  entre remover (*hard delete*) e manter com flag (*soft delete*).

### 5.2 Histórico (SCD tipo 2)

A raw já tem **todas as versões**. Então você pode derivar a SCD2 **direto dos eventos**, sem `dbt snapshot`:

| product_id | unit_price | valid_from | valid_to | is_current |
|---|---|---|---|---|
| 1 | 18.00 | (snapshot) | 2026-11-12 21:10 | false |
| 1 | 18.50 | 2026-11-12 21:10 | 2026-11-12 22:41 | false |
| 1 | 19.75 | 2026-11-12 22:41 | NULL | true |

Pistas: `valid_from` = `changed_at` da versão; `valid_to` = `changed_at` da **próxima** versão da mesma PK
(window `leadInFrame` ou `lagInFrame` ordenado por `_cdc_lsn`).

O gerador de tráfego reajusta `products.unit_price` e mexe em `units_in_stock`. Uma questão de modelagem: uma
mudança **só de estoque** deve abrir uma versão nova de preço?

### 5.3 `dbt snapshot`: compare as duas abordagens

O `dbt snapshot` (estratégia `check` em `unit_price`) também gera SCD2, só que **amostrando** o estado atual a cada
`dbt snapshot`. Rode os dois e compare: quantas versões cada abordagem captura se o preço mudar duas vezes entre
duas execuções do snapshot?

Exercícios:

1. `int_orders_current`, `int_order_details_current`, `int_customers_current`: estado atual com deletes tratados.
   **Teste**: `unique` na PK (agora tem que passar).
2. `int_products_history`: SCD2 de preço a partir dos eventos.
3. Valide contra a produção: `count()` de `int_orders_current` = `SELECT count(*) FROM orders` no Postgres.
4. Torne `int_orders_current` **incremental** (`materialized='incremental'`, filtrando por `_cdc_lsn` maior que o já
   processado). Pense no que acontece com um pedido que muda **depois** de já ter sido carregado.

---

## 6. Gold (marts): fatos e dimensões

**Objetivo:** modelo dimensional (estrela) pronto para BI e análise.

| | |
|---|---|
| **Entra** | modelos da silver |
| **Sai** | `fct_*` (eventos de negócio mensuráveis) e `dim_*` (contexto) |
| **Database** | `gold` |

Modelo sugerido:

```
                 dim_date
                    |
 dim_customers -- fct_order_lines -- dim_products (SCD2: preço vigente na data do pedido)
                    |        \
              dim_employees   dim_shippers
```

| modelo | grão | ideias de colunas |
|---|---|---|
| `fct_order_lines` | 1 linha por item de pedido (`order_id`, `product_id`) | quantidade, preço unitário, desconto, **receita líquida** = `unit_price * quantity * (1 - discount)`, datas do pedido, chaves das dimensões |
| `fct_orders` *(opcional)* | 1 linha por pedido | frete, nº de itens, receita total, **lead time** de envio (`shipped_date - order_date`), atraso (`shipped_date > required_date`) |
| `dim_customers` | 1 por cliente | nome, cidade, país padronizado |
| `dim_products` | 1 por **versão** de produto | categoria, fornecedor, preço, `valid_from`/`valid_to` |
| `dim_employees` | 1 por funcionário | nome, cargo, gestor (`reports_to`) |
| `dim_date` | 1 por dia | ano, mês, trimestre, dia da semana |

Exercícios:

1. Receita líquida por mês e por categoria.
2. Top 10 clientes por receita, e quantos são clientes criados pelo gerador de tráfego.
3. Lead time médio de envio por transportadora.
4. Pedidos cancelados (deletados) por mês. Onde essa informação ainda existe, se a silver remove deletados?
5. Join temporal: use a `dim_products` SCD2 para pegar o preço **de tabela vigente na data do pedido** e compare
   com o `unit_price` gravado no item.

---

## 7. Conectando o dbt ao lab

### Instalação (no host)

O dbt roda **fora** do compose, no seu ambiente. Use **Python 3.12** num ambiente virtual (versões muito novas de
Python podem ainda não ser suportadas pelo dbt):

```bash
uv venv --python 3.12 .venv-dbt          # ou: python3.12 -m venv .venv-dbt
source .venv-dbt/bin/activate             # fish: source .venv-dbt/bin/activate.fish
uv pip install dbt-clickhouse             # ou: pip install dbt-clickhouse
dbt --version
```

### `profiles.yml`

Crie `~/.dbt/profiles.yml` (ou `profiles.yml` na pasta do projeto dbt). Os valores vêm do `.env` do lab:

```yaml
northwind_lab:
  target: dev
  outputs:
    dev:
      type: clickhouse
      host: localhost
      port: 18123          # CLICKHOUSE_HTTP_PORT
      user: lab            # CLICKHOUSE_USER
      password: lab        # CLICKHOUSE_PASSWORD
      schema: bronze       # no ClickHouse, "schema" do dbt = database
      secure: false
      threads: 4
```

### Um database por camada

No `dbt_project.yml`, direcione cada pasta para o seu database:

```yaml
models:
  northwind_lab:
    bronze:
      +schema: bronze
      +materialized: view
    silver:
      +schema: silver
      +materialized: table
    gold:
      +schema: gold
      +materialized: table
```

Por padrão, o dbt **concatena** o schema do target com o customizado e cria `bronze_silver`. Para usar o nome
exato, sobrescreva a macro em `macros/generate_schema_name.sql`:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
```

E declare a raw como source:

```yaml
# models/bronze/_sources.yml
sources:
  - name: raw
    schema: raw
    loaded_at_field: _ingested_at
    freshness:
      warn_after: {count: 5, period: minute}
    tables:
      - name: orders
      - name: order_details
      - name: products
      - name: customers
      - name: employees
```

Teste a conexão:

```bash
dbt debug          # deve terminar com "All checks passed!"
```

---

## 8. Checklist de estudo

- [ ] Sei explicar por que a raw tem várias linhas por PK e o que é `_cdc_lsn`.
- [ ] Bronze: tipos de negócio e nomes padronizados, sem mudar a granularidade.
- [ ] Silver: estado atual com `unique` na PK passando e contagem igual à produção.
- [ ] Silver: SCD2 de preço derivada dos eventos (e comparada com `dbt snapshot`).
- [ ] Gold: `fct_order_lines` com receita líquida e dimensões conectadas.
- [ ] Testes (`not_null`, `unique`, `relationships`, `accepted_values`) e `source freshness` configurados.
- [ ] `dbt docs generate && dbt docs serve` para ver a linhagem raw → bronze → silver → gold.
