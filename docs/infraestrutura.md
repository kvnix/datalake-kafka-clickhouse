# Infraestrutura do lab: Postgres → Debezium → Kafka → ClickHouse (raw)

Este documento explica **como o dado sai do banco de produção e chega na camada `raw`** do ClickHouse.
Tudo aqui é responsabilidade da *ingestão*. O que acontece depois da raw (bronze/silver/gold) está em
[engenharia-de-dados.md](engenharia-de-dados.md).

```
 +-------------------+  WAL (pgoutput)  +--------------------+   JSON   +---------+
 | Postgres          | ---------------> | Kafka Connect      | -------> | Kafka   |
 | northwind         |  slot +          | + Debezium         |  1 tópico| (KRaft) |
 | ("produção")      |  publicação      |   PostgresConnector|  /tabela +----+----+
 +---------^---------+                  +--------------------+               |
           |                                                                 |
           | INSERT/UPDATE/DELETE                                            v
 +---------+---------+                                 +-----------------------------+
 | traffic-generator |                                 | ClickHouse                  |
 +-------------------+                                 |  _kafka_queue (Kafka engine)|
                                                       |   -> _cdc_stream (Null)     |
                                                       |   -> raw.<tabela> (1 por    |
                                                       |      tabela de produção)    |
                                                       +-----------------------------+
```

Os comandos abaixo rodam na raiz do projeto e usam `docker compose exec`, então não exigem nada instalado no host além do Docker.

<!-- ÍNDICE -->
- [0. Roteiro: siga um evento de ponta a ponta](#0-roteiro-siga-um-evento-de-ponta-a-ponta)
- [1. Banco de produção e replicação lógica](#1-banco-de-produção-e-replicação-lógica)
- [2. Debezium e o envelope de CDC](#2-debezium-e-o-envelope-de-cdc)
- [3. Kafka: tópicos, offsets e UI](#3-kafka-tópicos-offsets-e-ui)
- [4. Ingestão no ClickHouse](#4-ingestão-no-clickhouse)
- [5. Contrato da camada raw](#5-contrato-da-camada-raw)
- [6. Gerador de tráfego](#6-gerador-de-tráfego)
- [7. Troubleshooting](#7-troubleshooting)

---

## 0. Roteiro: siga um evento de ponta a ponta

O objetivo é fazer **uma** mudança no banco de produção e encontrá-la em cada etapa do pipeline.
A tabela `shippers` serve bem porque o gerador de tráfego não mexe nela.

**1) Deixe um consumidor Kafka aberto** (terminal 1):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic northwind.public.shippers --formatter-property print.key=true
```

**2) Faça um UPDATE na produção** (terminal 2):

```bash
docker compose exec postgres psql -U postgres -d northwind \
  -c "UPDATE shippers SET phone = '(503) 555-1234' WHERE shipper_id = 2;"
```

**3) Veja a mensagem chegar no terminal 1.** A chave é a PK, e o valor é o envelope Debezium com `op: "u"`,
`before.phone` (antigo) e `after.phone` (novo). Na UI (http://localhost:8080), a mesma mensagem aparece em
*Topics → northwind.public.shippers → Messages*.

**4) Ache a linha na raw** (em poucos segundos):

```bash
docker compose exec clickhouse clickhouse-client --user lab --password lab -q "
  SELECT _cdc_op, shipper_id, phone, _cdc_lsn, _kafka_offset, _cdc_before
  FROM raw.shippers WHERE shipper_id = 2 ORDER BY _kafka_offset FORMAT Vertical"
```

Você verá a linha do snapshot (`_cdc_op = r`) e, depois, a do update (`_cdc_op = u`) com o telefone novo e o
JSON anterior em `_cdc_before`. A raw guarda **as duas**: é o histórico.

**5) Volte da raw para o Kafka** usando o `_kafka_offset` da linha `u` (troque `N` pelo valor):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic northwind.public.shippers --partition 0 --offset N --max-messages 1
```

**6) Confira o WAL** que o Debezium já confirmou (o `confirmed_flush_lsn` avança a cada evento processado):

```bash
docker compose exec postgres psql -U postgres -d northwind \
  -c "SELECT slot_name, confirmed_flush_lsn, confirmed_flush_lsn - '0/0'::pg_lsn AS lsn_decimal
      FROM pg_replication_slots;"
```

O Postgres mostra o LSN em hexadecimal (`0/19EF598`). `lsn - '0/0'` converte para o mesmo número decimal que
aparece em `_cdc_lsn` na raw. O Debezium só **confirma** o LSN ao Postgres quando o Kafka Connect grava seus
offsets (a cada ~60 s, `offset.flush.interval.ms`). Logo depois do update, o `confirmed_flush_lsn` pode estar
**atrás** do `_cdc_lsn` da sua linha. Rode de novo em um minuto e ele terá passado. Esse atraso é o WAL que o slot
ainda está segurando.

---

## 1. Banco de produção e replicação lógica

O serviço `postgres` (Postgres 17) é o nosso **banco de produção**. Na primeira subida, com o volume vazio,
ele executa os scripts de `postgres/init/`:

| arquivo | o que faz |
|---|---|
| `01-northwind.sql` | dump original do [pthom/northwind_psql](https://github.com/pthom/northwind_psql), sem alteração: 14 tabelas, PKs e FKs |
| `02-cdc.sql` | aplica `REPLICA IDENTITY FULL` em todas as tabelas de `public` |

> Esses scripts **só rodam com o volume vazio**. Para recarregar o dump, use `docker compose down -v`.

### Como o CDC lê o Postgres

O Postgres registra toda mudança confirmada no **WAL** (*write-ahead log*), o log que ele já usa para
recuperação de falhas. Com `wal_level=logical`, o WAL passa a ter informação suficiente para
**decodificar** as mudanças em linhas (tabela, colunas e valores), e não só em páginas de disco.

```
 transação COMMIT --> WAL --> decodificação lógica (plugin pgoutput) --> slot --> Debezium
                                                   ^
                                    publicação: QUAIS tabelas são publicadas
```

| peça | o que é | quem cria |
|---|---|---|
| `wal_level=logical` | habilita a decodificação lógica | `command:` do serviço no compose |
| **publicação** (`dbz_northwind`) | lista de tabelas cujas mudanças são publicadas | Debezium (`publication.autocreate.mode=filtered`) |
| **slot de replicação** (`debezium_northwind`) | "marcador" que guarda até onde o consumidor leu. O Postgres **retém o WAL** a partir desse ponto até o consumidor confirmar | Debezium |
| `pgoutput` | plugin nativo de decodificação (não precisa instalar nada) | já vem no Postgres |
| `REPLICA IDENTITY FULL` | faz o WAL guardar a linha **anterior inteira** em UPDATE/DELETE | `02-cdc.sql` |

Sem `REPLICA IDENTITY FULL`, o padrão (`DEFAULT`) só guarda a **chave primária** da linha antiga, e o
campo `before` do Debezium chega quase vazio. Com `FULL`, dá para ver exatamente o que mudou.

### Consultas úteis

Abrir um psql no banco de produção:

```bash
docker compose exec postgres psql -U postgres -d northwind
```

Conferir a configuração de WAL:

```sql
SHOW wal_level;              -- logical
SHOW max_replication_slots;  -- 4
```

Conferir a replica identity de cada tabela (`f` = full, `d` = default):

```sql
SELECT c.relname AS tabela, c.relreplident
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
ORDER BY 1;
```

Ver a publicação e as tabelas publicadas (existe depois que o conector sobe):

```sql
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_northwind';
```

Ver o slot e **quanto WAL ele está segurando** (lag do consumidor):

```sql
SELECT slot_name, plugin, active, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS wal_retido
FROM pg_replication_slots;
```

Conectar a partir do host (DBeaver, psql etc.): `localhost:${POSTGRES_PORT}` (padrão `55432`),
banco `northwind`, usuário/senha do `.env`.

---

## 2. Debezium e o envelope de CDC

O **Debezium** é um conector que roda dentro do **Kafka Connect** (serviço `connect`). Ele abre uma conexão de
replicação com o Postgres, lê o slot e publica cada mudança como uma mensagem no Kafka.

### Registro do conector

O job `connect-init` executa `connect/register.sh`, que faz:

```
PUT /connectors/northwind-cdc/config   <-- connect/northwind-cdc.json
```

`PUT .../config` é **idempotente**: cria o conector se ele não existe e só atualiza a config se já existe.
A posição de leitura (offset do Debezium) fica no tópico `_connect_offsets`, então registrar de novo **não refaz o
snapshot**.

A senha não está no JSON. A config usa `${env:POSTGRES_PASSWORD}`, que o *config provider* do Connect resolve
a partir das variáveis de ambiente do container. Por isso a API REST devolve a referência e não o valor.

Configurações importantes (`connect/northwind-cdc.json`):

| config | valor | por quê |
|---|---|---|
| `plugin.name` | `pgoutput` | plugin nativo do Postgres |
| `snapshot.mode` | `initial` | na 1ª vez, lê tudo que já existe (`op = r`) e depois passa a fazer streaming |
| `topic.prefix` | `northwind` | tópicos `northwind.public.<tabela>` |
| `key/value.converter` | `JsonConverter`, `schemas.enable=false` | JSON legível, sem o schema repetido em cada mensagem |
| `tombstones.on.delete` | `false` | sem mensagem nula após o delete; o próprio evento `op = d` já representa o delete |
| `time.precision.mode` | `adaptive` | `date` vira **inteiro de dias desde 1970-01-01** (ex.: `9681` = 1996-07-04) |
| `binary.handling.mode` | `base64` | `bytea` vira texto base64 |
| *(sem SMT)* | | o envelope **não** é achatado: `before` e `source` chegam inteiros no Kafka |

### Anatomia de uma mensagem

**Chave** (a PK da linha; em `order_details` ela é composta):

```json
{"order_id":10248,"product_id":11}
```

**Valor** (envelope Debezium; exemplo de um UPDATE em `shippers`):

```json
{
  "before": {"shipper_id":1,"company_name":"Speedy Express","phone":"(503) 555-9831"},
  "after":  {"shipper_id":1,"company_name":"Speedy Express","phone":"(503) 555-0000"},
  "source": {
    "connector":"postgresql", "db":"northwind", "schema":"public", "table":"shippers",
    "lsn":27612345,          "txId":4170,          "ts_ms":1794526792894,
    "snapshot":"false"
  },
  "op": "u",
  "ts_ms": 1794526793010
}
```

| campo | significado |
|---|---|
| `op` | `r` = leitura do snapshot, `c` = insert, `u` = update, `d` = delete |
| `before` | linha antes da mudança (`null` em `r` e `c`) |
| `after` | linha depois da mudança (`null` em `d`) |
| `source.lsn` | posição no WAL; **ordena** os eventos de forma confiável |
| `source.txId` | id da transação no Postgres (eventos da mesma transação compartilham o valor) |
| `source.ts_ms` | quando a mudança aconteceu no banco |
| `ts_ms` | quando o Debezium processou o evento |

### Comandos úteis

```bash
# conectores e status
curl -s localhost:8083/connectors
curl -s localhost:8083/connectors/northwind-cdc/status

# configuração atual (senha aparece como ${env:...})
curl -s localhost:8083/connectors/northwind-cdc/config

# reiniciar conector + task
curl -s -X POST "localhost:8083/connectors/northwind-cdc/restart?includeTasks=true"

# registrar/atualizar de novo (idempotente)
docker compose run --rm connect-init
```

---

## 3. Kafka: tópicos, offsets e UI

O Kafka (serviço `kafka`) roda em **modo KRaft**, sem Zookeeper, com um único nó que é broker e controller ao mesmo tempo.
Há dois *listeners*:

| listener | endereço | quem usa |
|---|---|---|
| `INTERNAL` | `kafka:29092` | containers (Connect, ClickHouse, UI) |
| `EXTERNAL` | `localhost:${KAFKA_PORT}` (padrão `9092`) | ferramentas no host |

### Tópicos

| tópico | conteúdo |
|---|---|
| `northwind.public.<tabela>` | eventos CDC, **1 tópico por tabela**, 1 partição (preserva a ordem por tabela) |
| `_connect_configs`, `_connect_offsets`, `_connect_statuses` | estado interno do Kafka Connect |
| `__consumer_offsets` | offsets dos consumer groups (ex.: o grupo `clickhouse_raw` do ClickHouse) |

Um **offset** é a posição de uma mensagem dentro da partição (0, 1, 2...). O Kafka não apaga a mensagem quando ela é
lida. Cada *consumer group* guarda até onde já leu, e por isso o Kafka é a **fonte reprocessável** de todas as mudanças.

Tabelas sem nenhuma linha (no Northwind, `customer_customer_demo` e `customer_demographics`) não geram eventos no
snapshot, então o Debezium não criaria os tópicos delas. O job `raw-init` cria os tópicos que faltam antes de
o ClickHouse assiná-los.

### Comandos úteis

```bash
# listar tópicos
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list

# último offset de cada tópico CDC (= quantidade de mensagens, já que começa em 0)
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 \
  --topic-partitions 'northwind\.public\..*:0'

# ler as 3 primeiras mensagens de orders, com a chave
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic northwind.public.orders --from-beginning --max-messages 3 --formatter-property print.key=true

# acompanhar mensagens novas em tempo real (Ctrl+C para sair)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic northwind.public.products

# consumer groups e lag (o ClickHouse aparece como clickhouse_raw)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
  --describe --group clickhouse_raw
```

### Kafka UI

Abra **http://localhost:8080** (porta `KAFKA_UI_PORT`):

- **Topics → `northwind.public.orders` → Messages**: mensagens com chave e envelope. Use *Seek type: Offset* para ir a uma posição específica.
- **Consumers → `clickhouse_raw`**: offset atual e lag do ClickHouse por tópico.
- **Kafka Connect → `northwind-cdc`**: status do conector e da task, config e restart.

---

## 4. Ingestão no ClickHouse

O ClickHouse (serviço `clickhouse`) consome os tópicos CDC com o **Kafka table engine**: uma tabela especial
que, em vez de guardar dados, funciona como um **consumidor Kafka**. Quem grava nas tabelas de verdade são as
**materialized views** (MVs), que são *triggers de insert*: cada bloco de mensagens lido da fila é transformado
e inserido no destino.

### Topologia (fan-out)

```
 Kafka: northwind.public.* (14 tópicos)
   |
   v
 raw._kafka_queue          ENGINE = Kafka  (1 consumidor, group "clickhouse_raw",
   |                                        formato JSONAsString: a mensagem inteira em _raw)
   | raw._mv_queue_to_stream  (adiciona _kafka_topic/_partition/_offset/_timestamp)
   v
 raw._cdc_stream           ENGINE = Null   (não guarda nada; só repassa o bloco às MVs)
   |
   +--> raw._mv_orders     WHERE _kafka_topic = 'northwind.public.orders'    --> raw.orders
   +--> raw._mv_customers  WHERE _kafka_topic = 'northwind.public.customers' --> raw.customers
   +--> ...  (1 MV por tabela)                                               --> raw.<tabela>
```

Por que esse desenho:

- **Um consumidor só.** Cada tabela Kafka ocupa uma thread do pool de mensageria do ClickHouse (16 por padrão).
  Com uma fila para todos os tópicos, o lab usa 1.
- **Ordem de criação.** O Kafka engine **começa a consumir assim que a primeira MV é anexada a ele**. Se cada
  tabela tivesse a própria MV lendo direto da fila, a primeira MV criada já consumiria o snapshot, e as outras
  perderiam esses eventos. Por isso o job `raw-init` cria, nesta ordem:

  1. as tabelas `raw.<tabela>`
  2. `raw._cdc_stream` (Null)
  3. as MVs por tabela
  4. `raw._kafka_queue`
  5. `raw._mv_queue_to_stream`, **por último**: só aqui o consumo começa, com todo o fan-out pronto.

- **Offset inicial `earliest`** (`clickhouse/config.d/kafka.xml`): um consumer group novo lê o tópico desde o
  início, incluindo o snapshot.

### O que o `raw-init` faz

O job `raw-init` (`tools/raw_init.py`) roda a cada `docker compose up` e é idempotente:

1. Lê `information_schema.columns` do Postgres (tabelas de `public`, colunas em ordem, nulabilidade).
2. Mapeia os tipos (`tools/type_map.py`). Se algum tipo não tiver mapeamento, **para antes de criar qualquer coisa**
   e informa tabela, coluna e tipo.
3. Cria no Kafka os tópicos CDC que ainda não existem. O Debezium só cria o tópico no primeiro evento, então
   tabelas vazias (`customer_customer_demo`, `customer_demographics`) ficariam sem tópico.
4. Aplica o DDL com `IF NOT EXISTS`, na ordem acima, e cria os databases `bronze`, `silver` e `gold`.

Para ver o DDL gerado:

```bash
docker compose exec clickhouse clickhouse-client --user lab --password lab \
  -q "SHOW CREATE TABLE raw._mv_orders" --format TSVRaw
```

### Consultas úteis

Abrir o cliente do ClickHouse (ou use `localhost:${CLICKHOUSE_HTTP_PORT}` em DBeaver/dbt):

```bash
docker compose exec clickhouse clickhouse-client --user lab --password lab
```

Estado do consumidor Kafka (offset por tópico, mensagens lidas, erros):

```sql
SELECT table, assignments.topic, assignments.current_offset,
       num_messages_read, length(exceptions.text) AS n_erros, last_poll_time
FROM system.kafka_consumers
FORMAT Vertical;
```

> `current_offset = -1001` significa "ainda sem offset salvo" (tópico sem nenhuma mensagem).

Últimos erros do consumidor. É um **histórico** (os últimos 10), então olhe o horário:

```sql
SELECT arrayJoin(arrayZip(exceptions.time, exceptions.text)) AS e,
       e.1 AS quando, substring(e.2, 1, 200) AS erro
FROM system.kafka_consumers
ORDER BY quando DESC;
```

Volume por tabela raw:

```sql
SELECT name, total_rows FROM system.tables
WHERE database = 'raw' AND engine = 'MergeTree' ORDER BY name;
```

---

## 5. Contrato da camada raw

A raw é a **fronteira entre a ingestão e o engenheiro de dados**. O contrato é:

1. **Uma tabela por tabela de produção**: `raw.<tabela>`, com o mesmo nome.
2. **Mesmas colunas, mesma ordem, tipo equivalente e mesma nulabilidade** de produção (`NOT NULL` vira tipo
   não-`Nullable`).
3. **Mesmos valores de produção.** A ingestão só desfaz a *representação de transporte* do Debezium. Ela **não**
   faz conversão de negócio. `real` continua `Float32`, mesmo sendo dinheiro: decidir por `Decimal` é trabalho do dbt.
4. **Append-only.** Cada evento CDC vira uma linha nova. Nada é deduplicado, atualizado ou removido.
   Em um `DELETE` (`_cdc_op = 'd'`), as colunas vêm preenchidas com o estado anterior (`before`).
5. **Entrega at-least-once.** Em falhas e reinícios, o mesmo evento pode aparecer duas vezes. Deduplicar é tarefa da silver.

### Tipos

| Postgres | raw (ClickHouse) | conversão feita na ingestão |
|---|---|---|
| `smallint` / `integer` / `bigint` | `Int16` / `Int32` / `Int64` | — |
| `real` / `double precision` | `Float32` / `Float64` | — |
| `boolean` | `Bool` | — |
| `character varying` / `character` / `text` | `String` | — |
| `date` | `Date32` | dias desde 1970-01-01 → data. `Date32` porque há datas **antes de 1970** (`employees.birth_date` = 1937-09-19) |
| `bytea` | `String` (bytes) | base64 → bytes originais |
| `numeric(p,s)` | `Decimal(p,s)` | texto → decimal |
| `timestamp` | `DateTime64(6)` | microssegundos → data/hora |

### Colunas de metadado

| coluna | origem | uso típico |
|---|---|---|
| `_cdc_op` | `op` | `r` snapshot, `c` insert, `u` update, `d` delete |
| `_cdc_lsn` | `source.lsn` | **ordenar versões** da mesma PK (maior = mais recente) |
| `_cdc_tx_id` | `source.txId` | agrupar mudanças da mesma transação |
| `_cdc_source_ts_ms` | `source.ts_ms` | quando aconteceu no Postgres (epoch ms) |
| `_cdc_ts_ms` | `ts_ms` | quando o Debezium processou (epoch ms) |
| `_cdc_before` | `before` (JSON) | estado anterior em `u`/`d`; `''` em `r`/`c` |
| `_kafka_topic`, `_kafka_partition`, `_kafka_offset` | Kafka | rastrear a mensagem de origem; `(_kafka_topic, _kafka_offset)` identifica o evento |
| `_kafka_timestamp` | Kafka | quando a mensagem entrou no Kafka |
| `_ingested_at` | `now64(3)` | quando a linha chegou no ClickHouse |

Exemplo: o histórico do produto 1.

```sql
SELECT _cdc_op, unit_price, _cdc_lsn, _cdc_before
FROM raw.products
WHERE product_id = 1
ORDER BY _cdc_lsn;
```

---

## 6. Gerador de tráfego

O Northwind original é estático. O serviço `traffic-generator` (`tools/traffic.py`) mantém o banco de produção
**vivo**: a cada `TRAFFIC_TICK_SECONDS` ele sorteia uma ação (por peso) e a executa **em uma única transação**,
respeitando as FKs.

| ação | operação no Postgres | o que gera na raw | peso padrão |
|---|---|---|---|
| `new_order` | `INSERT orders` + 1 a 4 `order_details` | `c` em `orders` e `order_details` | 30 |
| `ship_order` | `UPDATE orders SET shipped_date` (pedido pendente) | `u` em `orders` (nova versão da mesma PK) | 25 |
| `stock` | `UPDATE products SET units_in_stock` | `u` frequente em `products` | 20 |
| `price` | `UPDATE products SET unit_price` (-5% a +10%) | `u` em `products`: histórico de preço para **SCD2** | 10 |
| `cancel_order` | `DELETE order_details` + `DELETE orders` (pedido pendente) | `d` em `order_details` e `orders` | 10 |
| `new_customer` | `INSERT customers` | `c` em `customers` | 5 |

Cada ação vira uma linha de log com a tabela e a PK, para você seguir a mudança até o Kafka e a raw:

```bash
docker compose logs -f traffic-generator
# 00:02:44 [traffic] new_order tabela=orders pk=11082 itens(order_details product_id)=21,20,18
# 00:02:49 [traffic] ship_order tabela=orders pk=11008
```

### Variáveis (`.env`)

| variável | padrão | efeito |
|---|---|---|
| `TRAFFIC_ENABLED` | `true` | `false` deixa o processo ocioso, sem nenhuma escrita |
| `TRAFFIC_TICK_SECONDS` | `5` | intervalo entre ações |
| `TRAFFIC_MAX_ORDER_ID` | `32000` | limite para novos `order_id` (a coluna é `smallint`, máx. 32767) |
| `TRAFFIC_WEIGHT_<AÇÃO>` | ver tabela | peso relativo de cada ação; `0` desliga a ação |

Para aplicar mudanças no `.env`: `docker compose up -d traffic-generator`.

### Detalhes que importam

- **Espera o slot do Debezium.** Antes da primeira ação, o gerador espera o slot `debezium_northwind` existir.
  O snapshot do Debezium enxerga o banco **no instante da criação do slot**, então tudo o que o gerador escreve
  depois vira evento `c/u/d`, e os eventos `r` continuam batendo exatamente com o dump original.
- **Chaves sem mexer no schema de produção.** O Northwind não tem sequences: o gerador usa `max(order_id) + 1`
  (há um único escritor). Ao chegar a `TRAFFIC_MAX_ORDER_ID`, ele para de criar pedidos e registra um `AVISO`,
  mas as outras ações continuam. Com os padrões, isso leva cerca de 4 dias. `docker compose down -v` reseta.
- **Falhas.** Em erro, a transação sofre rollback, o erro é logado e o gerador segue. Se o Postgres cair, ele
  reconecta sozinho.
- **Fuso horário.** Os containers rodam em **UTC**: `current_date` (usado em `order_date`) pode estar um dia à
  frente do seu relógio local à noite.

---

## 7. Troubleshooting

Comece sempre por:

```bash
docker compose ps -a        # jobs devem estar "Exited (0)"; serviços "healthy"
./scripts/verify.sh         # aponta qual etapa do pipeline falhou
```

### Um job saiu com erro

```bash
docker compose logs connect-init
docker compose logs raw-init
docker compose up -d        # roda os jobs de novo (são idempotentes)
```

`raw-init` aborta **antes de criar qualquer objeto** se encontrar um tipo Postgres sem mapeamento. A mensagem traz
tabela, coluna e tipo. Nesse caso, adicione o tipo em `tools/type_map.py`.

### Conector Debezium

```bash
curl -s localhost:8083/connectors/northwind-cdc/status
docker compose logs connect | grep -iE "error|exception" | tail -20

# reiniciar conector + task (mantém offsets: não refaz snapshot)
curl -s -X POST "localhost:8083/connectors/northwind-cdc/restart?includeTasks=true"
```

Se a task estiver `FAILED`, o campo `trace` do status mostra a causa (ex.: senha errada no `.env`, Postgres fora).

### Slot de replicação e WAL retido

Enquanto o conector está parado, o Postgres **guarda o WAL** a partir do slot, e o disco cresce. Para ver o quanto:

```bash
docker compose exec postgres psql -U postgres -d northwind -c "
  SELECT slot_name, active,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS wal_retido
  FROM pg_replication_slots;"
```

`active = f` com `wal_retido` crescendo indica que o Debezium não está consumindo.

**Descartar o slot** libera o WAL, mas o conector perde a posição. Os offsets salvos no Kafka Connect apontam
para um LSN que o slot novo não tem, e o Debezium **se recusa a continuar** (*"The connector is trying to read
change stream starting at..."*), para não perder dados em silêncio. Por isso é preciso zerar os offsets também,
e o conector **refaz o snapshot**. A raw recebe um **segundo conjunto de eventos `r`**, e o `verify.sh` passa a
acusar contagem de `r` diferente do dump.

```bash
C=localhost:8083/connectors/northwind-cdc
curl -s -X PUT $C/stop                 # 1. parar o conector (estado STOPPED)
curl -s -X DELETE $C/offsets           # 2. zerar os offsets do conector
docker compose exec postgres psql -U postgres -d northwind \
  -c "SELECT pg_drop_replication_slot('debezium_northwind');"   # 3. descartar o slot
curl -s -X PUT $C/resume               # 4. retomar: cria slot novo e refaz o snapshot
```

No lab, normalmente é mais simples fazer o **reset completo**: `docker compose down -v && docker compose up -d`.

### Dado não chega na raw

1. **O evento está no Kafka?** Veja o último offset do tópico (seção 3) ou a Kafka UI.
2. **O ClickHouse está consumindo?** Lag do grupo `clickhouse_raw`:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server kafka:29092 --describe --group clickhouse_raw
   ```

   `LAG` > 0 parado indica consumo travado. Veja os erros do consumidor (seção 4) e:

   ```bash
   docker compose logs clickhouse | grep -iE "kafka|error" | tail -20
   ```

3. **Uma MV falhou?** Se o insert em uma tabela de destino falhar, o bloco inteiro é reprocessado
   (*at-least-once*). As outras tabelas do mesmo bloco podem receber **linhas duplicadas**. Para conferir:

   ```sql
   SELECT count() - uniqExact(_kafka_offset) AS duplicadas FROM raw.orders;
   ```

   Esse é um dos motivos de a silver deduplicar por PK + `_cdc_lsn`.

### Mudei o schema de produção

A raw **não acompanha** mudanças de schema automaticamente. Uma coluna nova no Postgres continua chegando no Kafka,
mas não aparece em `raw.<tabela>`. Tabelas novas também precisam de `REPLICA IDENTITY FULL` manual. No lab, o
caminho mais simples é aplicar a mudança e fazer o reset completo (`down -v`), ou recriar a raw:

```bash
docker compose exec clickhouse clickhouse-client --user lab --password lab -q "DROP DATABASE raw SYNC"
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 \
  --delete --group clickhouse_raw
docker compose up -d raw-init     # recria a raw e relê os tópicos desde o início
```

### Porta ocupada

Erro `port is already allocated` no `up`: troque a porta correspondente no `.env` (ex.: `CLICKHOUSE_HTTP_PORT`) e
suba de novo.
