# Northwind CDC Lab: Postgres → Debezium → Kafka → ClickHouse

Lab local de engenharia de dados. Um banco de **produção** (Postgres com o dataset Northwind) recebe tráfego
contínuo. Cada mudança é capturada via **CDC** (Debezium), passa pelo **Kafka** e chega numa camada **raw** no
**ClickHouse**, pronta para você modelar com **dbt** (bronze → silver → gold).

```
 traffic-generator --> Postgres --WAL--> Debezium --> Kafka --> ClickHouse
                       (produção)        (Connect)    (KRaft)   raw.<tabela>
                                                                    |
                                            dbt (você) --> bronze -> silver -> gold
```

## Pré-requisitos

- Docker com Docker Compose v2 (`docker compose version`)
- ~4 GB de RAM livres
- Portas livres no host (ajustáveis no `.env`): `55432`, `9092`, `8083`, `18123`, `19000`, `8080`

## Subir

```bash
cp .env.example .env      # obrigatório: todos os serviços leem o .env
docker compose up -d      # 1ª vez: baixa imagens e builda a imagem tools (alguns minutos)
```

Ordem de subida, controlada por healthchecks:

```
postgres ---------------+
kafka ----> connect ----+--> connect-init --+--> raw-init --> traffic-generator
                             (registra o    |    (cria a raw)
                              Debezium)     |
clickhouse ---------------------------------+
kafka-ui: depois de kafka + connect
```

`connect-init` e `raw-init` são **jobs**: terminam com `Exited (0)`, e é isso que se espera deles.

## Verificar

```bash
./scripts/verify.sh
```

Confere o conector RUNNING, as 14 tabelas raw, o snapshot igual ao dump original e a chegada de eventos
`c`/`u`/`d`. Logo após a primeira subida, espere 1 ou 2 minutos para o gerador produzir eventos.

## Serviços

| serviço | para quê | acesso pelo host |
|---|---|---|
| `postgres` | banco de produção (Northwind) | `localhost:55432` · db `northwind` · `postgres`/`postgres` |
| `kafka` | broker (KRaft) | `localhost:9092` |
| `connect` | Kafka Connect + Debezium | API REST: http://localhost:8083/connectors |
| `kafka-ui` | UI de tópicos, mensagens, consumers e conectores | http://localhost:8080 |
| `clickhouse` | camada raw + databases do dbt | HTTP `localhost:18123` · nativo `localhost:19000` · `lab`/`lab` |
| `traffic-generator` | INSERT/UPDATE/DELETE contínuos | `docker compose logs -f traffic-generator` |
| `connect-init` | job: registra o conector | — |
| `raw-init` | job: cria tópicos ausentes + DDL da raw | — |

> As portas do ClickHouse fogem do padrão (8123/9000) para não conflitar com outro ClickHouse local.
> As credenciais são **de laboratório**, só para uso local.

Primeiros comandos:

```bash
# psql no banco de produção
docker compose exec postgres psql -U postgres -d northwind

# cliente do ClickHouse
docker compose exec clickhouse clickhouse-client --user lab --password lab
```

```sql
-- no ClickHouse: histórico de mudanças de um pedido
SELECT _cdc_op, order_id, shipped_date, _cdc_lsn
FROM raw.orders WHERE order_id = 10248 ORDER BY _cdc_lsn;
```

## Parar, resetar e desligar

| comando | efeito |
|---|---|
| `docker compose stop` | pausa tudo e mantém o estado |
| `docker compose down` | remove os containers e **mantém os volumes**. No próximo `up`, o CDC continua de onde parou, sem refazer o snapshot |
| `docker compose down -v` | **reset completo**: apaga Postgres, Kafka e ClickHouse. O próximo `up` recarrega o dump e refaz o snapshot |
| `docker compose up -d traffic-generator` | aplica mudanças do `.env` no gerador |

Para pausar só o tráfego: `docker compose stop traffic-generator`.

## Estrutura

```
docker-compose.yml        serviços do lab
.env.example              configuração (copie para .env)
postgres/init/            dump do Northwind (sem alterações) + preparação de CDC
connect/                  config do conector Debezium + script de registro
clickhouse/config.d/      config do Kafka engine (offset inicial)
tools/                    imagem Python: raw_init.py (DDL da raw), traffic.py (gerador), testes
scripts/verify.sh         verificação do pipeline
docs/                     documentação
```

## Documentação

- [docs/infraestrutura.md](docs/infraestrutura.md): como o dado vai do Postgres até a raw (WAL, Debezium, Kafka,
  ingestão no ClickHouse, contrato da raw, gerador e troubleshooting).
- [docs/engenharia-de-dados.md](docs/engenharia-de-dados.md): o que o engenheiro faz com dbt nas camadas
  bronze, silver e gold.

## Testes da imagem tools

```bash
docker compose run --rm --no-deps --entrypoint pytest raw-init -q
```

## Créditos

Dataset Northwind via [pthom/northwind_psql](https://github.com/pthom/northwind_psql) (Microsoft Public License).
Veja `postgres/init/NOTICE`.
