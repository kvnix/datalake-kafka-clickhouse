"""Gerador de tráfego: mantém o banco de "produção" (Northwind) mudando o tempo todo.

A cada TRAFFIC_TICK_SECONDS sorteia (por peso) uma ação e a executa em UMA transação:

  new_order     INSERT orders + order_details      (fato crescendo)
  ship_order    UPDATE orders.shipped_date         (mesma PK, nova versão)
  stock         UPDATE products.units_in_stock     (atualização frequente)
  price         UPDATE products.unit_price         (histórico p/ SCD2)
  cancel_order  DELETE order_details + orders      (deletes)
  new_customer  INSERT customers                   (dimensão crescendo)

Antes da primeira ação, espera o slot do Debezium existir: o snapshot do Debezium é o snapshot
exportado na criação do slot, então qualquer escrita posterior vira evento c/u/d e não "vaza"
para os eventos r (snapshot). Isso deixa a contagem de eventos r igual à do dump original.
"""

from __future__ import annotations

import os
import random
import string
import sys
import time
from datetime import datetime

import psycopg

SMALLINT_MAX = 32767


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def log(msg: str) -> None:
    print(f"{datetime.now():%H:%M:%S} [traffic] {msg}", flush=True)


ENABLED = env("TRAFFIC_ENABLED", "true").strip().lower() in ("1", "true", "yes", "sim")
TICK = float(env("TRAFFIC_TICK_SECONDS", "5"))
MAX_ORDER_ID = min(int(env("TRAFFIC_MAX_ORDER_ID", "32000")), SMALLINT_MAX)
SLOT_NAME = env("DEBEZIUM_SLOT_NAME", "debezium_northwind")

WEIGHTS = {
    "new_order": int(env("TRAFFIC_WEIGHT_NEW_ORDER", "30")),
    "ship_order": int(env("TRAFFIC_WEIGHT_SHIP_ORDER", "25")),
    "stock": int(env("TRAFFIC_WEIGHT_STOCK", "20")),
    "price": int(env("TRAFFIC_WEIGHT_PRICE", "10")),
    "cancel_order": int(env("TRAFFIC_WEIGHT_CANCEL_ORDER", "10")),
    "new_customer": int(env("TRAFFIC_WEIGHT_NEW_CUSTOMER", "5")),
}

DSN = (
    f"host={env('POSTGRES_HOST', 'postgres')} port={env('POSTGRES_PORT_INTERNAL', '5432')} "
    f"dbname={env('POSTGRES_DB', 'northwind')} user={env('POSTGRES_USER', 'postgres')} "
    f"password={env('POSTGRES_PASSWORD', 'postgres')}"
)

CONTACTS = ["Ana Souza", "Bruno Lima", "Carla Dias", "Diego Rocha", "Elisa Prado", "Fábio Nunes"]
TITLES = ["Owner", "Sales Manager", "Purchasing Agent", "Accounting Manager"]
PLACES = [  # (cidade, região, CEP, país)
    ("São Paulo", "SP", "01000-000", "Brazil"),
    ("Rio de Janeiro", "RJ", "20000-000", "Brazil"),
    ("Lisboa", None, "1000-001", "Portugal"),
    ("Seattle", "WA", "98101", "USA"),
    ("Berlin", None, "10115", "Germany"),
]


class Skip(Exception):
    """Ação não aplicável agora (ex.: não há pedido pendente). Não é erro."""


# ---------------------------------------------------------------------------
# Ações (cada uma recebe um cursor dentro de uma transação aberta)
# ---------------------------------------------------------------------------


def new_order(cur) -> str:
    cur.execute("SELECT coalesce(max(order_id), 0) + 1 FROM orders")
    order_id = cur.fetchone()[0]
    if order_id > MAX_ORDER_ID:
        raise Skip(
            f"AVISO: próximo order_id={order_id} excede TRAFFIC_MAX_ORDER_ID={MAX_ORDER_ID} "
            "(orders.order_id é smallint); pedidos novos pausados. Use `docker compose down -v` para resetar."
        )

    cur.execute(
        "SELECT customer_id, company_name, address, city, region, postal_code, country "
        "FROM customers ORDER BY random() LIMIT 1"
    )
    customer = cur.fetchone()
    cur.execute("SELECT employee_id FROM employees ORDER BY random() LIMIT 1")
    employee_id = cur.fetchone()[0]
    cur.execute("SELECT shipper_id FROM shippers ORDER BY random() LIMIT 1")
    shipper_id = cur.fetchone()[0]
    cur.execute(
        "SELECT product_id, unit_price FROM products WHERE discontinued = 0 "
        "ORDER BY random() LIMIT %s",
        (random.randint(1, 4),),
    )
    products = cur.fetchall()

    cur.execute(
        """
        INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date,
                            shipped_date, ship_via, freight, ship_name, ship_address, ship_city,
                            ship_region, ship_postal_code, ship_country)
        VALUES (%s, %s, %s, current_date, current_date + 28, NULL, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (order_id, customer[0], employee_id, shipper_id, round(random.uniform(1, 200), 2),
         customer[1], customer[2], customer[3], customer[4], customer[5], customer[6]),
    )
    for product_id, unit_price in products:
        cur.execute(
            "INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) "
            "VALUES (%s, %s, %s, %s, %s)",
            (order_id, product_id, unit_price, random.randint(1, 30),
             random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2])),
        )
    items = ",".join(str(p[0]) for p in products)
    return f"new_order tabela=orders pk={order_id} itens(order_details product_id)={items}"


def ship_order(cur) -> str:
    cur.execute(
        "SELECT order_id FROM orders WHERE shipped_date IS NULL "
        "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED"
    )
    row = cur.fetchone()
    if row is None:
        raise Skip("ship_order: nenhum pedido pendente")
    cur.execute(
        "UPDATE orders SET shipped_date = greatest(current_date, order_date) WHERE order_id = %s",
        (row[0],),
    )
    return f"ship_order tabela=orders pk={row[0]}"


def stock(cur) -> str:
    cur.execute("SELECT product_id FROM products ORDER BY random() LIMIT 1")
    product_id = cur.fetchone()[0]
    delta = random.randint(-20, 40)
    cur.execute(
        "UPDATE products SET units_in_stock = least(greatest(0, coalesce(units_in_stock, 0) + %s), 32000) "
        "WHERE product_id = %s RETURNING units_in_stock",
        (delta, product_id),
    )
    return f"stock tabela=products pk={product_id} units_in_stock={cur.fetchone()[0]} ({delta:+d})"


def price(cur) -> str:
    cur.execute("SELECT product_id FROM products ORDER BY random() LIMIT 1")
    product_id = cur.fetchone()[0]
    factor = 1 + random.uniform(-0.05, 0.10)
    cur.execute(
        "UPDATE products SET unit_price = greatest(0.5, round((unit_price * %s)::numeric, 2)) "
        "WHERE product_id = %s RETURNING unit_price",
        (factor, product_id),
    )
    return f"price tabela=products pk={product_id} unit_price={cur.fetchone()[0]}"


def cancel_order(cur) -> str:
    cur.execute(
        "SELECT order_id FROM orders WHERE shipped_date IS NULL "
        "ORDER BY random() LIMIT 1 FOR UPDATE SKIP LOCKED"
    )
    row = cur.fetchone()
    if row is None:
        raise Skip("cancel_order: nenhum pedido pendente para cancelar")
    cur.execute("DELETE FROM order_details WHERE order_id = %s", (row[0],))
    n_items = cur.rowcount
    cur.execute("DELETE FROM orders WHERE order_id = %s", (row[0],))
    return f"cancel_order tabela=orders pk={row[0]} (order_details removidos={n_items})"


def new_customer(cur) -> str:
    for _ in range(20):
        customer_id = "".join(random.choices(string.ascii_uppercase, k=5))
        cur.execute("SELECT 1 FROM customers WHERE customer_id = %s", (customer_id,))
        if cur.fetchone() is None:
            break
    else:
        raise Skip("new_customer: não achou customer_id livre")
    city, region, postal, country = random.choice(PLACES)
    cur.execute(
        """
        INSERT INTO customers (customer_id, company_name, contact_name, contact_title, address,
                               city, region, postal_code, country, phone, fax)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
        """,
        (customer_id, f"Cliente Lab {customer_id}", random.choice(CONTACTS), random.choice(TITLES),
         f"Rua Exemplo, {random.randint(1, 999)}", city, region, postal, country,
         f"({random.randint(10, 99)}) 5555-{random.randint(1000, 9999)}"),
    )
    return f"new_customer tabela=customers pk={customer_id}"


ACTIONS = {
    "new_order": new_order,
    "ship_order": ship_order,
    "stock": stock,
    "price": price,
    "cancel_order": cancel_order,
    "new_customer": new_customer,
}


# ---------------------------------------------------------------------------
# Loop
# ---------------------------------------------------------------------------


def connect() -> psycopg.Connection:
    while True:
        try:
            return psycopg.connect(DSN, autocommit=True)
        except psycopg.OperationalError as exc:
            log(f"aguardando Postgres ({exc.__class__.__name__})")
            time.sleep(3)


def wait_for_slot(conn: psycopg.Connection) -> None:
    while True:
        row = conn.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT_NAME,)
        ).fetchone()
        if row:
            log(f"slot '{SLOT_NAME}' encontrado: iniciando tráfego")
            return
        log(f"aguardando o slot do Debezium '{SLOT_NAME}'...")
        time.sleep(3)


def run_once(conn: psycopg.Connection, name: str) -> None:
    try:
        with conn.transaction(), conn.cursor() as cur:
            log(ACTIONS[name](cur))
    except Skip as exc:
        log(str(exc))


def main() -> int:
    if not ENABLED:
        log("TRAFFIC_ENABLED=false: gerador desabilitado (processo fica ocioso)")
        while True:
            time.sleep(3600)

    names = [n for n, w in WEIGHTS.items() if w > 0]
    weights = [WEIGHTS[n] for n in names]
    if not names:
        log("ERRO: todos os pesos são 0")
        return 1
    log(f"tick={TICK}s pesos={WEIGHTS} max_order_id={MAX_ORDER_ID}")

    conn = connect()
    wait_for_slot(conn)
    while True:
        name = random.choices(names, weights=weights, k=1)[0]
        try:
            run_once(conn, name)
        except psycopg.OperationalError as exc:
            log(f"ERRO de conexão em {name}: {exc}; reconectando")
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            conn = connect()
        except psycopg.Error as exc:
            # A transação já foi desfeita pelo context manager (rollback)
            log(f"ERRO em {name} (rollback): {exc.__class__.__name__}: {exc}".strip())
        time.sleep(TICK)


if __name__ == "__main__":
    sys.exit(main())
