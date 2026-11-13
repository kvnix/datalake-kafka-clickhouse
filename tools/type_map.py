"""Mapeamento de tipos Postgres -> ClickHouse para a camada raw.

Regra da raw: o valor gravado deve ser IGUAL ao de produção. Por isso, cada tipo tem duas partes:

* o tipo ClickHouse equivalente ao tipo Postgres;
* a expressão que desfaz a *representação de transporte* do Debezium (JSON), por exemplo
  ``date`` chega como inteiro de dias desde 1970-01-01 e volta a ser data.

Nada aqui faz conversão de NEGÓCIO (ex.: float -> decimal). Isso é trabalho do dbt.

Configuração do Debezium assumida (connect/northwind-cdc.json):
    time.precision.mode=adaptive, binary.handling.mode=base64, decimal.handling.mode=string
"""

from __future__ import annotations

from dataclasses import dataclass


class UnmappedTypeError(ValueError):
    """Tipo Postgres sem mapeamento definido para a raw."""


@dataclass(frozen=True)
class Column:
    table: str
    name: str
    data_type: str  # information_schema.columns.data_type
    nullable: bool
    numeric_precision: int | None = None
    numeric_scale: int | None = None


@dataclass(frozen=True)
class MappedColumn:
    name: str
    ch_type: str  # tipo final da coluna na raw (já com Nullable quando for o caso)
    expr: str  # expressão ClickHouse que extrai o valor do JSON da linha (ROW_ALIAS)


def quote_ident(name: str) -> str:
    """Identificador ClickHouse entre crases."""
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def quote_str(value: str) -> str:
    """Literal string ClickHouse entre aspas simples."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


# tipo Postgres -> (tipo ClickHouse base, tipo usado no JSONExtract, template de conversão)
# No template, {v} é o valor já extraído do JSON (Nullable quando a coluna aceita nulo).
_SIMPLE: dict[str, tuple[str, str, str]] = {
    "smallint": ("Int16", "Int16", "{v}"),
    "integer": ("Int32", "Int32", "{v}"),
    "bigint": ("Int64", "Int64", "{v}"),
    "real": ("Float32", "Float32", "{v}"),
    "double precision": ("Float64", "Float64", "{v}"),
    "boolean": ("Bool", "Bool", "{v}"),
    "character varying": ("String", "String", "{v}"),
    "character": ("String", "String", "{v}"),
    "text": ("String", "String", "{v}"),
    # date: Debezium envia dias desde 1970-01-01 (negativo antes de 1970).
    # Date32 cobre 1900..2299; o Date comum começa em 1970 e quebraria birth_date do Northwind.
    "date": ("Date32", "Int32", "addDays(toDate32('1970-01-01'), {v})"),
    # bytea: Debezium envia base64; a raw guarda os bytes originais.
    "bytea": ("String", "String", "base64Decode({v})"),
    # timestamp sem fuso: com adaptive e precisão padrão (6) chega como microssegundos.
    "timestamp without time zone": ("DateTime64(6)", "Int64", "fromUnixTimestamp64Micro({v})"),
}


# Nome do alias (definido no WITH da MV) que contém o JSON da linha: `after`, ou `before` em deletes.
# Prefixo duplo para não colidir com nomes de colunas de origem.
ROW_ALIAS = "__row"


def map_column(col: Column) -> MappedColumn:
    """Retorna tipo ClickHouse e expressão de extração para uma coluna de origem."""
    key = quote_str(col.name)

    if col.data_type == "numeric":
        if col.numeric_precision is None or col.numeric_scale is None:
            raise UnmappedTypeError(
                f"{col.table}.{col.name}: numeric sem precisão/escala definida não é suportado"
            )
        base = f"Decimal({col.numeric_precision}, {col.numeric_scale})"
        # decimal.handling.mode=string -> o valor chega como texto
        extract_type = "String"
        template = "CAST({v} AS " + ("Nullable(" + base + ")" if col.nullable else base) + ")"
    elif col.data_type in _SIMPLE:
        base, extract_type, template = _SIMPLE[col.data_type]
    else:
        raise UnmappedTypeError(
            f"tipo sem mapeamento: tabela={col.table} coluna={col.name} tipo={col.data_type}"
        )

    if col.nullable:
        ch_type = f"Nullable({base})"
        extract = f"JSONExtract({ROW_ALIAS}, {key}, 'Nullable({extract_type})')"
    else:
        ch_type = base
        extract = f"JSONExtract({ROW_ALIAS}, {key}, '{extract_type}')"

    return MappedColumn(name=col.name, ch_type=ch_type, expr=template.format(v=extract))
