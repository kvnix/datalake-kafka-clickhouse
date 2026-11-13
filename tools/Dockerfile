# Imagem usada por dois serviços do compose:
#   raw-init           -> python raw_init.py   (job: cria tópicos ausentes + DDL da raw)
#   traffic-generator  -> python traffic.py    (gera INSERT/UPDATE/DELETE no Northwind)
FROM python:3.12.16-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN useradd --create-home --uid 10001 lab
USER lab

CMD ["python", "raw_init.py"]
