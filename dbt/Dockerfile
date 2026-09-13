FROM python:3.11-slim

WORKDIR /dbt

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.12.4 \
    dbt-athena-community==1.11.1 \
    "botocore[crt]"

CMD ["dbt", "--version"]
