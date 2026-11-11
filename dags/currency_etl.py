"""Daily ETL: Frankfurter API -> transform -> MongoDB, passing data via named XCom keys."""
import os
from datetime import datetime, timezone

import requests
from airflow.sdk import dag, task

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE = "EUR"
QUOTES = "USD,GBP,JPY"


@dag(
    dag_id="currency_etl",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["etl", "currency"],
)
def currency_etl():
    @task
    def extract(ti=None):
        resp = requests.get(API_URL, params={"base": BASE, "quotes": QUOTES}, timeout=30)
        resp.raise_for_status()
        ti.xcom_push(key="raw_rates", value=resp.json())

    @task
    def transform(ti=None):
        raw = ti.xcom_pull(task_ids="extract", key="raw_rates")
        retrieved_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()
        # v2 returns a list of {date, base, quote, rate}; also handle the older dict shape.
        if isinstance(raw, list):
            rows = [(r["date"], r["base"], r["quote"], r["rate"]) for r in raw]
        else:
            rows = [(raw["date"], raw["base"], q, v) for q, v in raw["rates"].items()]
        records = [
            {
                "base_currency": base,
                "target_currency": quote,
                "exchange_rate": rate,
                "rate_date": date,
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            for date, base, quote, rate in rows
        ]
        ti.xcom_push(key="transformed_rates", value=records)

    @task
    def load(ti=None):
        from pymongo import MongoClient

        records = ti.xcom_pull(task_ids="transform", key="transformed_rates")
        client = MongoClient(os.environ["MONGO_URI"])
        col = client["currency_db"]["exchange_rates"]
        for r in records:
            col.update_one(
                {"base_currency": r["base_currency"], "target_currency": r["target_currency"],
                 "rate_date": r["rate_date"]},
                {"$set": r},
                upsert=True,
            )
        print(f"Loaded {len(records)} records")

    extract() >> transform() >> load()


currency_etl()
