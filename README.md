# Airflow Currency ETL → MongoDB

Daily ETL built with Astro CLI / Apache Airflow. It pulls EUR exchange rates (USD, GBP, JPY) from the [Frankfurter API](https://frankfurter.dev/), transforms them, and upserts them into MongoDB. Mongo Express is included as the web admin UI.

## Pipeline (`dags/currency_etl.py`)

| Task | What it does | XCom key |
|------|--------------|----------|
| `extract` | GET `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY` | `raw_rates` |
| `transform` | Reshapes the response into one document per currency pair | `transformed_rates` |
| `load` | Upserts into `currency_db.exchange_rates` (key: base, target, rate_date) | – |

Data moves between tasks only through named XComs (`ti.xcom_push` / `ti.xcom_pull` with explicit keys).

Output document:

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1202,
  "rate_date": "2026-11-11",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-11T13:08:22"
}
```

## Docker services (`docker-compose.override.yml`)

- **mongo** (`mongo:7`): primary database, port 27017
- **mongo-express** (`mongo-express:1.0.2`): admin UI at http://localhost:8081
- **Networking:** both join the `airflow` network so the DAG reaches Mongo at `mongo:27017`
- **Persistent storage:** named volume `currency_mongo_data` mounted at `/data/db`
- Metadata is attached as image/container `labels` (description, network, persistent storage)

## Run

Requires Docker and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).

1. Create `.env` (optional; defaults are `admin` / `admin123`):
   ```
   MONGO_USER=admin
   MONGO_PASSWORD=admin123
   MONGO_URI=mongodb://admin:admin123@mongo:27017/
   ```
   `MONGO_URI` is required by the DAG's `load` task.
2. `astro dev start`
3. Open the Airflow UI (URL printed by Astro), unpause and trigger `currency_etl`.
4. Check results in Mongo Express at http://localhost:8081 → `currency_db` → `exchange_rates`.

Local credentials are for development only.
