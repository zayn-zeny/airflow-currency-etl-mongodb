FROM astrocrpublic.azurecr.io/runtime:3.3-12

# Meta details
LABEL description="Airflow ETL pipeline: Frankfurter API -> MongoDB"
LABEL network="airflow (shared Docker network with mongo and mongo-express)"
LABEL persistent-storage="MongoDB data stored in named volume currency_mongo_data"
