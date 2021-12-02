import psycopg2.extras
import psycopg2
import json
from models import Genre, Distributor, Movie

MODELS = [Genre, Distributor, Movie]


conn = psycopg2.connect(
    dbname='lab3', user='admin',
    password='secret123', host='localhost'
)

for model in MODELS:
    with open(f'json_export/{model.TABLE_NAME}.json', 'w') as f:
        json.dump(model.export_all_as_json(conn), f, indent=4, default=str)

conn.close()
