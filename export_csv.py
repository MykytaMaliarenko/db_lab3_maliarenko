import psycopg2.extras
import psycopg2
import csv
from models import Genre, Distributor, Movie

MODELS = [Genre, Distributor, Movie]


conn = psycopg2.connect(
    dbname='lab3', user='admin',
    password='secret123', host='localhost'
)

for model in MODELS:
    with open(f'csv_export/{model.TABLE_NAME}.csv', 'w') as f:
        headers, values = model.export_all_as_csv(conn)
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(values)

conn.close()
