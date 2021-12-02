import csv
import psycopg2.extras
import psycopg2
from dataclasses import dataclass, fields
from typing import ClassVar, Any


@dataclass
class AbstractModel:
    TABLE_NAME: ClassVar[str]

    @staticmethod
    def default_sql_repr(value: Any) -> str:
        if isinstance(value, str):
            return f"'{value}'"
        else:
            return str(value)

    def save_sql(self) -> str:
        column_names = [field.name for field in fields(self.__class__) if field.name != 'id']
        values = [self.default_sql_repr(getattr(self, column)) for column in column_names]
        return f'insert into {self.TABLE_NAME} ({",".join(column_names)}) values ({",".join(values)})'


@dataclass
class Genre(AbstractModel):
    name: str
    movies_number: int
    market_share: float

    TABLE_NAME = 'top_revenue_genre'


@dataclass
class Distributor(AbstractModel):
    name: str
    movies_number: int
    market_share: float

    TABLE_NAME = 'top_revenue_distributor'


@dataclass
class Movie(AbstractModel):
    name: str
    rating: str
    total_revenue: int
    year: int
    distributor_id: int
    genre_id: int

    TABLE_NAME = 'top_revenue_movie'


def import_genre(cur):
    with open("raw_data/TopGenres.csv") as csv_file:
        for row in csv.DictReader(csv_file):
            name = row['GENRES']
            movies_number = row['MOVIES']
            if ',' in movies_number:
                movies_number = movies_number.replace(',', '')
            movies_number = int(movies_number)
            market_share = float(row['MARKET SHARE'].replace('%', ''))

            cur.execute(Genre(name, movies_number, market_share).save_sql())


def import_distributors(cur):
    with open("raw_data/TopDistributors.csv") as csv_file:
        for row in csv.DictReader(csv_file):
            name = row['DISTRIBUTORS']
            movies_number = int(row['MOVIES'])
            market_share = float(row['MARKET SHARE'].replace('%', ''))

            cur.execute(Distributor(name, movies_number, market_share).save_sql())


def import_movies(cur):
    with open("raw_data/HighestGrossers.csv") as csv_file:
        for row in csv.DictReader(csv_file):
            name = row['MOVIE']
            rating = row['MPAA RATING']
            year = int(row['\ufeffYEAR'])
            total_revenue = int(row['TOTAL IN 2019 DOLLARS'].replace('$', '').replace(',', ''))

            genre = row['GENRE']
            if not genre:
                continue
            cur.execute('select id from top_revenue_genre where name = %s', (genre,))
            genre_id = cur.fetchone()[0]

            distributor = row['DISTRIBUTOR']
            cur.execute('select id from top_revenue_distributor where name = %s', (distributor,))
            distributor_id = cur.fetchone()[0]

            cur.execute(
                Movie(
                    name=name, rating=rating, year=year,
                    total_revenue=total_revenue, genre_id=genre_id,
                    distributor_id=distributor_id
                ).save_sql()
            )


if __name__ == '__main__':
    conn = psycopg2.connect(
        dbname='lab_2', user='admin',
        password='12345', host='localhost'
    )

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        import_genre(cursor)
        conn.commit()

        import_distributors(cursor)
        conn.commit()

        import_movies(cursor)
        conn.commit()
    conn.close()
