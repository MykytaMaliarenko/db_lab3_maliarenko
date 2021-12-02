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