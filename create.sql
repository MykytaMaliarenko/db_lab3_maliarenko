DROP TABLE IF EXISTS top_revenue_movie CASCADE;
DROP TABLE IF EXISTS top_revenue_distributor CASCADE;
DROP TABLE IF EXISTS top_revenue_genre CASCADE;

create table top_revenue_genre (
    id SERIAL PRIMARY KEY NOT NULL,
    name varchar(150) NOT NULL,
    movies_number int NOT NULL,
    market_share numeric(5, 3) NOT NULL
);

create table top_revenue_distributor (
    id SERIAL PRIMARY KEY NOT NULL,
    name varchar(150) NOT NULL,
    movies_number int NOT NULL,
    market_share numeric(5, 3) NOT NULL
);

create table top_revenue_movie (
    id SERIAL PRIMARY KEY NOT NULL,
    name varchar(150) NOT NULL,
    rating varchar(50) NOT NULL,
    total_revenue int not null,
    year int not null,
    distributor_id int NOT NULL,
    genre_id int NOT NULL,

    FOREIGN KEY (distributor_id) REFERENCES top_revenue_distributor (id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES top_revenue_genre (id) ON DELETE CASCADE
);