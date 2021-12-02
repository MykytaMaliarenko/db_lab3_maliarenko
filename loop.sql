DO $$
DECLARE
    genre_name top_revenue_genre.name%TYPE;
    movies_number top_revenue_genre.movies_number%TYPE;

BEGIN
    genre_name := 'my genre ';
    movies_number := 10;
    FOR counter IN 1..5
        LOOP
            INSERT INTO top_revenue_genre(name, movies_number, market_share)
            VALUES (genre_name || counter, movies_number * counter, 0);
        END LOOP;
END
$$