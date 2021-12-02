import psycopg2
import psycopg2.extras
import plotly.graph_objects as go


def years_total_revenue(cur):
    cursor.execute('drop view if exists RevenueByYear')
    cursor.execute(
        """
        create view RevenueByYear as
        select year, total_revenue from top_revenue_movie;
        """
    )
    cur.execute('select * from RevenueByYear;')

    years = []
    total_revenue = []
    for result in cur.fetchall():
        years.append(int(result['year']))
        total_revenue.append(float(result['total_revenue']))

    fig = go.Figure(data=go.Scatter(x=years, y=total_revenue))
    fig.update_layout(
        xaxis_title='year',
        yaxis_title='revenue',
    )
    fig.write_image('years_total_revenue.jpeg')


def companies_by_movies_number(cur):
    cursor.execute('drop view if exists MoviesNumberByMarketShare')
    cursor.execute(
        """
        create view MoviesNumberByMarketShare as
        select movies_number, market_share from top_revenue_distributor;
        """
    )
    cur.execute('select * from MoviesNumberByMarketShare;')

    movies_number = []
    market_share = []
    for result in cur.fetchall():
        movies_number.append(int(result['movies_number']))
        market_share.append(float(result['market_share']))

    fig = go.Figure(data=go.Bar(x=movies_number, y=market_share))
    fig.update_layout(
        xaxis_title='movies number',
        yaxis_title='market share',
    )
    fig.write_image('companies_by_movies_number.jpeg')


def distributors_by_market_share(cur):
    cursor.execute('drop view if exists DistributorsByMarketShare')
    cursor.execute(
        """
        create view DistributorsByMarketShare as
        select name, market_share from top_revenue_distributor;
        """
    )
    cur.execute('select * from DistributorsByMarketShare;')

    name = []
    market_share = []
    for result in cur.fetchall():
        name.append(result['name'])
        market_share.append(float(result['market_share']))

    fig = go.Figure(data=go.Pie(labels=name, values=market_share))
    fig.update_layout(
        xaxis_title='distributor',
        yaxis_title='market share',
    )
    fig.write_image('distributor_by_movies_number.jpeg')


if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname='lab3', user='admin',
        password='secret123', host='localhost'
    )
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        years_total_revenue(cursor)
        companies_by_movies_number(cursor)
        distributors_by_market_share(cursor)

    conn.close()
