import os
from sqlalchemy import create_engine
import pandas as pd

def create_date_table(start='2000-01-01', end='2020-12-31'):
    df = pd.DataFrame({'date': pd.date_range(start, end)})
    df['date_id'] = df.index + 1
    df['year'] = df.date.dt.year
    df['month'] = df.date.dt.month
    df['day'] = df.date.dt.day
    df['day_name'] = df.date.dt.weekday_name
    df['day_week'] = df.date.dt.dayofweek
    df['week'] = df.date.dt.weekofyear
    df['quarter'] = df.date.dt.quarter
    
    df = df[['date_id', 'date', 'year', 'month', 'day', 'day_name', 'day_week', 'week', 'quarter']] 
    
    return df

def create_time_table(start='00:00', end='23:59', freq='1min'):
    
    df = pd.DataFrame(pd.date_range(start, end, freq=freq ), columns=['datetime'])
    df['time'] = df.datetime.dt.time
    df['hour'] = df.datetime.dt.hour
    df['minute'] = df.datetime.dt.minute

    return df


def main():

    PG_HOST = os.environ.get('PGHOST')
    PG_USERNAME = os.environ.get('PGUSERNAME')
    PG_PASSWORD = os.environ.get('PGPASSWORD')
    PG_DATABASE = os.environ.get('PGDATABASE')
   
    engine = create_engine(f'postgresql+psycopg2://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}/{PG_DATABASE}')

    # create track dimension
    media_type_raw = pd.read_sql_table('media_type', con=engine)
    artist_raw = pd.read_sql_table('artist', con=engine)
    track_raw = pd.read_sql_table('track', con=engine)
    genre_raw = pd.read_sql_table('genre', con=engine)
    album_raw = pd.read_sql_table('album', con=engine)

    media_type_raw.rename(columns={'name': 'media_type'}, inplace=True)
    artist_raw.rename(columns={'name': 'artist'}, inplace=True)
    genre_raw.rename(columns={'name': 'genre'}, inplace=True)
    album_raw.rename(columns={'title': 'album'}, inplace=True)

    album = album_raw.merge(artist_raw, on='artist_id')
    track = track_raw.merge(media_type_raw, on='media_type_id')
    track = track.merge(genre_raw, on='genre_id')
    track_dim = track.merge(album, on='album_id')

    track_dim.drop(['album_id', 'media_type_id', 'genre_id', 'artist_id'], axis=1, inplace=True)
    track_dim.sort_values('track_id', inplace=True)

    track_dim.to_sql('track_dim', engine, index=False, method='multi', schema='dwh')

    # create customer dimension
    customer_dim = pd.read_sql_table('customer', con=engine)
    customer_dim.to_sql('customer_dim', engine, index=False, method='multi', schema='dwh')

    # create date dimension
    date_dim = create_date_table()
    date_dim.to_sql('date_dim', engine, index=False, method='multi', schema='dwh')

    # create invoice dimension
    invoice_raw = pd.read_sql_table('invoice', con=engine)
    invoice_dim = invoice_raw.drop(['customer_id','invoice_date'], axis=1)

    invoice_dim.to_sql('invoice_dim', engine, index=False, method='multi', schema='dwh')

    # create invoice_line fact
    invoice_line_raw = pd.read_sql_table('invoice_line', con=engine)

    invoice = pd.merge(invoice_line_raw, invoice_raw[['invoice_id','invoice_date', 'customer_id']], on='invoice_id', how='left')
    invoice = invoice.merge(date_dim[['date', 'date_id']], left_on='invoice_date', right_on='date')
    invoice_fact = invoice[['invoice_line_id', 'invoice_id', 'track_id', 'date_id', 'customer_id', 'unit_price', 'quantity']]

    invoice_fact.to_sql('invoice_fact', engine, index=False, method='multi', schema='dwh')

    print(f'Sucessfully created {PG_DATABASE} data warehouse')
    
if __name__ == '__main__':
    main()
