import requests
import pandas as pd
import time
import logging
import os 
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine, text

# setup & configuration
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

url = "https://realty-in-us.p.rapidapi.com/properties/v3/list"

headers = {
    "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY"),
    "X-RapidAPI-Host": "realty-in-us.p.rapidapi.com",
    "Content-Type": "application/json"
}

# Extraction layer
def fetch_all_properties(postal_code, max_pages=5):
    all_houses = []
    offset = 0
    limit = 50

    payload = {
        "limit": limit,
        "offset": offset,
        "postal_code": postal_code,
        "status": ['for_sale'],
        "sort":{
            "direction": "desc",
            "field": "list_date"
                }
    }

    for page in range(max_pages):
        logging.info(f"Fetching page {page + 1} for ZIP {postal_code}...")
        payload["offset"] = offset
        try:
            response = requests.post(url, json=payload, headers=headers)

            if response.status_code == 429: # too many requests
                logging.warning("rate limit hit! Sleeping for 10 sec..")
                time.sleep(10)

            data = response.json()
            results = data.get('data', {}).get ('home_search', {}).get('results',[])

            if not results:
                logging.info("No more results found")
                break

            all_houses.extend(results)
            offset += limit
            time.sleep(1)

        except Exception as e:
            logging.error(f"API Erro {e}")

    return pd.json_normalize(all_houses)

# Transformation layer
def clean_real_estate_data(df_raw):
    if df_raw.empty:
        logging.warning("No data to transform")
        return pd.DataFrame()
    
    logging.info("Starting Data Cleaning")

    # define columns(Schema)
    # cols_to_keep = [
    #     #primary ids
    #     'property_id',
    #     'listing_id',
    #     # listing lifecycle & time intelligence
    #     'list_date',
    #     'last_sold_date',
    #     'last_sold_price',
    #     'price_reduced_amount',
    #     'flags.is_price_reduced',
    #     'flags.is_new_listing',
    #     # Pricing intelligence
    #     'list_price',
    #     'list_price_min',
    #     'list_price_max',
    #     'estimate.estimate',
    #     # location & geo-spatial analytics
    #     'location.address.city',
    #     'location.address.state_code',
    #     'location.address.postal_code',
    #     'location.address.coordinate.lat',
    #     'location.address.coordinate.lon',
    #     # Property characteristics
    #     'description.type',
    #     'description.sub_type',
    #     'description.beds',
    #     'description.baths',
    #     'description.sqft',
    #     'description.lot_sqft',
    #     # market condition signals
    #     'flags.is_new_construction',
    #     'flags.is_foreclosure',
    #     'flags.is_pending',
    #     # listing  quality
    #     'photo_count'
    # ]

    # dynamic schema validation
    required_cols = [
         'property_id',
         'description.sqft',
         'list_price'
    ]

    optional_cols = [
        'listing_id',
        'list_date',
        'last_sold_date',
        'last_sold_price',
        'price_reduced_amount',
        'flags.is_price_reduced',
        'flags.is_new_listing',
        'list_price_min',
        'list_price_max',
        'estimate.estimate',
        'location.address.city',
        'location.address.state_code',
        'location.address.postal_code',
        'location.address.coordinate.lat',
        'location.address.coordinate.lon',
        'description.type',
        'description.sub_type',
        'description.beds',
        'description.baths',
        'description.lot_sqft',
        'flags.is_new_construction',
        'flags.is_foreclosure',
        'flags.is_pending',
        'photo_count'
    ]

    missing_essentials = [col for col in required_cols if col not in df_raw.columns]
    if missing_essentials:
        logging.error(f"Critical schema change detected! Missing")
        return pd.DataFrame()
    
    #filter only if columns exist
    available_cols = [c for c in (required_cols + optional_cols) if c in df_raw.columns]
    df = df_raw[available_cols].copy()

    # Handling missing values
    df = df.dropna(subset=['list_price','description.sqft'])

    # Data type conversion
    df['list_date'] = pd.to_datetime(df['list_date']).dt.tz_localize(None)

    # Calculation part
    df['price_per_sqft'] = df['list_price'] / df['description.sqft']

    # data validation
    df = df[df['list_price'] > 10000]
    df = df[df['description.sqft'] > 100]

    def segment_property(price):
        if price > 2000000: return "Luxury"
        elif price > 1000000: return 'Premium'
        else: return 'Standard'

    df['market_segment'] = df['list_price'].apply(segment_property)

    # we columns won't there it will crash so do this
    df = df.dropna(subset=['location.address.coordinate.lat','location.address.coordinate.lon'])

    ## Geo-spatial clustering prep
    # we round lat/lon to create 'Mini-Neighborhood' clusters
    df['geo_cluster'] = df.apply(
        lambda x: f"{round(x['location.address.coordinate.lat'], 2)}_{round(x['location.address.coordinate.lon'], 2)}", axis=1
    )

    # preparing for incermental loads
    df['processed_at'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    return df

## LOAD to SQL
# DB_URL = "postgresql://postgres:Supabase%402026@localhost:5432/Automate_Real_Estate_DB"
# DB_URL = "postgresql://myuser:mypassword@127.0.0.1:5432/real_estate_db"
# DB_URL = os.getenv("DB_URL")
# engine = create_engine(DB_URL)

def load_to_postgres(df,engine_instance):
    if df.empty:
        logging.warning("No Data to load to Postgres.")
        return
    
    logging.info(f"Loading {len(df)} records to PostgreSQL (UPSERT LOgic)...")

    # 1. We create the properties table if it doesn't exist (Static Data)
    # 2. We create the price_history table (Temporal Data)

    with engine_instance.begin() as conn:
        # creating Tables
        conn.execute(text(""" 
            create table if not exists properties(
                property_id varchar primary key,
                listing_id varchar,
                city varchar,
                state_code varchar,
                zip_code varchar,
                beds float,
                baths float,
                sqft float,
                lot_sqft float,
                prop_type varchar,
                geo_cluster varchar,
                latitude float,
                longitude float
                );
            create table if not exists price_history(
                id serial primary key,
                property_id varchar references properties (property_id),
                list_price decimal,
                price_per_sqft decimal,
                market_segment varchar,
                is_pending boolean,
                is_new_listing boolean,
                scanned_at timestamp default current_timestamp
                );
            """))

        for _, row in df.iterrows():
            #upsert logic
            conn.execute(text("""
                insert into properties (
                    property_id, listing_id, city, state_code, zip_code, beds, baths, sqft, lot_sqft, prop_type, geo_cluster, latitude, longitude
                )
                values (
                    :pid, :lid, :city, :state, :zip,
                    :beds, :baths, :sqft, :lot, :type, :cluster, :lat, :lon  
                        )

                on conflict (property_id) do update set
                    listing_id = EXCLUDED.listing_id,
                    beds = EXCLUDED.beds,
                    baths = EXCLUDED.baths;
            """), {
                "pid": row['property_id'],
                "lid": row.get('listing_id'),
                "city": row.get('location.address.city'),
                "state": row.get('location.address.state_code'),
                "zip": row.get('location.address.postal_code'),
                "beds": row.get('description.beds'),
                'baths': row.get('description.baths'),
                "sqft": row.get('description.sqft'),
                "lot": row.get('description.lot_sqft'),
                "type": row.get('description.type'),
                "cluster": row['geo_cluster'],
                "lat": row['location.address.coordinate.lat'],
                "lon": row['location.address.coordinate.lon']
            })

            # insert into price_history
            conn.execute(text("""
                insert into price_history (
                    property_id, list_price, price_per_sqft, market_segment, is_pending, is_new_listing )
                values (
                    :pid, :price, :psqft, :segment, :pending, :new_listing)
            """), {
                "pid": row['property_id'],
                "price": row['list_price'],
                "psqft": row['price_per_sqft'],
                "segment": row['market_segment'],
                "pending": bool(row.get('flags.is_pending', False)),
                "new_listing": bool(row.get('flags.is_new_listing', False))
            })

# def get_engine():
#     user = "myuser"
#     password = "mypassword"
#     host = "localhost"
#     port = "5432"
#     db_name = "real_estate_db"
#     conn_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
#     return create_engine(conn_url)
# engine = get_engine()

def get_engine():
    conn_url = os.getenv('DB_URL')

    if not conn_url:
        conn_url = "postgresql://myuser:mypassword@127.0.0.1:5432/real_estate_db"
        logging.info("Using Local Docker Database")
    else:
        logging.info("Using CLoud Supabase Database")
    return create_engine(conn_url)

## EXECUTION
if __name__ == "__main__" :
    # step 1 : extarct
    raw_data = fetch_all_properties(postal_code="90004", max_pages=2)

    if not raw_data.empty:
        # step 2 : Transform
        clean_df = clean_real_estate_data(raw_data)

        if not clean_df.empty:
            db_engine = get_engine()
            load_to_postgres(clean_df,db_engine)
            logging.info("Success: data loaded to Postgres")
            clean_df.to_excel("report.xlsx", index=False)
            logging.info(f"Successfully processed {len(clean_df)} properties")
        else:
            logging.error("transformation failed")

       
    else:
        logging.error("No data fetched")


# worked on this
# ✔ Pagination
# ✔ Rate-limit handling
# ✔ Schema validation
# ✔ Dynamic column selection
# ✔ Defensive cleaning
# ✔ Geo clustering
# ✔ Market segmentation