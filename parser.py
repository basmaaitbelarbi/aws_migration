from sqlalchemy import create_engine, Column, Integer, String, Date, Float, MetaData, UniqueConstraint, Table
from params import *

import pandas as pd


def is_duplicate(df, col): 
    return not df[col].is_unique    

# Connection 
engine = create_engine(
    f"postgresql+psycopg2://{USER}:{PASSWORD}@{RDS_ENDPOINT}:{PORT}/{DB_NAME}",
    connect_args={"sslmode": "require"}  # Required for AWS RDS
)

df = pd.read_csv("retail_data.csv")

if is_duplicate(df, "order_id"):
    df = df.drop_duplicates(subset=["order_id"])
    df = df.fillna({"category": "Unknown", "ship_mode": "Standard"})

# Schema definition 
metadata = MetaData()
retail_table = Table(
    "retail",
    metadata,
    Column("order_id", String, primary_key=True),  
    Column("order_date", Date),
    Column("quantity", Integer),
    Column("sales", Float),
    # ... other columns


    # ... Constraints
    # UniqueConstraint("product_name", "order_date", name="uq_product_order") # if we need to add constraints when inserting rows
    
)
metadata.create_all(engine)  # Creates table if not exists

# Data insertion
df.to_sql(
    name="retail",
    con=engine,
    if_exists="append",  # Use 'replace' for initial import
    index=False,
    method="multi",  # Faster bulk insert
    chunksize=1000
)
