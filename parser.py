from sqlalchemy import create_engine, Column, Integer, String, Date, Float, MetaData, UniqueConstraint, Table
from params import *

import pandas as pd

# Connection 
engine = create_engine(
    f"postgresql+psycopg2://{USER}:{PASSWORD}@{RDS_ENDPOINT}:{PORT}/{DB_NAME}",
    connect_args={"sslmode": "require"}  # pool_size=10, max_overflow=20
)

df = pd.read_csv("retail_data.csv")
df['Order_ID'] = range(1, len(df) + 1) # primary key

# # Schema definition 
metadata = MetaData()
retail_table = Table(
    "retail",
    metadata,
    Column("Order_ID", String, primary_key=True),  
    Column("Order_Date", Date),
    Column("Ship_Mode", Integer),
    Column("Customer ID", Float),
    Column("Country", String),  
    Column("City", Date),
    Column("State", Integer),
    Column("Postal_Code", Float),
    Column("Region", String),  
    Column("Product_ID", Date),
    Column("Category", Integer),
    Column("Sub_Category", Float),
    Column("Product_Name", String),  
    Column("Sales", Date),
    Column("Quantity", Integer),
    Column("Discount", Float),
    Column("Profit", String),  
    Column("Gender", Date),
    Column("Age", Integer),
    Column("Order_Day", Float),
    Column("Order_Month", String),  
    Column("Order_Year", Date),

    # ... Constraints
    UniqueConstraint("Product_Name", "Order_Date", "Quantity", name="uq_product_order") 
)
metadata.create_all(engine)  # Creates table if not exists

# Data insertion
# df.to_sql(
#     name="retail",
#     con=engine,
#     if_exists="replace",  # Use 'append' for non-initial import
#     index=False,
#     method="multi",  # Faster bulk insert
#     chunksize=1000
# )
