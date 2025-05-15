from parser import * 
from sqlalchemy import text

total_rows = "SELECT COUNT(*) FROM retail"
# ... 

with engine.connect() as conn:
    result = conn.execute(text("GRANT CONNECT ON DATABASE retailDB TO postgres;"))
    print(result.scalar())