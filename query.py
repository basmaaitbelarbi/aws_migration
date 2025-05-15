from parser import * 
from sqlalchemy import text

total_rows = "SELECT COUNT(*) FROM retail"
# ... 

with engine.connect() as conn:
    result = conn.execute(text(total_rows))
    print(result.scalar())