# import pandas as pd
# from sqlalchemy import create_engine, text

# # Connection
# DB_URL = "postgresql://myuser:mypassword@localhost:5432/real_estate_db"
# engine = create_engine(DB_URL)

# def run_validation():
#     print("--- DATA VALIDATION REPORT ---")
    
#     with engine.connect() as conn:
#         # 1. Total Property Count
#         count = conn.execute(text("SELECT count(*) FROM properties")).scalar()
#         print(f"✅ Total Properties in DB: {count}")

#         # 2. Market Segment Breakdown
#         print("\n--- Market Segment Breakdown ---")
#         segments = pd.read_sql(text("""
#             SELECT market_segment, count(*), ROUND(AVG(list_price), 2) as avg_price 
#             FROM price_history 
#             GROUP BY market_segment
#         """), conn)
#         print(segments)

#         # 3. Check for Nulls (Integrity Check)
#         nulls = conn.execute(text("SELECT count(*) FROM properties WHERE city IS NULL")).scalar()
#         if nulls == 0:
#             print("\n✅ Data Integrity: No missing cities found.")
#         else:
#             print(f"\n❌ Warning: {nulls} records are missing city names.")

# if __name__ == "__main__":
#     run_validation()


from sqlalchemy import create_engine, text

# Use 127.0.0.1 instead of localhost
engine = create_engine("postgresql://myuser:mypassword@127.0.0.1:5432/real_estate_db")

try:
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS test_connection (id INT);"))
        conn.commit()
        print("🎉 Successfully connected and created a table!")
except Exception as e:
    print(f"Failed to connect: {e}")