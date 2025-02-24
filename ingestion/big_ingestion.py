import boto3
import pandas as pd
import psycopg2
import os
import logging

# 🔹 Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# 🔹 S3 Configuration
BUCKET_NAME = "mybucket-digo2"
S3_KEY = "sales.csv"
LOCAL_FILE = "C:/Users/Desktop/Desktop/Studies/data_warehouse/dataset/sales.csv"

# 🔹 PostgreSQL Configuration
pg_host = "localhost"
pg_port = "5432"
pg_user = "postgres"
pg_password = "postgres"
pg_dbname = "postgres"

# 🔹 Create S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="sa-east-1"
)

# 🔹 Download file from S3
try:
    s3_client.download_file(BUCKET_NAME, S3_KEY, LOCAL_FILE)
    logging.info(f"✅ File {S3_KEY} successfully downloaded to {LOCAL_FILE}")
except Exception as e:
    logging.error(f"❌ Error downloading the S3 file: {e}")
    exit()

# 🔹 Establish PostgreSQL connection
try:
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        dbname=pg_dbname
    )
    cursor = conn.cursor()
    logging.info("✅ Successfully connected to PostgreSQL!")
except Exception as e:
    logging.error(f"❌ Error connecting to PostgreSQL: {e}")
    exit()

# 🔹 Create table if it does not exist
create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.sales_data (
        sales_id TEXT PRIMARY KEY,
        sales_person_id TEXT,
        customer_id TEXT,
        product_id TEXT,
        quantity TEXT,
        discount TEXT,
        total_price TEXT,
        sales_current_date TIMESTAMP,
        transaction_number TEXT
    );
"""
cursor.execute(create_table_sql)
conn.commit()
logging.info("✅ Table 'sales_data' created/verified successfully.")

# 🔹 Enable PostgreSQL performance extension (only required the first time)
cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
conn.commit()

# 🔹 Truncate the table before inserting data (optional)
cursor.execute("TRUNCATE TABLE public.sales_data;")
conn.commit()
logging.info("✅ Existing data in the table has been cleared.")

# 🔹 Disable triggers to speed up insertion
cursor.execute("ALTER TABLE public.sales_data DISABLE TRIGGER ALL;")
conn.commit()

# 🔹 Read and insert data in chunks
chunksize = 100000
logging.info(f"🚀 Starting data ingestion in chunks of {chunksize} records...")

try:
    for chunk in pd.read_csv(LOCAL_FILE, chunksize=chunksize):
        chunk.fillna("", inplace=True)  # Replace NaN values

        # ✅ Convert sales_date column to proper datetime format
        chunk["SalesDate"] = pd.to_datetime(chunk["SalesDate"], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce")

        # ✅ Save the processed chunk into a temporary file for COPY
        temp_file = LOCAL_FILE.replace(".csv", "_temp.csv")
        chunk.to_csv(temp_file, index=False, header=False, sep=",")

        with open(temp_file, "r", encoding="utf-8") as f:
            cursor.copy_expert(f"COPY public.sales_data FROM STDIN WITH CSV", f)
            conn.commit()

        os.remove(temp_file)  # Remove temporary file after each chunk
        logging.info(f"✅ {len(chunk)} records inserted into PostgreSQL.")

except Exception as e:
    conn.rollback()
    logging.error(f"❌ Error inserting data: {e}")

# 🔹 Re-enable triggers after insertion
cursor.execute("ALTER TABLE public.sales_data ENABLE TRIGGER ALL;")
conn.commit()

# 🔹 Count inserted records
cursor.execute("SELECT COUNT(*) FROM public.sales_data;")
count = cursor.fetchone()[0]
logging.info(f"📊 Total records in 'sales_data' table: {count}")

# 🔹 Remove the downloaded file
try:
    os.remove(LOCAL_FILE)
    logging.info("✅ Local file has been removed after ingestion!")
except Exception as e:
    logging.warning(f"⚠️ Unable to remove the local file: {e}")

# 🔹 Close connection
cursor.close()
conn.close()
logging.info("✅ PostgreSQL connection closed.")
