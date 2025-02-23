import boto3
import pandas as pd
import psycopg2
import os
import logging


# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Config parameters
BUCKET_NAME = "mybucket-digo2"
S3_KEY = "netflix_titles.csv"
LOCAL_FILE = "C:/Users/Desktop/Desktop/Studies/data_warehouse/dataset/netflix_titles.csv"

# PostgreSQL's config
pg_host = "localhost"
pg_port = "5432"
pg_user = "postgres"
pg_password = "postgres"
pg_dbname = "postgres"

# Create a client S3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="sa-east-1"
)

# 🔹 Download the file .csv from S3
try:
    s3_client.download_file(BUCKET_NAME, S3_KEY, LOCAL_FILE)
    logging.info(f"✅ File {S3_KEY} successfully downloaded to {LOCAL_FILE}")
except Exception as e:
    logging.error(f"❌ Error during the download of the S3's file: {e}")
    exit()

# Create connection with PostgreSQL
conn = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    user=pg_user,
    password=pg_password,
    dbname=pg_dbname
)

cursor = conn.cursor()
cursor.execute("DELETE FROM public.movies_netflix")
# Criar a tabela se não existir
cursor.execute("""
    CREATE TABLE IF NOT EXISTS public.movies_netflix (
        show_id TEXT PRIMARY KEY,
        type TEXT,
        title TEXT,
        director TEXT,
        "cast" TEXT,
        country TEXT,
        date_added TEXT,
        release_year TEXT,
        rating TEXT,
        duration TEXT,
        listed_in TEXT,
        description TEXT
    );
""")
logging.info("✅ Tabela 'movies_netflix' criada/verificada com sucesso.")
# Ingestion file block
# 🔹 Read the CSV
df = pd.read_csv(LOCAL_FILE)

# Usando executemany() para Inserção em Lote
"""
✔ Vantagem: Mais eficiente que um loop com .execute(), pois insere vários registros de uma vez.
✔ Desvantagem: Ainda pode ser um pouco mais lento para arquivos muito grandes (~1 milhão de linhas).
"""

# 🔹 Criar lista de tuplas com os valores do DataFrame
data = list(df.itertuples(index=False, name=None))

# 🔹 Comando SQL sem placeholders (%s)
try:
    sql = """
        INSERT INTO public.movies_netflix (show_id, type, title, director, "cast", country, date_added, release_year, rating, duration, listed_in, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (show_id) DO NOTHING;
    """
    cursor.executemany(sql, data)
    conn.commit()
    logging.info("✅ Inserção concluída com executemany()!")
except Exception as e:
    logging.error(f"❌ Erro ao inserir dados no PostgreSQL: {e}")

# 🔹 Teste se os dados foram inseridos
cursor.execute("SELECT COUNT(*) FROM public.movies_netflix;")
count = cursor.fetchone()[0]
logging.info(f"📊 Total de registros na tabela: {count}")


"""
for _, row in df.iterrows():
    cursor.execute(
        "INSERT INTO movies_netflix (transaction_id, sender_account, receiver_account, transaction_amount, transaction_date) VALUES (%s, %s, %s, %s, %s)",
        (row["Transaction ID"], row["Sender Account ID"], row["Receiver Account ID"], row["Transaction Amount"], row["Transaction Date"])
    )

conn.commit()
cursor.close()
conn.close()
"""

# 🔹 Remover arquivo temporário
try:
    os.remove(LOCAL_FILE)
    logging.info("✅ Arquivo local removido!")
except Exception as e:
    logging.warning(f"⚠️ Não foi possível remover o arquivo local: {e}")
