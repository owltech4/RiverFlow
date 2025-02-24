import boto3
import pandas as pd
import psycopg2
import os
import logging

# 🔹 Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# 🔹 Configuração do S3
BUCKET_NAME = "mybucket-digo2"

# 🔹 Configuração do PostgreSQL
pg_host = "localhost"
pg_port = "5432"
pg_user = "postgres"
pg_password = "postgres"
pg_dbname = "postgres"

# 🔹 Lista de arquivos CSV e tabelas correspondentes
csv_files = {
    "categories.csv":"categories_info",
    "cities.csv":"cities_info",
    "customers.csv":"customers",
    "employees.csv": "employees_info",
    "products.csv": "products_info",
    "refined_ecommerce_product_data.csv":"refined_ecommerce_product_data",
    "retail_store_sales.csv":"retail_stores_sales_data",
}

# 🔹 Criar cliente S3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),  # Melhor usar variáveis de ambiente
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="sa-east-1"
)

# 🔹 Criar conexão com PostgreSQL
try:
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        dbname=pg_dbname
    )
    cursor = conn.cursor()
    logging.info("✅ Conexão bem-sucedida com o PostgreSQL!")
except Exception as e:
    logging.error(f"❌ Erro ao conectar ao PostgreSQL: {e}")
    exit()

# 🔹 Loop para processar múltiplos arquivos
for csv_file, table_name in csv_files.items():
    local_file = f"C:/Users/Desktop/Desktop/Studies/data_warehouse/dataset/{csv_file}"

    try:
        # 🔹 Baixar o arquivo do S3
        s3_client.download_file(BUCKET_NAME, csv_file, local_file)
        logging.info(f"✅ Arquivo {csv_file} baixado com sucesso para {local_file}")

        # 🔹 Ler CSV e substituir valores nulos por string vazia
        df = pd.read_csv(local_file).fillna("")
        columns = df.columns.tolist()  # 🔥 Correção: Definir antes de usar

        # 🔹 Criar a tabela dinamicamente
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS public.{table_name} (
            {", ".join(f'"{col}" TEXT' for col in columns)}
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        logging.info(f"✅ Tabela '{table_name}' criada/verificada com sucesso!")

        # 🔹 Preparar SQL para inserção
        columns_escaped = ', '.join([f'"{col}"' for col in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        sql = f"""
            INSERT INTO public.{table_name} ({columns_escaped}) 
            VALUES ({placeholders}) 
            ON CONFLICT DO NOTHING;
        """

        # 🔹 Inserir dados no PostgreSQL
        data = list(df.itertuples(index=False, name=None))
        cursor.executemany(sql, data)
        conn.commit()
        logging.info(f"✅ Inserção concluída na tabela '{table_name}'!")

        # 🔹 Contar registros inseridos
        cursor.execute(f"SELECT COUNT(*) FROM public.{table_name};")
        count = cursor.fetchone()[0]
        logging.info(f"📊 Total de registros na tabela '{table_name}': {count}")

        # 🔹 Remover arquivo temporário
        os.remove(local_file)
        logging.info(f"✅ Arquivo {csv_file} removido após a ingestão!")

    except psycopg2.Error as e:
        conn.rollback()  # 🔥 Corrige erro "current transaction is aborted"
        logging.error(f"❌ Erro ao processar {csv_file}: {e}")

# 🔹 Fechar conexão com PostgreSQL
cursor.close()
conn.close()
logging.info("✅ Conexão com PostgreSQL encerrada.")
