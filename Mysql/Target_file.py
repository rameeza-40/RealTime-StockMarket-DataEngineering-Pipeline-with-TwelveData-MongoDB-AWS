import os
import pandas as pd
from sqlalchemy import create_engine, text

# --- TiDB Cloud connection details ---
host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"  # Example, check in your TiDB Cloud console
port = 4000
user = "QS8VFHkTW1ooE3P.root"
password = "gnZfmmMwFTpiT0D2"
database = "S3_DESTINATION"

# Create engine (TiDB is MySQL-compatible)

engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
    connect_args={
        "ssl": {
            "ssl_ca": r"D:\Guvi_project\certs\isrgrootx1.pem"
        }
    }
)

# --- Your two folders containing CSV files ---
folders = [
    r"D:\Guvi_project\Real-Time-Stock-Market-Data-Engineering-Pipeline-with-TwelveData-MongoDB-AWS-EMR-and-RDS\Glue\Onehour_Data",
    r"D:\Guvi_project\Real-Time-Stock-Market-Data-Engineering-Pipeline-with-TwelveData-MongoDB-AWS-EMR-and-RDS\Glue\Thirty_mins_data"
]

# --- Function to upload CSV to TiDB Cloud ---
def upload_csv_to_tidb(csv_path, engine):
    df = pd.read_csv(csv_path)

    # Use file name (without extension) as table name
    table_name = os.path.splitext(os.path.basename(csv_path))[0]

    # Create table if not exists (basic schema: all VARCHAR)
    with engine.connect() as conn:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])}
        )
        """
        conn.execute(text(create_table_sql))

    # Insert data
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"✅ Uploaded {csv_path} → table `{table_name}`")

# --- Loop through both folders and upload ---
for folder in folders:
    for file in os.listdir(folder):
        if file.endswith(".csv"):
            csv_path = os.path.join(folder, file)
            upload_csv_to_tidb(csv_path, engine)

print("🎉 All CSV files uploaded successfully to TiDB Cloud!")






