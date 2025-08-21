import os
import pandas as pd
from sqlalchemy import create_engine, text
import requests  # for Slack notifications

# --- Slack Webhook ---
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"  # replace with your webhook

def send_slack_notification(message: str):
    """Send a notification to Slack"""
    try:
        payload = {"text": message}
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        if response.status_code == 200:
            print(f"📢 Slack notification sent: {message}")
        else:
            print(f" Failed to send Slack notification: {response.text}")
    except Exception as e:
        print(f" Slack notification error: {e}")


# --- TiDB Cloud connection details ---
host = "gateway01.ap-southeast-1.prod.aws.tidbcloud.com"
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
    msg = f"Uploaded {csv_path} → table `{table_name}`"
    print(msg)
    send_slack_notification(msg)


# --- Loop through both folders and upload ---
for folder in folders:
    if not os.path.exists(folder):  # check if folder exists
        warning_msg = f" Folder not found: {folder}"
        print(warning_msg)
        send_slack_notification(warning_msg)
        continue  # skip this folder

    for file in os.listdir(folder):
        if file.endswith(".csv"):
            csv_path = os.path.join(folder, file)
            try:
                upload_csv_to_tidb(csv_path, engine)
            except Exception as e:
                error_msg = f"❌ Failed to upload {csv_path}: {e}"
                print(error_msg)
                send_slack_notification(error_msg)

send_slack_notification("🎉 All CSV files upload process completed!")
print("🎉 All CSV files uploaded successfully to TiDB Cloud!")















