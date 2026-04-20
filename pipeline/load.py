from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def load_to_bigquery(df: pd.DataFrame, table_id: str):
    client = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()
    
    print(f"Loaded {job.output_rows} rows to {table_id}")