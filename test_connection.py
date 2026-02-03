from google.cloud import bigquery
import os

# Set this if running locally, otherwise GitHub Action handles it
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/key.json"

def verify_setup():
    PROJECT_ID = 'playstore2026'
    LOCATION = 'asia-south1'
    DATASET_ID = 'play_store_data'
    
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    
    print(f"--- Testing Connection to {LOCATION} ---")
    try:
        # Check if Dataset exists
        dataset = client.get_dataset(DATASET_ID)
        print(f"✅ Dataset '{DATASET_ID}' found in {dataset.location}")
        
        # Check if Table exists
        table_id = f"{PROJECT_ID}.{DATASET_ID}.app_reviews"
        table = client.get_table(table_id)
        print(f"✅ Table 'app_reviews' found with {table.num_rows} rows.")
        
        # Test Job Creation (Permissions check)
        print("Testing Job User permissions...")
        query_job = client.query("SELECT 1", location=LOCATION)
        query_job.result()
        print("✅ BigQuery Job User role is working.")
        
    except Exception as e:
        print(f"❌ Setup Failed: {e}")

if __name__ == "__main__":
    verify_setup()
