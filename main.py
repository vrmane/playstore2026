import os
import pandas as pd
import pytz
from datetime import datetime, timedelta
from google_play_scraper import Sort, reviews
from google.cloud import bigquery

# --- Configuration ---
PROJECT_ID = 'playstore2026'
DATASET_ID = 'play_store_data'
TABLE_NAME = 'app_reviews'
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
LOCATION = 'asia-south1'
IST = pytz.timezone('Asia/Kolkata')

APPS = [
    {"name": "MoneyView", "id": "com.whizdm.moneyview.loans"},
    {"name": "KreditBee", "id": "com.kreditbee.android"},
    {"name": "Navi", "id": "com.naviapp"},
    {"name": "Fibe", "id": "com.earlysalary.android"}
]

def scrape_all_apps():
    """
    Scrapes Google Play Store reviews, filters for recent days (IST),
    and performs a deduplicated MERGE into BigQuery with correct types.
    """
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    
    # Define window in IST
    now_ist = datetime.now(IST)
    three_days_ago = (now_ist - timedelta(days=3)).date()
    
    print(f"--- 🚀 Starting Scrape (IST Now: {now_ist}) ---")
    print(f"--- 📅 Looking for reviews since: {three_days_ago} ---")

    all_new_reviews = []

    for app in APPS:
        print(f"🔍 Fetching reviews for {app['name']}...")
        try:
            result, _ = reviews(
                app['id'],
                lang='en',
                country='in', 
                sort=Sort.NEWEST,
                count=500 
            )

            if not result:
                continue

            df = pd.DataFrame(result)
            
            # Convert to IST for accurate FILTERING
            df['at'] = pd.to_datetime(df['at']).dt.tz_localize('UTC').dt.tz_convert(IST)
            df['app_name'] = app['name']
            
            mask = (df['at'].dt.date >= three_days_ago) & (df['content'].str.len() >= 30)
            df_filtered = df[mask].copy()
            
            if not df_filtered.empty:
                # FIX: Convert 'at' back to STRING to match your BigQuery table schema
                df_filtered['at'] = df_filtered['at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                all_new_reviews.append(df_filtered)
                print(f"✅ Found {len(df_filtered)} valid reviews for {app['name']}.")

        except Exception as e:
            print(f"❌ Error scraping {app['name']}: {e}")

    if not all_new_reviews:
        print("🛑 No new reviews passed the filters.")
        return

    # 1. Combine and Local Deduplication
    final_df = pd.concat(all_new_reviews, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=['reviewId'])
    
    # 2. Add Ingestion Timestamp as a DATETIME OBJECT (for the TIMESTAMP column)
    final_df['review_added_timestamp'] = now_ist.replace(tzinfo=None)

    # 3. Upload to Staging Table
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_staging_reviews"
    
    # FIX: Explicitly tell BigQuery that 'at' is a STRING and 'review_added_timestamp' is a TIMESTAMP
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("at", "STRING"), 
            bigquery.SchemaField("review_added_timestamp", "TIMESTAMP"),
        ],
        autodetect=True 
    )

    try:
        print(f"📤 Uploading {len(final_df)} unique reviews to staging...")
        load_job = client.load_table_from_dataframe(final_df, staging_table_id, job_config=job_config)
        load_job.result()
        
        # 4. MERGE into Main Table
        # Using backticks around `at` just in case
        merge_sql = f"""
        MERGE `{TABLE_ID}` T
        USING `{staging_table_id}` S
        ON T.reviewId = S.reviewId
        WHEN NOT MATCHED THEN
          INSERT (reviewId, content, score, `at`, app_name, review_added_timestamp)
          VALUES (S.reviewId, S.content, S.score, S.at, S.app_name, S.review_added_timestamp)
        """
        
        print("🔗 Executing deduplicated MERGE...")
        query_job = client.query(merge_sql)
        query_job.result()
        
        # Cleanup
        client.delete_table(staging_table_id, not_found_ok=True)
        print(f"🎉 SUCCESS! Ingested {query_job.num_dml_affected_rows} unique reviews.")
        
    except Exception as e:
        print(f"🔥 Workflow Failed: {e}")

if __name__ == "__main__":
    scrape_all_apps()
