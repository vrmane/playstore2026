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
    and performs a deduplicated MERGE into BigQuery.
    """
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    
    # Define a 3-day lookback window in IST to ensure no data gap
    now_ist = datetime.now(IST)
    three_days_ago = (now_ist - timedelta(days=3)).date()
    
    print(f"--- 🚀 Starting Scrape (IST Now: {now_ist}) ---")
    print(f"--- 📅 Looking for reviews since: {three_days_ago} ---")

    all_new_reviews = []

    for app in APPS:
        print(f"🔍 Fetching reviews for {app['name']}...")
        try:
            # Scrape latest 500 reviews
            result, _ = reviews(
                app['id'],
                lang='en',
                country='in', 
                sort=Sort.NEWEST,
                count=500 
            )

            if not result:
                print(f"⚠️ No raw data returned for {app['name']}")
                continue

            df = pd.DataFrame(result)
            
            # Convert Play Store UTC timestamps to IST for accurate filtering
            df['at'] = pd.to_datetime(df['at']).dt.tz_localize('UTC').dt.tz_convert(IST)
            df['app_name'] = app['name']
            
            # Filter: Date >= 3 days ago AND content length >= 30 chars
            mask = (df['at'].dt.date >= three_days_ago) & (df['content'].str.len() >= 30)
            df_filtered = df[mask].copy()
            
            if not df_filtered.empty:
                # Format timestamp for BigQuery (remove timezone info for cleaner storage)
                df_filtered['at'] = df_filtered['at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                all_new_reviews.append(df_filtered)
                print(f"✅ Found {len(df_filtered)} valid reviews for {app['name']}.")
            else:
                print(f"ℹ️ Found reviews, but 0 matched the date/length filter.")

        except Exception as e:
            print(f"❌ Error scraping {app['name']}: {e}")

    if not all_new_reviews:
        print("🛑 No new reviews passed the filters. Ending job.")
        return

    # 1. Combine and Local Deduplication
    final_df = pd.concat(all_new_reviews, ignore_index=True)
    # Remove duplicates within this batch based on reviewId
    final_df = final_df.drop_duplicates(subset=['reviewId'])
    
    # Add ingestion timestamp
    final_df['review_added_timestamp'] = now_ist.strftime('%Y-%m-%d %H:%M:%S')

    # 2. Upload to Staging Table
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_staging_reviews"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)

    try:
        print(f"📤 Uploading {len(final_df)} unique reviews to staging table...")
        load_job = client.load_table_from_dataframe(
            final_df, 
            staging_table_id, 
            job_config=job_config
        )
        load_job.result()
        
        # 3. MERGE into Main Table
        # CRITICAL FIX: Added backticks around `at` because it is a reserved keyword in SQL.
        merge_sql = f"""
        MERGE `{TABLE_ID}` T
        USING `{staging_table_id}` S
        ON T.reviewId = S.reviewId
        WHEN NOT MATCHED THEN
          INSERT (reviewId, content, score, `at`, app_name, review_added_timestamp)
          VALUES (S.reviewId, S.content, S.score, S.at, S.app_name, S.review_added_timestamp)
        """
        
        print("🔗 Merging unique reviews into main table...")
        query_job = client.query(merge_sql)
        query_job.result()
        
        # Cleanup staging table
        client.delete_table(staging_table_id, not_found_ok=True)
        
        print(f"🎉 SUCCESS! Added {query_job.num_dml_affected_rows} new unique reviews to BigQuery.")
        
    except Exception as e:
        print(f"🔥 BigQuery Workflow Failed: {e}")

if __name__ == "__main__":
    scrape_all_apps()
