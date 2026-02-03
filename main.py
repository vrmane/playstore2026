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
    """Scrapes Play Store reviews and performs a deduplicated IST-aligned merge."""
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    
    # Define "Yesterday" in IST
    now_ist = datetime.now(IST)
    yesterday_ist = (now_ist - timedelta(days=1)).date()
    # Also fetch today's partial data to ensure no gap during midnight runs
    today_ist = now_ist.date()
    
    print(f"--- Starting Scrape for IST Date: {yesterday_ist} (and partial {today_ist}) ---")

    all_new_reviews = []

    for app in APPS:
        print(f"Fetching reviews for {app['name']}...")
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
            
            # The Play Store returns UTC. We convert it to IST for filtering.
            df['at'] = pd.to_datetime(df['at']).dt.tz_localize('UTC').dt.tz_convert(IST)
            df['app_name'] = app['name']
            
            # Filter: IST Date must be yesterday or today, and length >= 30
            mask = (df['at'].dt.date >= yesterday_ist) & (df['content'].str.len() >= 30)
            df_filtered = df[mask].copy()
            
            if not df_filtered.empty:
                # Format for BigQuery (removing timezone offset for clean TIMESTAMP storage)
                df_filtered['at'] = df_filtered['at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                all_new_reviews.append(df_filtered)
                print(f"Found {len(df_filtered)} matching reviews for {app['name']}.")
            else:
                print(f"No new reviews for {app['name']}.")

        except Exception as e:
            print(f"Error scraping {app['name']}: {e}")

    if not all_new_reviews:
        print("No new reviews found in the IST window.")
        return

    # 1. Combine and Local Deduplication
    final_df = pd.concat(all_new_reviews, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=['reviewId'])
    
    # 2. Add IST Ingestion Timestamp
    final_df['review_added_timestamp'] = now_ist.strftime('%Y-%m-%d %H:%M:%S')

    # 3. Upload to Staging Table
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_staging_reviews"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)

    try:
        print(f"Uploading {len(final_df)} reviews to staging...")
        load_job = client.load_table_from_dataframe(final_df, staging_table_id, job_config=job_config)
        load_job.result()
        
        # 4. MERGE: Insert only if reviewId doesn't exist
        merge_sql = f"""
        MERGE `{TABLE_ID}` T
        USING `{staging_table_id}` S
        ON T.reviewId = S.reviewId
        WHEN NOT MATCHED THEN
          INSERT (reviewId, content, score, at, app_name, review_added_timestamp)
          VALUES (S.reviewId, S.content, S.score, S.at, S.app_name, S.review_added_timestamp)
        """
        
        print("Executing deduplicated MERGE...")
        query_job = client.query(merge_sql)
        query_job.result()
        
        # Cleanup
        client.delete_table(staging_table_id, not_found_ok=True)
        print(f"✅ IST Sync Complete: {query_job.num_dml_affected_rows} new reviews added.")
        
    except Exception as e:
        print(f"Workflow Failed: {e}")

if __name__ == "__main__":
    scrape_all_apps()
