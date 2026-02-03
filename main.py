import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from google_play_scraper import Sort, reviews
from google.cloud import bigquery

# --- Configuration ---
PROJECT_ID = 'playstore2026'
DATASET_ID = 'play_store_data'
TABLE_NAME = 'app_reviews'
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
LOCATION = 'asia-south1'

APPS = [
    {"name": "MoneyView", "id": "com.whizdm.moneyview.loans"},
    {"name": "KreditBee", "id": "com.kreditbee.android"},
    {"name": "Navi", "id": "com.naviapp"},
    {"name": "Fibe", "id": "com.earlysalary.android"}
]

def scrape_all_apps():
    """Scrapes Play Store reviews and performs a deduplicated merge into BigQuery."""
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    
    # Define D-1 (Yesterday) in UTC
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    print(f"--- Starting Scrape for {yesterday} ---")

    all_new_reviews = []

    for app in APPS:
        print(f"Fetching reviews for {app['name']}...")
        try:
            # Scrape latest reviews
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
            df['at'] = pd.to_datetime(df['at'])
            df['app_name'] = app['name']
            
            # Filter for Yesterday and quality (length >= 30)
            mask = (df['at'].dt.date == yesterday) & (df['content'].str.len() >= 30)
            df_filtered = df[mask].copy()
            
            if not df_filtered.empty:
                # Convert timestamp to string for BQ ingestion
                df_filtered['at'] = df_filtered['at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                all_new_reviews.append(df_filtered)
                print(f"Found {len(df_filtered)} reviews for {app['name']}.")
            else:
                print(f"No reviews matched criteria for {app['name']}.")

        except Exception as e:
            print(f"Error scraping {app['name']}: {e}")

    if not all_new_reviews:
        print("No new reviews to upload today.")
        return

    # 1. Combine and Local Deduplication
    final_df = pd.concat(all_new_reviews, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=['reviewId'])
    
    # 2. Add Ingestion Timestamp
    final_df['review_added_timestamp'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    # 3. Upload to Staging Table
    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_staging_reviews"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)

    print(f"Uploading {len(final_df)} unique reviews to staging...")
    try:
        load_job = client.load_table_from_dataframe(final_df, staging_table_id, job_config=job_config)
        load_job.result()
        
        # 4. MERGE: Only insert reviews that do NOT exist in the main table
        # This keeps counts accurate (e.g., your 261 unique reviews)
        merge_sql = f"""
        MERGE `{TABLE_ID}` T
        USING `{staging_table_id}` S
        ON T.reviewId = S.reviewId
        WHEN NOT MATCHED THEN
          INSERT (reviewId, content, score, at, app_name, review_added_timestamp)
          VALUES (S.reviewId, S.content, S.score, S.at, S.app_name, S.review_added_timestamp)
        """
        
        print("Merging unique reviews into main table...")
        query_job = client.query(merge_sql)
        query_job.result()
        
        # Cleanup staging
        client.delete_table(staging_table_id, not_found_ok=True)
        
        print(f"✅ Successfully ingested {query_job.num_dml_affected_rows} new unique reviews.")
        
    except Exception as e:
        print(f"Workflow Failed: {e}")

if __name__ == "__main__":
    scrape_all_apps()
