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
    # Initialize client with the specific Mumbai region
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
                count=500  # Adjust based on daily volume
            )

            if not result:
                continue

            df = pd.DataFrame(result)
            df['at'] = pd.to_datetime(df['at'])
            df['app_name'] = app['name']
            
            # Filter for D-1 and quality (length > 30)
            mask = (df['at'].dt.date == yesterday) & (df['content'].str.len() >= 30)
            df_filtered = df[mask].copy()
            
            if not df_filtered.empty:
                # Convert timestamp to string for clean BQ ingestion
                df_filtered['at'] = df_filtered['at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                all_new_reviews.append(df_filtered)
                print(f"Found {len(df_filtered)} reviews.")
            else:
                print(f"No reviews matched criteria for {app['name']}.")

        except Exception as e:
            print(f"Error scraping {app['name']}: {e}")

    if not all_new_reviews:
        print("No new reviews to upload today.")
        return

    # Combine data from all apps
    final_df = pd.concat(all_new_reviews, ignore_index=True)
    
    # BigQuery Load Configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        # Explicitly map the 'at' column if autodetect has issues
        autodetect=True,
    )

    print(f"Uploading {len(final_df)} total reviews to {TABLE_ID}...")

    try:
        job = client.load_table_from_dataframe(
            final_df, 
            TABLE_ID, 
            job_config=job_config,
            location=LOCATION
        )
        job.result()  # Wait for completion
        print("Upload successful!")
    except Exception as e:
        print(f"BigQuery Upload Failed: {e}")

if __name__ == "__main__":
    scrape_all_apps()
