import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from google_play_scraper import Sort, reviews
from google.cloud import bigquery

# The list of apps you provided
APPS = [
    {"name": "MoneyView", "id": "com.whizdm.moneyview.loans"},
    {"name": "KreditBee", "id": "com.kreditbee.android"},
    {"name": "Navi", "id": "com.naviapp"},
    {"name": "Fibe", "id": "com.earlysalary.android"}
]

PROJECT_ID = 'your-gcp-project-id'
TABLE_ID = 'your_dataset.play_store_reviews'

def scrape_all_apps():
    client = bigquery.Client()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    all_new_reviews = []

    for app in APPS:
        print(f"Scraping {app['name']}...")
        
        # Scrape latest batch
        result, _ = reviews(
            app['id'],
            lang='en',
            country='in', # Adjusted to India for these specific apps
            sort=Sort.NEWEST,
            count=300
        )

        if not result:
            continue

        df = pd.DataFrame(result)
        df['at'] = pd.to_datetime(df['at'])
        df['app_name'] = app['name'] # Add source app name
        
        # Filter 1: Correct Date (D-1)
        # Filter 2: Length >= 30 chars (to ensure quality)
        mask = (df['at'].dt.date == yesterday) & (df['content'].str.len() >= 30)
        df_filtered = df[mask].copy()
        
        if not df_filtered.empty:
            df_filtered['at'] = df_filtered['at'].astype(str)
            all_new_reviews.append(df_filtered)

    if not all_new_reviews:
        print(f"No qualifying reviews found for {yesterday}.")
        return

    # Combine all apps into one upload
    final_df = pd.concat(all_new_reviews, ignore_index=True)
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    job = client.load_table_from_dataframe(final_df, TABLE_ID, job_config=job_config)
    job.result()
    print(f"Success! Loaded {len(final_df)} total reviews from {len(all_new_reviews)} apps.")

if __name__ == "__main__":
    scrape_all_apps()
