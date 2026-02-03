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
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)

    now_ist = datetime.now(IST)
    three_days_ago = (now_ist - timedelta(days=3)).date()

    print(f"--- 🚀 Starting Scrape (IST Now: {now_ist}) ---")

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

            optional_cols = [
                'userName', 'userImage', 'thumbsUpCount',
                'reviewCreatedVersion', 'replyContent', 'repliedAt'
            ]
            for col in optional_cols:
                if col not in df.columns:
                    df[col] = None

            # --- Correct timestamp mapping ---
            df['at_timestamp'] = pd.to_datetime(df['at']).dt.tz_localize('UTC').dt.tz_convert(IST)

            df['repliedAt'] = pd.to_datetime(df['repliedAt'], errors='coerce')
            if not df['repliedAt'].isna().all():
                df['repliedAt'] = df['repliedAt'].dt.tz_localize('UTC').dt.tz_convert(IST)

            df['app_name'] = app['name']

            # AI columns (future processing)
            df['ai_processed_timestamp'] = None
            df['ai_output'] = None

            # --- Filtering ---
            mask = (
                (df['at_timestamp'].dt.date >= three_days_ago) &
                (df['content'].str.len() >= 30)
            )

            df_filtered = df[mask].copy()

            if not df_filtered.empty:
                df_filtered['at_timestamp'] = df_filtered['at_timestamp'].dt.tz_localize(None)
                df_filtered['repliedAt'] = df_filtered['repliedAt'].apply(
                    lambda x: x.tz_localize(None) if pd.notnull(x) else None
                )

                all_new_reviews.append(df_filtered)
                print(f"✅ Found {len(df_filtered)} valid reviews for {app['name']}.")

        except Exception as e:
            print(f"❌ Error scraping {app['name']}: {e}")

    if not all_new_reviews:
        print("🛑 No new reviews passed filters.")
        return

    final_df = pd.concat(all_new_reviews, ignore_index=True)
    final_df = final_df.drop_duplicates(subset=['reviewId'])

    final_df['review_added_timestamp'] = now_ist.replace(tzinfo=None)

    final_df['score'] = final_df['score'].astype('Int64')
    final_df['thumbsUpCount'] = final_df['thumbsUpCount'].astype('Int64')

    staging_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_staging_reviews"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("reviewId", "STRING"),
            bigquery.SchemaField("userName", "STRING"),
            bigquery.SchemaField("userImage", "STRING"),
            bigquery.SchemaField("content", "STRING"),
            bigquery.SchemaField("score", "INTEGER"),
            bigquery.SchemaField("thumbsUpCount", "INTEGER"),
            bigquery.SchemaField("reviewCreatedVersion", "STRING"),
            bigquery.SchemaField("at_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("replyContent", "STRING"),
            bigquery.SchemaField("repliedAt", "TIMESTAMP"),
            bigquery.SchemaField("app_name", "STRING"),
            bigquery.SchemaField("review_added_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("ai_processed_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("ai_output", "STRING"),
        ],
    )

    try:
        print(f"📤 Uploading {len(final_df)} reviews to staging...")
        load_job = client.load_table_from_dataframe(
            final_df,
            staging_table_id,
            job_config=job_config
        )
        load_job.result()

        merge_sql = f"""
        MERGE `{TABLE_ID}` T
        USING `{staging_table_id}` S
        ON T.reviewId = S.reviewId
        WHEN NOT MATCHED THEN
          INSERT (
            reviewId, userName, userImage, content, score, thumbsUpCount,
            reviewCreatedVersion, at_timestamp, replyContent, repliedAt,
            app_name, review_added_timestamp,
            ai_processed_timestamp, ai_output
          )
          VALUES (
            S.reviewId, S.userName, S.userImage, S.content, S.score, S.thumbsUpCount,
            S.reviewCreatedVersion, S.at_timestamp, S.replyContent, S.repliedAt,
            S.app_name, S.review_added_timestamp,
            S.ai_processed_timestamp, S.ai_output
          )
        """

        print("🔗 Running MERGE...")
        query_job = client.query(merge_sql)
        query_job.result()

        client.delete_table(staging_table_id, not_found_ok=True)

        print(f"🎉 SUCCESS — inserted {query_job.num_dml_affected_rows} new reviews.")

    except Exception as e:
        print(f"🔥 Pipeline failed: {e}")

if __name__ == "__main__":
    scrape_all_apps()
