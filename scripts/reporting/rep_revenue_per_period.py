#!/usr/bin/env python
# coding: utf-8

# In[4]:


from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

print('Libraries imported successfully')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Alikaki\Desktop\capstone_project\scripts\project-6ae092ab-63ed-448c-8d3-62884be42593.json"

project_id = 'project-6ae092ab-63ed-448c-8d3'
dataset_id_reporting = 'reporting_db'
dataset_id_staging = 'staging_db'
table_id = 'rep_revenue_per_period'

print("Τα στοιχεία ορίστηκαν σωστά!")

client = bigquery.Client(project=project_id)

# --- SQL QUERY ---
query = f"""
WITH raw_data AS (
    SELECT
        p.payment_date,
        p.amount
    FROM `{project_id}.{dataset_id_staging}.stg_payment` p
    JOIN `{project_id}.{dataset_id_staging}.stg_rental` r ON p.rental_id = r.rental_id
    JOIN `{project_id}.{dataset_id_staging}.stg_inventory` i ON r.inventory_id = i.inventory_id
    JOIN `{project_id}.{dataset_id_staging}.stg_film` f ON i.film_id = f.film_id
    WHERE f.title != 'GOODFELLAS SALUTE'
),
daily_revenue AS (
    SELECT
        'Day' as reporting_period,
        DATE(payment_date) as reporting_date,
        COALESCE(SUM(amount), 0) as total_revenue
    FROM raw_data
    GROUP BY 2
),
monthly_revenue AS (
    SELECT
        'Month' as reporting_period,
        DATE_TRUNC(DATE(payment_date), MONTH) as reporting_date,
        COALESCE(SUM(amount), 0) as total_revenue
    FROM raw_data
    GROUP BY 2
),
yearly_revenue AS (
    SELECT
        'Year' as reporting_period,
        DATE_TRUNC(DATE(payment_date), YEAR) as reporting_date,
        COALESCE(SUM(amount), 0) as total_revenue
    FROM raw_data
    GROUP BY 2
)

SELECT * FROM daily_revenue
UNION ALL
SELECT * FROM monthly_revenue
UNION ALL
SELECT * FROM yearly_revenue
ORDER BY reporting_date DESC
"""

print("Εκτέλεση Query και μεταφορά σε DataFrame...")
df = client.query(query).to_dataframe()


df['total_revenue'] = df['total_revenue'].astype(float)
print("Έγινε μετατροπή του revenue σε float.")
print(df.head())

full_table_id = f"{project_id}.{dataset_id_reporting}.{table_id}"
print(f"Ο πίνακας θα δημιουργηθεί στο: {full_table_id}")

schema = [
    bigquery.SchemaField('reporting_period', 'STRING'),
    bigquery.SchemaField('reporting_date', 'DATE'),
    bigquery.SchemaField('total_revenue', 'FLOAT')
]

job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition="WRITE_TRUNCATE"
)

try:
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result() 
    print(f"✅ ΕΠΙΤΥΧΙΑ! Ο πίνακας {table_id} δημιουργήθηκε.")
except Exception as e:
    print(f"❌ Σφάλμα: {e}")


# In[ ]:




