#!/usr/bin/env python
# coding: utf-8

# In[17]:


# Αν τα έχεις εγκαταστήσει ήδη, άσε τα όπως είναι (με το #).
# Αν δεις λάθος "Module not found", σβήσε το # από την αρχή της γραμμής και ξανατρέξε.

get_ipython().system('pip install --upgrade pip')
get_ipython().system('pip install google-cloud-bigquery')
get_ipython().system('pip install pandas-gbq -U')
get_ipython().system('pip install db-dtypes')
get_ipython().system('pip install packaging --upgrade')


# In[31]:


from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

print('Libraries imported successfully')


# In[40]:


import os

# Βάλε το r μπροστά από τα εισαγωγικά για να διαβάσει σωστά τη διαδρομή
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Alikaki\Desktop\capstone_project\scripts\project-6ae092ab-63ed-448c-8d3-62884be42593.json"

# ΔΙΟΡΘΩΣΗ: Το Project ID με βάση το αρχείο σου
project_id = 'project-6ae092ab-63ed-448c-8d3'

dataset_id = 'staging_db'
table_id = 'stg_rental'

print("Τα στοιχεία ορίστηκαν σωστά!")


# In[41]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# -- YOUR CODE GOES BELOW THIS LINE

# Define your SQL query here
query = """
with base as (
  select *
  from `project-6ae092ab-63ed-448c-8d3.pagila_productionpublic.rental` 
  )

, final as (
  select
    rental_id
    , rental_date as rental_date
    , inventory_id as inventory_id
    , customer_id as customer_id
    , return_date as return_date
    , staff_id as staff_id
    , last_update as last_update
  FROM base
  )

select * from final
"""

# -- YOUR CODE GOES ABOVE THIS LINE

# Execute the query and store the result in a dataframe
df = client.query(query).to_dataframe()

# Explore some records
df.head()


# In[42]:


# 1. Ορίζουμε ξανά τις μεταβλητές για σιγουριά (με το σωστό Project ID)
project_id = 'project-6ae092ab-63ed-448c-8d3'
dataset_id = 'staging_db' 
table_id = 'stg_rental' # Το αλλάξαμε σε actor γιατί έχουμε δεδομένα ηθοποιών

# 2. Define the full table ID (Όπως στη φωτογραφία)
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

print(f"Ο πίνακας θα δημιουργηθεί στο: {full_table_id}")

# -- YOUR CODE GOES BELOW THIS LINE
# Define table schema based on the project description

schema = [
    bigquery.SchemaField('rental_id', 'INTEGER'),
    bigquery.SchemaField('rental_date', 'TIMESTAMP'),
    bigquery.SchemaField('inventory_id', 'INTEGER'),
    bigquery.SchemaField('customer_id', 'INTEGER'),
    bigquery.SchemaField('return_date', 'TIMESTAMP'),
    bigquery.SchemaField('staff_id', 'INTEGER'),
    bigquery.SchemaField('last_update', 'TIMESTAMP') 
]

# -- YOUR CODE GOES ABOVE THIS LINE

print("✅ Το σχήμα (Schema) ορίστηκε σωστά!")


# In[43]:


from pandas_gbq import to_gbq # Το χρειάζεσαι για την εντολή to_gbq

# Check if the table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Write the dataframe to the table (overwrite if it exists, create if it doesn't)
if table_exists(client, full_table_id):
    # If the table exists, overwrite it
    destination_table = f"{dataset_id}.{table_id}"
    # Write the dataframe to the table (overwrite if it exists)
    to_gbq(df, destination_table, project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} exists. Overwritten.")
else:
    # If the table does not exist, create it
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result() # Wait for the job to complete
    print(f"Table {full_table_id} did not exist. Created and data loaded.")


# In[ ]:


# Αντικατέστησε το "το_δικό_σου_όνομα.ipynb" με το πραγματικό όνομα του αρχείου σου
get_ipython().system('python -m jupyter nbconvert --to python "stg_rental.ipynb"')


# In[ ]:





# In[ ]:




