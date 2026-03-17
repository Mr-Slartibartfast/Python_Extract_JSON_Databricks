
import pandas
import json
import requests
from datetime import datetime
from pyspark.sql import functions as F

url = "https://wd5-url" 

user = "username"
pwd = "password"
 

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
raw_path = f"/Volumes/raw/landing/ingest/workday/workday_manual_client_report{ts}.json" # creates a timestamp within the name of the file to ensure uniqueness and prevent overwriting existing files
 

# Download -- streaming
r = requests.get(url, auth=(user, pwd), stream=True, timeout=300)
r.raise_for_status()

 

with open(raw_path, 'wb') as f:
  for chunk in r.iter_content(chunk_size=1024*1024):
    if chunk:
      f.write(chunk) 

print(f"Downloaded to {raw_path}")

 

df_raw = spark.read.option('mode', 'PERMISSIVE').option('multiLine', 'true').json(raw_path)
df_flat = df_raw.select(F.explode("Report_Entry").alias("entry")).select("entry.*")
# display(df_flat.limit(5)) # uncomment to display the first 5 rows of the flattened DataFrame for verification 

 

df_flat.write.mode("overwrite").saveAsTable("dev.bronze.workday_client_report_manual") #  This will write the table, overwriting any existing data in the "dev.bronze.workday_client_report_manual" table. Adjust the mode as needed (e.g., "append" to add to existing data instead of overwriting).