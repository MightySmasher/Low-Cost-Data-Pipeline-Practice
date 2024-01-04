# Low-Cost-Data-Pipeline-Practice
 data pipeline to get data of "fees paid in usd" from defillama and upload it to bq, using cloud function, cloud pub/sub, and cloud scheduler.

## Data Pipeline Overview
Cloud Scheduler >> Pub/Sub >> Cloud Function 1 >> GCS >> Cloud Function 2 >> BQ

## Setup
### #1 Create empty bucket in GCS.
- Free 5 GB/months of storage is available in us-east1, us-west1, and us-central1 regions only.

### #2 Create empty table in BQ with the same as your GCS and set schema as below. 

- timestamp     <DATE> 
- defillamaId   <INTERGER>
- displayName   <STRING>
- module        <STRING>
- category      <STRING>
- portocolType  <STRING>
- chain         <STRING>
- fees          <FLOAT>

~ Also, set patitioning to "timestamp".


### #3 Create first Cloud Function (api-to-gcs) with the following configs.

####  - Basics
- Environment = 1st gen
- Function name = <your function name>
- Region = SAME AS YOUR GCS WOULD BE BEST!!

#### - Trigger
- Trigger type = Cloud Pub/Sub
- Cloud Pub/Sub topic = CREATE NEW TOPIC AND USE IT!
- Click SAVE

#### - Runtime
- Memory allocated = 512 Mib
- Timeout = 240 seconds
- Runtime environment variables = SEE .ENV file for this configs.
- Click NEXT

#### - Code
- Runtime = python 3.10 or newer
- Entry point = api-to-gcs
- main.py = COPY THE CODE FROM "defillama-api-to-gcs.py" TO "main.py"
- requirements.txt = SEE "requirements.txt"
- CLICK DEPLOY

### #4 Create second Cloud Function (gcs-to-bq) with the following configs.

####  - Basics
- Environment = 1st gen
- Function name = <your function name>
- Region = SAME AS YOUR GCS WOULD BE BEST!!

#### - Trigger
- Trigger type = Cloud Storage
- Event type = On (finalizing/creating) file in the selected bucket
- Bucket = <your created bucket>
- Click SAVE

#### - Runtime
- Memory allocated = 512 Mib
- Timeout = 120 seconds
- Runtime environment variables = SEE .ENV file for this configs.
- Click NEXT

#### - Code
- Runtime = python 3.10 or newer
- Entry point = gcs-to-bq
- main.py = COPY THE CODE FROM "defillama-gcs-to-bq.py" TO "main.py"
- requirements.txt = SEE "requirements.txt"
- CLICK DEPLOY

### #5 Create Cloud Scheduler with the following configs.

####  - Define the schedule
- Region = SAME AS YOUR GCS WOULD BE BEST!!
Frequency = 0 12 * * * #at 12:00PM
- Timezone = Coordinated Universal Time (UTC)
- CLICK CONTINUE

####  - Configure the execution
- Target type = Pub/Sub
- Select a Cloud Pub/Sub topic = <your topic>
- Message body = 1
- CLICK CREATE
