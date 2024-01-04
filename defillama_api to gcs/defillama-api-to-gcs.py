from google.cloud import storage
import base64
import os
import io
import json
import datetime
import pandas as pd
import requests

# Input your Config in runtime environment setting when creating cloud function
class Config:
    url = os.environ.get('url')
    bucket_name = os.environ.get('bucket_name')
    destination_blob_name = os.environ.get('destination_blob_name')

# Read file in your bucket
def read_blob():
    storage_client = storage.Client()

    # bucket_name is your bucket's name
    bucket_name = Config.bucket_name
    bucket = storage_client.bucket(bucket_name)

    # destination_blob_name is the file's name that is stored/or will be stored in your bucket
    destination_blob_name = Config.destination_blob_name
    blob = bucket.blob(destination_blob_name)

    # read file with string datatype
    with blob.open("r") as f:
        data = f.read()
    
    # Convert string to dataframe
    df = pd.read_csv(io.StringIO(data), encoding="utf-8", sep=",")
    return df

# Will be use in get_historical_data() and get_daily_data()
def get_available_chain():
    response = requests.get(Config.url).text
    chains = json.loads(response)
    ls_chain = chains['allChains']
    print(type(ls_chain),'# of available chains:',len(ls_chain))
    return ls_chain

# Will be use in get_data()
def get_historical_data():
    print('listing available chains')
    ls_chain = get_available_chain()

    print("retriving historical data..")
    df_fees = pd.DataFrame()
    for i in ls_chain:
        # Extract
        try:
            destination = requests.get(Config.url+i).json()
        except:
            pass
        
        # Transform 
        try:
            protocols = pd.DataFrame(destination['protocols'])
            protocols_info = protocols[['defillamaId','displayName','module','category','protocolType']]

            totalDataChartBreakdown = pd.DataFrame(destination['totalDataChartBreakdown'],columns=['timestamp','dict'])            
            normalized = pd.json_normalize(totalDataChartBreakdown['dict'])
            normalized['timestamp'] = totalDataChartBreakdown['timestamp'].apply(lambda x : datetime.datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d'))
            transformed = pd.melt(normalized, id_vars=['timestamp'], value_vars=normalized.columns, var_name='displayName', value_name='fees')
            
            transformed['chain'] = i
            merged = transformed.merge(protocols_info,how='left', left_on='displayName', right_on='displayName').sort_values(by='timestamp')
            df_fees = pd.concat([df_fees,merged])
            df_fees = df_fees[['timestamp','defillamaId','displayName','module','category','protocolType','chain','fees']]
        except:
            pass
    return df_fees

# Will be use in get_data()
def get_daily_data():
    print('listing available chains')
    ls_chain = get_available_chain()
    
    print('retriving data..')
    new_batch = pd.DataFrame()
    for i in ls_chain:
        # Extract
        try:
            response = requests.get(str(Config.url+i)).json()
        except:
            pass
        
        # Transform
        try:
            new_records = pd.DataFrame(response['protocols'])
            new_records['timestamp'] = datetime.datetime.today().astimezone(datetime.timezone.utc).strftime('%Y-%m-%d')
            new_records['chain'] = i
            new_records.rename(columns={'dailyFees':'fees'}, inplace=True)
            new_records = new_records[['timestamp','defillamaId','displayName','module','category','protocolType','chain','fees']]
            new_batch = pd.concat([new_batch,new_records])
        except:
            pass
    return new_batch

def get_data():
    # Check if file is already in your bucket or not
    try:
        df = read_blob()
        
    # If not. get historical data from defillama api
    except:
        print("file is missing.. start getting historical data")
        create_new_file = get_historical_data()
        print('completed!!')
        return create_new_file
    
    # If yes. Check if the data is up to date
    try:
        current_date = datetime.datetime.now().astimezone(datetime.timezone.utc)
        last_updated = datetime.datetime.strptime(max(df.timestamp), '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
        
        date_diff = pd.to_datetime(current_date) - pd.to_datetime(last_updated)
        date_diff = date_diff.days

        print("current date : "+str(current_date))
        print("last updated : "+last_updated)
    
    # If up to date
        if date_diff <= 1:
            print('file has already up to date!! please waiting for DefiLlama to update at 00.00UTC')
            pass
    
    # If not up to date. Daily update
        elif date_diff == 2:
            
            print('updating...daily data')
            daily_data = get_daily_data()
            daily_updated = pd.concat([df,daily_data])

            print('complete!!')
            return daily_updated     

    # If not up to date with missing update    
        elif date_diff > 2:
            print(f'missing more than {date_diff} days')
            his_data = get_historical_data()
            missing_data = his_data[his_data['timestamp'] > last_updated]

            if len(missing_data) == 0:
                pass
            else:
                missing_updated = pd.concat([df,missing_data])
                print('complete!!')
                return missing_updated
    except:
        pass

# Upload file to the bucket
def upload_blob():
    storage_client = storage.Client()

    # bucket_name is your bucket's name
    bucket_name = Config.bucket_name
    bucket = storage_client.bucket(bucket_name)

    # destination_blob_name is the file's name that is stored/or will be stored in your bucket
    destination_blob_name = Config.destination_blob_name
    blob = bucket.blob(destination_blob_name)

    try:
        # get data from destination api
        api_data = get_data()

        # Upload file (when .to_csv(index=False, encoding='utf-8') the datatype become string)
        source_file_name = api_data.to_csv(index=False, encoding='utf-8')
        
        # Uploads file to the bucket.
        blob.upload_from_string(source_file_name, if_generation_match=None,content_type='text/csv')

        print(f"File {source_file_name} uploaded to {destination_blob_name}.")
    except:
        print('Error getting data or file has already up to date!! please waiting for DefiLlama to update at 00.00UTC')

def api_to_gcs(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print("pubsub message type :" + str(type(pubsub_message)))
    upload_blob()