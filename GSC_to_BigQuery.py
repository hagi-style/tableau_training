import argparse
import pandas as pd
import urllib.parse
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import os
from google.cloud import bigquery, storage

GSC_PROPERTY_NAME = 'https://parallux.net/'
BUCKET_NAME = 'gsc_storage_estyle'
FILE_DIR_NAME = 'gsc_data/'
GOOGLE_APPLICATION_CREDENTIALS_PATH = './credentials.json'
CLIENT_SECRET_PATH = './client_secret.json'
CSV_PREFIX = "gsc_"

GCS_DIR = 'gs://' + BUCKET_NAME + '/'
DATA_SET_ID = 'gsc_dataset'
TABLE_PREFIX = "gsc_"

parser = argparse.ArgumentParser(description='gsc_to_gcs')
parser.add_argument('date', help='end date; yyyy-mm-dd')
args = parser.parse_args()

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']


def main():
    file_name = CSV_PREFIX + args.date.replace("-", "") + ".csv"
    print(file_name, ":start creating csv file")
    #GSC のデータを取得して、df に変換
    df = get_gsc_data()
    #GSC にアップロードするための一時 CSV データを作成
    temp_file_name = 'temp.csv'
    df.to_csv(temp_file_name, index=False)
    #Temp.csv を GCS にアップロード
    gsc_to_gcs(file_name, temp_file_name)
    #Tempファイルの削除
    if os.path.exists(temp_file_name):
        os.remove(temp_file_name)

    table_name = TABLE_PREFIX + args.date.replace("-", "")
    print(table_name, "start creating table")
    #GCS のデータを BigQuery にアップロード
    gcs_to_bq(table_name)

def get_gsc_data():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRET_PATH, SCOPES)
    webmasters = build('webmasters', 'v3', credentials=credentials)
    print(webmasters.sites().list().execute())

    d_list = ['query', 'date', 'country', 'device', 'page']
    start_date = '2020-07-10'
    end_date = args.date
    row_limit = 5000

    body = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': d_list,
        'rowLimit': row_limit
    }

    response = webmasters.searchanalytics().query(siteUrl=GSC_PROPERTY_NAME, 
        body=body).execute()
    df = pd.io.json.json_normalize(response['rows'])

    for i, d in enumerate(d_list):
        df[d] = df['keys'].apply(lambda x: x[i])
    df.drop(columns='keys', inplace=True)
    df['page'] = df['page'].apply(lambda x: urllib.parse.unquote(x))

    return df

def gsc_to_gcs(file_name, temp_file_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS_PATH
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_DIR_NAME + file_name)
    blob.upload_from_filename(filename=temp_file_name)
    print("GCS バケットへ保存が完了")

def gcs_to_bq(table_name):
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS_PATH

    client = bigquery.Client()
    data_set_ref = client.dataset(DATA_SET_ID)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.skip_leading_rows = 1

    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    table_ref = client.dataset(DATA_SET_ID).table(table_name)
    try:
        client.delete_table(table_ref)  # API request
    except:
        pass
    
    uri = GCS_DIR + FILE_DIR_NAME + table_name + '.csv'
    load_job = client.load_table_from_uri(
        uri,
        data_set_ref.table(table_name),
        job_config=job_config)  # API request

    assert load_job.job_type == 'load'

    load_job.result()  # Waits for table load to complete.

    assert load_job.state == 'DONE'

    print("BigQuery にアップロード完了")


if __name__ == '__main__':
    main()