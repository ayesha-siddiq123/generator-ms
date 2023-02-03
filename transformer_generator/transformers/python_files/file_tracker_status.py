import shutil
import requests
import json
import configparser
import os
path=os.path.dirname(os.path.abspath(__file__))

configuartion_path =path + "/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);
url = config['CREDs']['server_url']


def file_check(KeyFile,ingestion_type):
  if not os.path.exists("/processing_data/" + KeyFile):
    shutil.move('/input_data/' + KeyFile, '/processing_data/' + KeyFile)
    status_track(KeyFile,ingestion_type , 'Processing')
def status_track(file_name,ingestion_type,status):
  ingestion_name=file_name.strip('.csv')
  headers = {
    'Content-Type': 'application/json'
  }
  request = json.dumps({
    "file_name": file_name,
    "ingestion_type": ingestion_type,
    "ingestion_name": ingestion_name,
    "status":status
  })
  response = requests.request("PUT", url, headers=headers, data=request)
  re = response.json()
  if re['ready_to_archive'] == True:
    shutil.move('/processing_data/'+file_name,'/archived_data/'+file_name)



