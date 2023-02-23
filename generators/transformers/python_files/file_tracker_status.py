import shutil
import requests
import json
import configparser
import os
path=os.path.dirname(os.path.abspath(__file__))
root_path=os.path.dirname(os.path.dirname(os.path.dirname(path)))
configuartion_path =path + "/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);
ingestion_host = config['CREDs']['ingestion_host']
ingestion_port = config['CREDs']['ingestion_port']
url=ingestion_host+':'+ingestion_port+'/file-status'
print(url,':::url::::::')

def file_check(KeyFile,ingestion_type):
  if not os.path.exists(os.path.dirname(root_path)+"processing_data/" + KeyFile):
    shutil.move(os.path.dirname(root_path)+'input_data/' + KeyFile, os.path.dirname(root_path)+'processing_data/' + KeyFile)
    status_track(KeyFile,ingestion_type , 'Processing')
def status_track(file_name,ingestion_type,status):
  ingestion_name=file_name.split('.')[0]
  headers = {
    'Content-Type': 'application/json'
  }
  request = json.dumps({
    "file_name": file_name,
    "ingestion_type": ingestion_type,
    "ingestion_name": ingestion_name,
    "status":status
  })
  print(request,'::::request body:::::::::::::')
  response = requests.request("PUT", url, headers=headers, data=request)
  re = response.json()
  print(re,':::::::::::response:::::::::::::::')
  if re['ready_to_archive'] == True:
    shutil.move(os.path.dirname(root_path)+'processing_data/'+file_name,os.path.dirname(root_path)+'archived_data/'+file_name)



