import shutil
import requests
import json
import configparser
import os

configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
print(configuartion_path)
config = configparser.ConfigParser()
config.read(configuartion_path);
url = config['CREDs']['server_url']

def create_folder(folder_name):
  if not (os.path.exists(os.path.abspath(__file__))+folder_name):
    os.makedirs(os.path.exists(os.path.abspath(__file__)) + folder_name)
def file_check(KeyFile):
  if not os.path.exists(os.path.dirname(os.path.abspath(__file__)) + "/processing/" + KeyFile):
    shutil.move('input/' + KeyFile, 'processing/' + KeyFile)
    status_track(KeyFile, 'event', 'Processing')
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
  if re['ready_to_archive'] == "true":
    create_folder('archive')
    shutil.move(os.path.dirname(os.path.abspath(__file__)) +'processing/'+file_name, os.path.dirname(os.path.abspath(__file__)) +'archive/'+file_name)



