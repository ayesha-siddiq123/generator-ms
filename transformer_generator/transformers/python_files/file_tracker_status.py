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

def create_folder(folder_name):
  if not os.path.exists(os.path.dirname(path)+folder_name):
    os.makedirs(os.path.dirname(path) + folder_name)
def file_check(KeyFile,ingestion_type):
  if not os.path.exists(os.path.dirname(path) + "/processing/" + KeyFile):
    shutil.move(os.path.dirname(path)+'/input/' + KeyFile, os.path.dirname(path)+'/processing/' + KeyFile)
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
    create_folder('/archive')
    shutil.move(os.path.dirname(path)+'/processing/'+file_name,os.path.dirname(path) +'/archive/'+file_name)



