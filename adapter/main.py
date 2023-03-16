import csv
import re
import os
import sys
import zipfile

import boto3
import pandas as pd
import configparser
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import io

configuartion_path =os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

class CollectData:
    def __init__(self):
        '''
        Collecting all required Keys from config and from the arguments passing while running the script
        '''
        self.program = sys.argv[1]
        self.input_file = sys.argv[2]
        self.date_today = datetime.now().strftime('%d-%b-%Y')
        self.env = config['CREDs']['env']
        # self.date_today = '14-Mar-2023'
        # ______________AZURE Blob config keys______________________
        self.azure_connection_string = config['CREDs']['azure_connection_string']
        self.azure_account_name = config['CREDs']['azure_account_name']
        self.azure_account_key = config['CREDs']['azure_account_key']
        self.azure_container = config['CREDs']['azure_container']
        self.azure_input_folder = 'emission/' + self.date_today
        # self.azure_input_folder = '14-Mar-2023'
        self.azure_output_folder = 'process_input/' + self.program + '/' + self.date_today

        # __________________S3 bucket config keys___________________
        self.aws_access_key = config['CREDs']['aws_access_key']
        self.aws_secret_key = config['CREDs']['aws_secret_key']
        self.s3_bucket = config['CREDs']['s3_bucket']
        self.s3_input_folder = 'emission/'+self.date_today
        self.rep_list = []

        # ___________________AZURE Blob Connection___________________________
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.azure_connection_string)
            self.container_client = self.blob_service_client.get_container_client(self.azure_container)
        except Exception:
            print('Failed to connect to azure blob')

        #__________________S3 Bucket Connection ____________________
        try:
            self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key,aws_secret_access_key=self.aws_secret_key)
            self.objects_list = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_input_folder)

        except Exception:
            print('Failed to connect to S3 bucket')

    def column_rename(self,df):
        for col in df.columns.tolist():
            x=re.sub(r'^[\d.\s]+|[\d.\s]+$]+','',col)
            self.rep_list.append(x)
        col_list = df.columns.tolist()
        df_snap=df[col_list]
        df_snap.columns=self.rep_list
        return df_snap
    def  get_file(self):
        if self.env == 'azure':
            blobs_list = self.container_client.list_blobs(name_starts_with=self.azure_input_folder)
            if any(blobs_list):
                blob_client = self.container_client.get_blob_client(blob=self.azure_input_folder+'/'+self.input_file)
                content=blob_client.download_blob().content_as_bytes()
                df=pd.read_csv(io.BytesIO(content))
                df_snap=self.column_rename(df)
                return df_snap
            else:
                print(f'The folder {self.azure_input_folder} not exists in azure blob container.')
        elif self.env == 'AWS':
            if 'Contents' in self.objects_list:
                file_bytes =self.s3.get_object(Bucket=self.s3_bucket, Key=self.s3_input_folder + '/' + self.input_file)
                data=io.BytesIO(file_bytes['Body'].read())
                with zipfile.ZipFile(data) as myzip:
                    with myzip.open(myzip.namelist()[0]) as myfile:
                        df = pd.read_csv(myfile)
                        df_snap = self.column_rename(df)
                        return df_snap
            else:
                print(f"The folder {self.s3_input_folder} does not exist in the bucket {self.s3_bucket}.")
        elif self.env == 'local':
            pass

    def upload_file(self,csv_data,output_file):
        if self.env == 'azure':
            blob_client=self.container_client.get_blob_client(blob=self.azure_output_folder+'/'+output_file)
            blob_client.upload_blob(io.BytesIO(csv_data.encode('utf-8')), overwrite=True)
            print("File uploaded successfully.")
        # elif self.env == 'AWS':
        #     if 'Contents' in self.objects_list:
        #         print("Folder already exists.")
        #     else:
        #         # Create the folder
        #         self.s3.put_object(Bucket=self.s3_bucket, Key=)

            #Upload the file to the folder
            # self.s3.upload_file(file_name, bucket_name, folder_name + file_name)
            print("File uploaded successfully.")
        elif self.env == 'local':
            pass


obj=CollectData()
obj.get_file()
