import csv
import re
import os
import sys
import zipfile
from minio import Minio
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
        self.program      = sys.argv[1]
        self.input_file   = sys.argv[2]
        self.date_today = datetime.now().strftime('%d-%b-%Y')
        self.env          = config['CREDs']['storage_type']

        # ______________AZURE Blob Config keys______________________
        self.azure_connection_string = config['CREDs']['azure_connection_string']
        self.azure_account_name      = config['CREDs']['azure_account_name']
        self.azure_account_key       = config['CREDs']['azure_account_key']
        self.azure_container         = config['CREDs']['azure_container']
        self.azure_input_folder      = 'emission/'+self.date_today+'/'+self.input_file
        self.azure_output_folder     = 'process_input/' + self.program + '/' + self.date_today

        # __________________S3 Bucket Config Keys___________________

        self.aws_access_key     = config['CREDs']['aws_access_key']
        self.aws_secret_key     = config['CREDs']['aws_secret_key']
        self.s3_bucket          = config['CREDs']['s3_bucket']
        self.s3_input_folder    = 'emission/' + self.date_today+'/'+self.input_file
        self.s3_output_folder   = 'process_input/' + self.program + '/' + self.date_today
        self.rep_list = []

        #___________________Minio Bucket Config Keys___________________

        self.minio_endpoint     = config['CREDs']['end_point']
        self.minio_port         = config['CREDs']['port']
        self.minio_access_key   = config['CREDs']['minio_access_key']
        self.minio_secrete_key  = config['CREDs']['minio_secrete_key']
        self.minio_bucket       = config['CREDs']['minio_bucket']
        self.minio_input_folder = 'emission/' + self.date_today+'/'+self.input_file
        self.minio_output_folder= 'process_input/'+ self.program + '/' + self.date_today

        # ___________________AZURE Blob Connection___________________________
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.azure_connection_string)
            self.container_client = self.blob_service_client.get_container_client(self.azure_container)
        except Exception:
            print(f'Error: Failed to connect to azure {self.azure_container} container')

        #__________________S3 Bucket Connection ____________________

        try:
            self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key,aws_secret_access_key=self.aws_secret_key)
            self.s3_objects_list = self.s3.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_input_folder)
        except Exception:
            print(f'Error: Failed to connect to {self.s3_bucket} bucket')

        #__________________ Minio Bucket Connection_________________
        try:
            self.minio_client = Minio(endpoint=self.minio_endpoint+':'+self.minio_port,access_key=self.minio_access_key,secret_key=self.minio_secrete_key,secure=False)  # set this to True if your Minio instance is secured with SSL/TLS
            self.minio_object_list=self.minio_client.list_objects(self.minio_bucket, prefix=self.minio_input_folder, recursive=True)
        except Exception:
            print(f'Error: Failed to connect to {self.minio_bucket} bucket')

    #___________________________Column renaming after reading file from colud__________

    def column_rename(self,df):
        for col in df.columns.tolist():
            x=re.sub(r'^[\d.\s]+|[\d.\s]+$]+','',col)
            self.rep_list.append(x)
        col_list = df.columns.tolist()
        df_snap=df[col_list]
        df_snap.columns=self.rep_list
        return df_snap
    def data_parser(self,data):
        with zipfile.ZipFile(data) as myzip:
            with myzip.open(myzip.namelist()[0]) as myfile:
                df = pd.read_csv(myfile)
                df_snap = self.column_rename(df)
                return df_snap

    #_____________________________Read the File from Cloud_____________________________

    def  get_file(self):
        if self.env == 'azure': ## reading frile from Azure cloud
            blobs_list = self.container_client.list_blobs(name_starts_with=self.azure_input_folder)
            if any(blobs_list):
                blob_client = self.container_client.get_blob_client(blob=self.azure_input_folder)
                data=io.BytesIO(blob_client.download_blob().readall())
                df_snap=self.data_parser(data)
                return df_snap
            else:
                print(f'Error: The folder {self.azure_input_folder} not exists in azure blob container.')

        elif self.env == 'AWS': ## Reading file from AWS cloud
            if 'Contents' in self.s3_objects_list:
                file_bytes =self.s3.get_object(Bucket=self.s3_bucket, Key=self.s3_input_folder)
                data=io.BytesIO(file_bytes['Body'].read())
                df_snap=self.data_parser(data)
                return df_snap
            else:
                print(f"Error: The folder {self.s3_input_folder} does not exist in the bucket {self.s3_bucket}.")

        elif self.env == 'local': ## Reading file from local Minio
            if not self.minio_object_list:
                object_data = self.minio_client.get_object(self.minio_bucket, self.minio_input_folder)
                data=io.BytesIO(object_data.read())
                df_snap=self.data_parser(data)
                return df_snap
            else:
                print(f'Error: Folder {self.minio_input_folder} does not exist')

    #___________________________Upload the file to the cloud folder_______________________

    def upload_file(self,csv_data,output_file):

        csv_bytes = csv_data.to_csv().encode('utf-8')
        csv_buffer = io.BytesIO(csv_bytes)

        if self.env == 'azure': ## uploading file to Azure blob container
            blob_client=self.container_client.get_blob_client(blob=self.azure_output_folder+'/'+output_file)
            blob_client.upload_blob(csv_buffer, overwrite=False)
            print(f"File {output_file} uploaded successfully to the folder {self.azure_output_folder}.")

        elif self.env == 'AWS': ## uploading file to AWS bucket
            self.s3.put_object(Body=csv_buffer,Bucket=self.s3_bucket,Key=self.s3_output_folder+'/'+output_file)
            print(f"File {output_file} uploaded successfully to the folder {self.s3_output_folder}.")

        elif self.env == 'local': ## uploading file to local Minio
            self.minio_client.put_object(bucket_name=self.minio_bucket,object_name=self.minio_output_folder+'/'+output_file,data=csv_buffer,length=len(csv_bytes),content_type='application/csv')
            print(f"File {output_file} uploaded successfully to the folder {self.minio_output_folder}.")



