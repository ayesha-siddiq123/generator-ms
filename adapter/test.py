import re
import os
import sys
import boto3
import pandas as pd
import configparser
from datetime import datetime
from azure.storage.blob import BlobServiceClient,ContentSettings
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

        # ____________AZURE Blob config keys______________________
        self.azure_connection_string = config['CREDs']['azure_connection_string']
        self.azure_account_name = config['CREDs']['azure_account_name']
        self.azure_account_key = config['CREDs']['azure_account_key']
        self.azure_container = config['CREDs']['azure_container']
        self.azure_input_folder='emission/'+self.date_today
        self.azure_output_folder='process_input/'+self.program+'/'+self.date_today
        self.blob_service_client = BlobServiceClient.from_connection_string(self.azure_connection_string)
        self.container_client = self.blob_service_client.get_container_client('cqube-container')

        #___________s3 bucket config keys____________________________
        self.aws_access_key = config['CREDs']['aws_access_key']
        self.aws_secret_key = config['CREDs']['aws_secret_key']
        self.s3_emission_bucket=config['CREDs']['s3_emission_bucket']
        self.rep_list=[]
    # def azure_connection(self):
    #     try:
    #         self.blob_service_client = BlobServiceClient.from_connection_string(self.azure_connection_string)
    #         self.container_client =self.blob_service_client.get_container_client('cqube-container')
    #         self.blobs_list=self.container_client.list_blobs(prefix=self.folder_name)

        # except Exception:
        #     print('Failed to connect azure blob')

    # def s3_connection(self):
    #     try:
    #         self.s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key, aws_secret_access_key=self.aws_secret_key)
    #         self.objects_list = self.s3.list_objects_v2(Bucket=self.s3_emission_bucket, Prefix=self.folder_name)
    #     except Exception:
    #         print('Failed to connect to S3 bucket')

    def column_rename(self,df):
        for col in df.columns.tolist():
            x=re.sub(r'^[\d.\s]+|[\d.\s]+$]+','',col)
            self.rep_list.append(x)
        col_list = df.columns.tolist()
        df_snap=df[col_list]
        df_snap.columns=self.rep_list
        return df_snap

    def  get_file(self):
        if self.env == 'AZURE':
            blobs_list = self.container_client.list_blobs(prefix='14-Mar-2023/'+self.input_file)
            for i in blobs_list:
                print(i.name,':::::::::::::::::::::::')
            if any(self.blobs_list):
                print(':::::::::::::')
                blob_client =self.blob_service_client.get_blob_client(container='cqube-container',blob='14-Mar-2023/' + self.input_file)
                file_bytes=blob_client.download_blob().content_as_bytes()
                df=pd.read_csv(io.BytesIO(file_bytes))
                df_data=obj.column_rename(self,df)
                print(df_data,':::::::::::::::::::::::')
            else:
                print("Folder is not exists.")
        # elif self.env == 'AWS':
        #     if 'Contents' in self.objects_list:
        #         file_bytes = self.s3.get_object(Bucket=self.s3_emission_bucket, Key=self.input_file)['Body'].read()
        #         df = pd.read_csv(io.BytesIO(file_bytes))
        #         df_data=obj.column_rename(self,df)
        #     else:
        #         print("Folder is not exists.")
        # elif self.env == 'MINIO':
        #     pass
    def Dataframe_to_csv(self,df_data,output_file):
        self.output_file=output_file
        csv_bytes = io.StringIO()
        df_data.to_csv(csv_bytes, index=False, encoding='utf-8')
        self.csv_bytes = csv_bytes.getvalue().encode('utf-8')

    def upload_file(self):
        if self.env == 'AZURE':
            blobs_list = self.container_client.list_blobs(name_starts_with=self.azure_output_folder)
            # Create the folder if not exists
            if  not any(blob.name == self.azure_output_folder for blob in blobs_list):
                self.container_client.upload_blob(name=self.azure_output_folder,data='')
            else:
                # Upload the file to the folder
                self.container_client.upload_blob(name=self.azure_output_folder+self.output_file, data=self.csv_bytes, overwrite=True)
                print("File uploaded successfully.")
        # elif self.env == 'AWS':
        #     if 'Contents' in self.objects_list:
        #         print("Folder already exists.")
        #     else:
        #         # Create the folder
        #         self.s3.put_object(Bucket=bucket_name, Key=folder_name)
        #
        #     # Upload the file to the folder
        #     self.s3.upload_file(file_name, bucket_name, folder_name + file_name)
        #     print("File uploaded successfully.")
        # elif self.env == 'MINIO':
        #     pass




obj=CollectData()
# obj.azure_connection()
obj.get_file()