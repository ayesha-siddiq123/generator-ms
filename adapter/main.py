import pandas as pd
import configparser
import re
import os
import sys

configuartion_path =os.path.dirname(os.path.abspath(__file__)) + "/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);

class CollectData:
    def __init__(self):
        self.state = sys.argv[1]
        self.program = sys.argv[2]
        self.file_name = sys.argv[3]
        self.input_path = config['KEYs']['input']
        self.output_path = config['KEYs']['output']
        self.rep_list=[]
    def create_dir(self):
        if not os.path.exists(self.output_path + '/' + self.state + '/' + self.program):
            os.makedirs(self.output_path + '/' + self.state + '/' + self.program)

    def column_rename(self):
        df_data = pd.read_csv(self.input_path + '/' + self.file_name)
        for col in df_data.columns.tolist():
            x=re.sub(r'^[\d.\s]+|[\d.\s]+$]+','',col)
            self.rep_list.append(x)
        col_list = df_data.columns.tolist()
        df_snap=df_data[col_list]
        df_snap.columns=self.rep_list
        return df_snap


