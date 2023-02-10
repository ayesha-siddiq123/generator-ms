import configparser
import json
import os
import re
import pandas as pd
import psycopg2 as pg


configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/transformers/python_files/config.ini"
print(configuartion_path)
config = configparser.ConfigParser()
config.read(configuartion_path);

port = config['CREDs']['db_port']
host = config['CREDs']['db_host']
user = config['CREDs']['db_user']
password = config['CREDs']['db_password']
database = config['CREDs']['database']
CeatedTransformersList = []

def KeysMapping(InputKeys, Template, Transformer, Response):
    if not (os.path.exists(os.path.dirname(os.path.abspath(__file__))+ "/transformers/python_files")):
        os.makedirs(os.path.dirname(os.path.abspath(__file__)) + "/transformers/python_files")
    if os.path.exists(os.path.dirname(os.path.abspath(__file__)) + '/transformers/python_files/' + Transformer):
        os.remove(os.path.dirname(os.path.abspath(__file__)) + '/transformers/python_files/' + Transformer)
    with open(os.path.dirname(os.path.abspath(__file__)) + '/templates/' + Template, 'r') as fs:
        valueOfTemplate = fs.readlines()
    if len(InputKeys) != 0:
        for valueOfTemplate in valueOfTemplate:
            ToreplaceString = valueOfTemplate
            templateKeys = re.findall("(?<={)(.*?)(?=})", ToreplaceString)
            for key in templateKeys:
                replaceStr = '{' + key + '}'
                ToreplaceString = ToreplaceString.replace(replaceStr, str(InputKeys[key]))
            with open(os.path.dirname(os.path.abspath(__file__)) + '/transformers/python_files/' + Transformer, 'a') as fs:
                fs.write(ToreplaceString)
        CeatedTransformersList.append({"filename": Transformer})
    else:
        print('ERROR : InputKey is Empty')
        return Response(json.dumps({"Message": "InputKey is empty"}))

InputKeys = {}
def collect_dimension_keys(request, Response):
    Dimension = request.json['ingestion_name']
    KeyFile = request.json['key_file']
    Path = os.path.dirname(os.path.abspath(__file__)) + "/key_files/" + KeyFile
    try:
        df = pd.read_csv(Path)
        dimension_list=df['dimension_name'].drop_duplicates().tolist()
        if len(df) == 0:
            return Response(json.dumps({"Message": KeyFile + " is empty"}))
        if Dimension not in dimension_list:
            return Response(json.dumps({"Message": "Invalid dimension name ", "Dimension": Dimension}))
        df = df.loc[df['dimension_name'] == Dimension]
        Dimensionkeys = df.keys().tolist()
        DimensionValues = df.values.tolist()
        for value in DimensionValues:
            TemplateDatasetMapping = (dict(zip(Dimensionkeys, value)))
            DimensionName = TemplateDatasetMapping['dimension_name']
            Transformer = DimensionName + '.py'
            TransformerType = TemplateDatasetMapping['transformer_template']
            Template = TransformerType + '.py'
            con = pg.connect(database=database, user=user, password=password, host=host, port=port)
            cur = con.cursor()
            QueryString = '''SELECT dimension_data FROM spec.dimension WHERE dimension_name='{}';'''.format(DimensionName)
            cur.execute(QueryString)
            con.commit()
            if cur.rowcount == 1:
                for records in cur.fetchall():
                    for record in list(records):
                        DimensionArray=list(record['input']['properties']['dimension']['items']['required'])
                        TargetTable = record['input']['properties']['target_table']['pattern']

                        if TransformerType == 'Dataset_Dimension':
                            InputKeys.update({'ValueCols':DimensionArray, "KeyFile": Dimension + '.csv',
                                                  'TargetTable':TargetTable,
                                                  'InputCols': ','.join(DimensionArray),
                                                  'Values': '{}',"DimensionName":DimensionName})
                        else:
                            return Response(json.dumps({"Message": "Invalid transformer type", "TransformerType": TranformerType,
                                     "Dataset": DimensionName}))
                            print(Transformer,':transformer:::::::::::')
                        KeysMapping(InputKeys, Template, Transformer, Response)
            else:
                return Response(json.dumps({"Message": "No dimension found " + Dimension}))
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Transformer not created ", "transformerFiles": Transformer, "code": 400}))
    return Response(json.dumps({"Message": "Transformer created successfully", "TransformerFiles": CeatedTransformersList, "code": 200}))


def collect_dataset_keys(request, Response):
    KeyFile = request.json['key_file']
    Program = request.json['program']
    EventName = request.json['ingestion_name']
    Path = os.path.dirname(os.path.abspath(__file__)) + "/key_files/" + KeyFile
    ####### Reading Transformer Mapping Key Files ################
    try:
        df = pd.read_csv(Path)
        if len(df) == 0:
            return Response(json.dumps({"Message": KeyFile + " is empty"}))
        program_list=df['program'].drop_duplicates().tolist()
        event_list=df['event_name'].drop_duplicates().tolist()
        if Program not in program_list:
             return Response(json.dumps({"Message":"Invalid program name","Program":Program}))
        if EventName not in event_list:
             return Response(json.dumps({"Message":"Invalid ingestion name","IngestionName":EventName}))
        df = df.loc[df['program'] == Program]
        df = df.loc[df['event_name'] == EventName]
        Datasetkeys = df.keys().tolist()
        DatasetValues = df.values.tolist()
        for value in DatasetValues:
            TemplateDatasetMapping = (dict(zip(Datasetkeys, value)))
            DatasetName = TemplateDatasetMapping['dataset_name']
            global Transformer
            Transformer = DatasetName + '.py'
            TransformerType = TemplateDatasetMapping['transformer_template']
            global Template
            Template = TransformerType + '.py'
            con = pg.connect(database=database, user=user, password=password, host=host, port=port)
            cur = con.cursor()
            EventQueryString = ''' SELECT event_data FROM spec.event WHERE event_name='{}';'''.format(EventName)
            cur.execute(EventQueryString)
            con.commit()
            if cur.rowcount == 1:
                DatasetQueryString = '''SELECT dataset_data FROM spec.dataset WHERE dataset_name='{}';'''.format(
                    DatasetName)
                cur.execute(DatasetQueryString)
                con.commit()
                if cur.rowcount == 1:
                    for records in cur.fetchall():
                        for record in list(records):
                            Dataset = record['input']['properties']['dataset']['properties']
                            DatasetArray=Dataset['items']['items']['required']
                            Dimensions = record['input']['properties']['dimensions']['properties']
                            NumeratorCol = Dataset['aggregate']['properties']['numerator_col']['pattern']
                            DenominatorCol = Dataset['aggregate']['properties']['denominator_col']['pattern']
                            fun = Dataset['aggregate']['properties']['function']
                            DateFilter = []
                            YearFilter = []
                            for i in DatasetArray:
                                if 'date' in i.casefold():
                                    DateFilter.append('df_dataset = df_dataset.loc[df_dataset[' + json.dumps(i) + '] == str(date.today())]')
                                elif 'year' in i.casefold():
                                    YearFilter.append('df_dataset = df_dataset.loc[df_dataset[' + json.dumps(i) + '] == str((date.today()).year)]')
                            UpdateCols = []
                            ReplaceFormat = []
                            IncrementFormat = []
                            PercentageIncrement = []
                            UpdateColArray=Dataset['aggregate']['properties']['update_cols']
                            for i in UpdateColArray:
                                if i == 'percentage':
                                    ReplaceFormat.append(i + '=EXCLUDED.' + i)
                                else:
                                    ReplaceFormat.append(i + '=EXCLUDED.' + i)
                                    UpdateCols.append('row["' + i + '"]')
                                    IncrementFormat.append(i + '=main_table.' + i + '::numeric+{}::numeric')
                                    PercentageIncrement.append('main_table.' + i + '::numeric+{}::numeric')
                            agg_col =Dataset['aggregate']['properties']['columns']['items']['properties']['column']
                            AggCols = (dict(zip(agg_col, (fun * len(agg_col)))))
                            InputKeys.update({'Values': '{}','ValueCols': DatasetArray,'DateFilter':','.join(DateFilter),'YearFilter': ','.join(YearFilter),
                                'GroupBy': Dataset['group_by'],'AggCols': AggCols,'DimensionTable':Dimensions['table']['pattern'],
                                'DimensionCols': ','.join(Dimensions['column']),'DimColCast':json.dumps(Dimensions['column']),'MergeOnCol': Dimensions['merge_on_col']['pattern'],
                                 'TargetTable': Dataset['aggregate']['properties']['target_table']['pattern'],
                                'InputCols': ','.join(DatasetArray),'ConflictCols': ','.join(Dataset['group_by']),
                                'IncrementFormat': ','.join(IncrementFormat),'ReplaceFormat': ','.join(ReplaceFormat),
                                'UpdateCols': ','.join(UpdateCols * 2),'UpdateCol': ','.join(UpdateCols),
                                "KeyFile": EventName + '.csv','DatasetName':DatasetName})

                            print(Template, '::::::::::::Template::::::::::::')

                            if TransformerType in ['EventToCube', 'EventToCubeIncrement']:
                                InputKeys.update(InputKeys)

                            elif TransformerType in ['EventToCubePer', 'EventToCubePerIncrement']:
                                InputKeys.update({'NumeratorCol': NumeratorCol, 'DenominatorCol': DenominatorCol,'AggColOne':agg_col[0],'AggColTwo':agg_col[1],
                                                  'QueryDenominator': PercentageIncrement[1],'QueryNumerator': PercentageIncrement[0]})

                            elif TransformerType in ['E&CToCubePerIncrement','E&CToCubePer']:
                                table = Dataset['aggregate']['properties']['columns']['items']['properties']['table']['pattern']
                                InputKeys.update({'Table': table,'QueryDenominator': PercentageIncrement[1],
                                     'QueryNumerator': PercentageIncrement[0],"eventCol":agg_col[0],"RenameCol":NumeratorCol,
                                     "NumeratorCol":NumeratorCol,"DenominatorCol":DenominatorCol})

                            elif TransformerType in ['CubeToCubePerFilter', 'CubeToCubePerFilterIncrement']:
                                table = Dataset['aggregate']['properties']['columns']['items']['properties']['table']['pattern']
                                filter = Dataset['aggregate']['properties']['filters']['properties']
                                InputKeys.update({'Table': table, 'FilterCol': filter['filter_col']['pattern'],'AggCol':agg_col[0],
                                     'FilterType':filter['filter_type']['pattern'],'Filter':filter['filter']['pattern'],'NumeratorCol': NumeratorCol, 'DenominatorCol': DenominatorCol,
                                     'QueryDenominator': PercentageIncrement[1],'QueryNumerator': PercentageIncrement[0]})

                            elif TransformerType in ['EventToCubePerFilterIncrement']:
                                filter = Dataset['aggregate']['properties']['filters']['properties']
                                InputKeys.update({'FilterCol': filter['filter_col']['pattern'], 'AggCol': agg_col[0],
                                                  'FilterType': filter['filter_type']['pattern'], 'Filter': filter['filter']['pattern'],
                                                  'NumeratorCol': NumeratorCol, 'DenominatorCol': DenominatorCol,
                                                  'QueryDenominator': PercentageIncrement[1],'QueryNumerator': PercentageIncrement[0]})
                            else:
                                return Response(json.dumps({"Message": "Invalid transformer type", "TransformerType": TransformerType,
                                     "Dataset": DatasetName}))
                            print(Transformer, ':::::::::::Transformer:::::::::::::::')
                            KeysMapping(InputKeys, Template,Transformer, Response)
                else:
                    print('ERROR : No dataset found')
                    return Response(json.dumps({"Message": "No dataset found " + DatasetName}))
            else:
                print('ERROR : No Event Found')
                return Response(json.dumps({"Message": "No event found " + EventName}))
            if cur is not None:
                cur.close()
            if con is not None:
                con.close()
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Transformer not created ", "transformerFiles": Transformer, "code": 400}))
    return Response(json.dumps({"Message": "Transformer created successfully", "TransformerFiles": CeatedTransformersList, "code": 200}))
