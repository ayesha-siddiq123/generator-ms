import configparser
import json
import os
import re
import pandas as pd
import psycopg2 as pg

configuartion_path = os.path.dirname(os.path.abspath(__file__)) + "/transformers/config.ini"
print(configuartion_path)
config = configparser.ConfigParser()
config.read(configuartion_path);

port = config['CREDs']['port']
host = config['CREDs']['host']
user = config['CREDs']['user']
password = config['CREDs']['password']
database = config['CREDs']['database']

CeatedTransformersList = []


def KeysMapping(InputKeys, Template, Transformer, Response):
    if os.path.exists(os.path.dirname(os.path.abspath(__file__)) + '/transformers/' + Transformer):
        os.remove(os.path.dirname(os.path.abspath(__file__)) + '/transformers/' + Transformer)
    with open(os.path.dirname(os.path.abspath(__file__)) + '/templates/' + Template, 'r') as fs:
        valueOfTemplate = fs.readlines()
    if len(InputKeys) != 0:
        for valueOfTemplate in valueOfTemplate:
            ToreplaceString = valueOfTemplate
            templateKeys = re.findall("(?<={)(.*?)(?=})", ToreplaceString)
            for key in templateKeys:
                replaceStr = '{' + key + '}'
                ToreplaceString = ToreplaceString.replace(replaceStr, str(InputKeys[key]))
            with open(os.path.dirname(os.path.abspath(__file__)) + '/transformers/' + Transformer, 'a') as fs:
                fs.write(ToreplaceString)
        CeatedTransformersList.append({"filename": Transformer})
        return Response(json.dumps({"Message": "Transformer created successfully", "TransformerFiles": CeatedTransformersList, "code": 200}))
    else:
        print('ERROR : InputKey is Empty')
        return Response(json.dumps({"Message": "InputKey is empty"}))

InputKeys = {}
def dimension_data_insert(request, Response):
    Dimension = request.json['ingestion_name']
    KeyFile = request.json['key_file']
    Path = os.path.dirname(os.path.abspath(__file__)) + "/key_files/" + KeyFile
    df = pd.read_csv(Path)
    if len(df) == 0:
        return Response(json.dumps({"Message": KeyFile + " is empty"}))
    df = df.loc[df['dimension_name'] == Dimension]
    Dimensionkeys = df.keys().tolist()
    DimensionValues = df.values.tolist()
    for value in DimensionValues:
        TemplateDatasetMapping = (dict(zip(Dimensionkeys, value)))
        DimensionName = TemplateDatasetMapping['dimension_name']
        Transformer = DimensionName + '.py'
        TranformerType = TemplateDatasetMapping['template']
        Template = TranformerType + '.py'
        con = pg.connect(database=database, user=user, password=password, host=host, port=port)
        cur = con.cursor()
        DatasetQueryString = '''SELECT dimension_data FROM spec.dimension WHERE dimension_name='{}';'''.format(
            DimensionName)
        cur.execute(DatasetQueryString)
        con.commit()
        if cur.rowcount == 1:
            for records in cur.fetchall():
                for record in list(records):
                    DimensionObject = list(record['input']['properties']['dimension']['items']['properties'].keys())
                    DimensionArray=list(record['input']['properties']['dimension']['items']['required'])
                    TargetTable = list(record['input']['properties']['target_table']['properties'].keys())
                    string_col_list = []
                    DatasetCasting = []
                    df = pd.json_normalize(record['input']['properties']['dimension']['items']['properties'])
                    for cols in DimensionObject:
                        col = cols + '.type'
                        if (df[col] == "string").item():
                            string_col_list.append(cols)
                    if len(string_col_list) != 0:
                            DatasetCasting.append('df_dataset.update(df_dataset[' + json.dumps(string_col_list) + '].applymap("\'{}\'".format))')

                    if TranformerType == 'Dataset':
                        InputKeys.update({'ValueCols':DimensionArray, "KeyFile": json.dumps(Dimension + '.csv'),
                                              'DatasetCasting': ','.join(DatasetCasting),
                                              'TargetTable':','.join(TargetTable),
                                              'InputCols': ','.join(DimensionArray),
                                              'Values': '{}'})
                    else:
                        return Response(json.dumps({"Message": "Transformer type is not correct", "TransformerType": TranformerType,
                                 "Dataset": DimensionName}))
                        print(Transformer,':transformer:::::::::::')
        else:
            return Response(json.dumps({"Message": "No dimension found " + Dimension}))
    return KeysMapping(InputKeys, Template,Transformer, Response)



def collect_keys(request, Response):
    KeyFile = request.json['key_file']
    Program = request.json['program']
    EventName = request.json['ingestion_name']
    Path = os.path.dirname(os.path.abspath(__file__)) + "/key_files/" + KeyFile
    ####### Reading Transformer Mapping Key Files ################
    try:
        df = pd.read_csv(Path)
        if len(df) == 0:
            return Response(json.dumps({"Message": KeyFile + " is empty"}))
        df = df.loc[df['program'] == Program]
        df = df.loc[df['event_name'] == EventName]
        Datasetkeys = df.keys().tolist()
        DatasetValues = df.values.tolist()
        for value in DatasetValues:
            TemplateDatasetMapping = (dict(zip(Datasetkeys, value)))
            DatasetName = TemplateDatasetMapping['dataset_name']
            Transformer = DatasetName + '.py'
            TranformerType = TemplateDatasetMapping['template']
            Template = TranformerType + '.py'
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
                            DatasetObject = list(Dataset['items']['items']['properties'].keys())
                            DatasetArray=list(Dataset['items']['items']['required'])
                            Dimensions = record['input']['properties']['dimensions']['properties']
                            ReplaceFormat = []
                            IncrementFormat = []
                            PercentageIncrement = []
                            UpdateCols = []
                            DatasetCasting = []
                            EventCasting = []
                            string_col_list = []
                            NumeratorCol = \
                                list(Dataset['aggregate']['properties']['numerator_col']['properties'].keys())[0]
                            DenominatorCol = \
                                list(Dataset['aggregate']['properties']['denominator_col']['properties'].keys())[0]
                            fun = list(Dataset['aggregate']['properties']['function']['items']['properties'].keys())
                            UpdateColList = []
                            df = pd.json_normalize(Dataset['items']['items']['properties'])
                            for cols in DatasetObject:
                                col = cols + '.type'
                                if (df[col] == "string").item():
                                    string_col_list.append(cols)
                            if len(string_col_list) != 0:
                                DatasetCasting.append('df_dataset.update(df_dataset[' + json.dumps(
                                    string_col_list) + '].applymap("\'{}\'".format))')
                                EventCasting.append('df_events.update(df_events[' + json.dumps(
                                    string_col_list) + '].applymap("\'{}\'".format))')
                            if not (NumeratorCol.lower()).startswith(('sum_', 'count_')):
                                UpdateColList.append(fun[0] + '_' + NumeratorCol)
                            else:
                                UpdateColList.append(NumeratorCol)
                            if not (DenominatorCol.lower()).startswith(('sum_', 'count_')):
                                UpdateColList.append(fun[0] + '_' + DenominatorCol)
                                UpdateColList.append("percentage")
                            else:
                                UpdateColList.append(DenominatorCol)
                                UpdateColList.append("percentage")
                            for i in UpdateColList:
                                if i == 'percentage':
                                    ReplaceFormat.append(i + '=EXCLUDED.' + i)
                                else:
                                    ReplaceFormat.append(i + '=EXCLUDED.' + i)
                                    UpdateCols.append('row["' + i + '"]')
                                    IncrementFormat.append(i + '=main_table.' + i + '::numeric+{}::numeric')
                                    PercentageIncrement.append('main_table.' + i + '::numeric+{}::numeric')
                            # col = list(Dataset['aggregate']['properties']['columns']['items']['properties']['column']['items']['properties'].keys())
                            agg_col=[NumeratorCol,DenominatorCol]
                            AggCols = (dict(zip(agg_col, (fun * len(agg_col)))))
                            InputKeys.update({'AWSKey': '{}', 'AWSSecretKey': '{}', 'BucketName': '{}', 'ObjKey': '{}',
                                              'Values': '{}', 'EventCasting': ','.join(EventCasting),
                                              'DatasetCasting': ','.join(DatasetCasting), 'ValueCols': DatasetArray,
                                              'GroupBy': list(Dataset['group_by']['items']['properties'].keys()),
                                              'AggCols': AggCols,
                                              'DimensionTable': list(Dimensions['table']['properties'].keys())[0],
                                              'DimensionCols': ','.join(
                                                  list(Dimensions['column']['items']['properties'].keys())),
                                              'MergeOnCol': list(Dimensions['merge_on_col']['properties'].keys()),
                                              'TargetTable': list(Dataset['aggregate']['properties']['target_table'][
                                                                      'properties'].keys())[0],
                                              'InputCols': ','.join(DatasetArray),
                                              'ConflictCols': ','.join(
                                                  list(Dataset['group_by']['items']['properties'].keys())),
                                              'IncrementFormat': ','.join(IncrementFormat),
                                              'ReplaceFormat': ','.join(ReplaceFormat),
                                              'UpdateCols': ','.join(UpdateCols * 2),
                                              'UpdateCol': ','.join(UpdateCols),
                                              "KeyFile": json.dumps(EventName + '.csv')})
                            print(Template, '::::::::::::Template::::::::::::')
                            if TranformerType in ['EventToCube', 'EventToCubeIncrement']:
                                InputKeys.update(InputKeys)
                            elif TranformerType in ['EventToCubePer', 'EventToCubePerIncrement']:
                                InputKeys.update({'NumeratorCol': NumeratorCol, 'DenominatorCol': DenominatorCol,
                                                  'QueryDenominator': PercentageIncrement[1],
                                                  'QueryNumerator': PercentageIncrement[0]})
                            elif TranformerType in ['CubeToCube', 'CubeToCubeIncrement']:
                                table = list(
                                    Dataset['aggregate']['properties']['columns']['items']['properties']['table'][
                                        'properties'].keys())
                                InputKeys.update({'Table': table[0]})
                            elif TranformerType in ['CubeToCubePer', 'CubeToCubePerIncrement', 'E&CToCubePerIncrement',
                                                    'E&CToCubePer']:
                                table = list(
                                    Dataset['aggregate']['properties']['columns']['items']['properties']['table'][
                                        'properties'].keys())
                                InputKeys.update(
                                    {'Table': table[0], 'NumeratorCol': NumeratorCol, 'DenominatorCol': DenominatorCol,
                                     'QueryDenominator': PercentageIncrement[1],
                                     'QueryNumerator': PercentageIncrement[0]})
                            elif TranformerType in ['CubeToCubePerFilter', 'CubeToCubePerFilterIncrement']:
                                table = list(
                                    Dataset['aggregate']['properties']['columns']['items']['properties']['table'][
                                        'properties'].keys())
                                filter = Dataset['aggregate']['properties']['filters']['items']['properties']
                                InputKeys.update(
                                    {'Table': table[0], 'FilterCol': list(filter['column']['properties'].keys()),
                                     'FilterType': list(filter['filter_type']['properties'].keys())[0],
                                     'Filter': list(filter['filter']['properties'].keys())[0],
                                     'DimensionTable': list(Dimensions['table']['properties'].keys())[0],
                                     'NumeratorCol': NumeratorCol, 'DenominatorCol': DenominatorCol,
                                     'QueryDenominator': PercentageIncrement[1],
                                     'QueryNumerator': PercentageIncrement[0]})
                            else:
                                return Response(json.dumps(
                                    {"Message": "Transformer type is not correct", "TransformerType": TranformerType,
                                     "Dataset": DatasetName}))
                            print(Transformer, ':::::::::::Transformer:::::::::::::::')
                            KeysMapping(InputKeys, Template, Program + '_' + Transformer, Response)
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
        return Response(
            json.dumps({"Message": "Transformer not created ", "transformerFiles": Transformer, "code": 400}))
    return KeysMapping(InputKeys, Template, Program + '_' + Transformer, Response)
