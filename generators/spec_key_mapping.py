import json
import os
import re
from datetime import date
import pandas as pd
import glob
root_path=os.path.dirname(os.path.abspath(__file__))
##### Getting todays Date
todays_date = date.today()
SpecTemplatePath=glob.glob(root_path+'/templates/*.json')
SpecTemplateList=[spec.split('/')[-1].split('.')[0] for spec in SpecTemplatePath]
print(SpecTemplateList)
CreatedSpecList = []

def KeysMaping(Program, InputKeys, SpecTemplate, SpecFile, Response):
    SpecTemplate = SpecTemplate + '.json'
    SpecFile = SpecFile + '.json'
    Program = Program + '_Specs'
    ### creating folder with program name
    if not os.path.exists(root_path+'/'+Program):
        os.makedirs(root_path+'/'+Program)
    #### deleting Grammar file if already exists with the same name
    if os.path.exists(root_path + '/' + Program + '/' + SpecFile):
        os.remove(root_path + '/' + Program + '/' + SpecFile)
    #### reading grammar template
    with open(root_path + '/templates/' + SpecTemplate, 'r') as fs:
        ValueOfTemplate = fs.readlines()
    if (len(InputKeys) != 0):
        ### iterating lines in the template
        for valueOfTemplate in ValueOfTemplate:
            ToReplaceString = valueOfTemplate

            #### finding replacing string "{string}" in iterating line
            TemplateKeys = re.findall("(?<=<)(.*?)(?=>)", ToReplaceString)
            for key in TemplateKeys:
                replaceStr = '<' + key + '>'
                ### replacing inputkeys values with replacing string
                ToReplaceString = ToReplaceString.replace(replaceStr, str(InputKeys[key]))

            ### writing into json file after replacing
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + Program + '/' + SpecFile, 'a') as fs:
                fs.write(ToReplaceString)

        ### collecting generated file list
        CreatedSpecList.append({"filename": SpecFile})
    else:
        print('ERROR : InputKey is empty')
        return Response(json.dumps({"Message": "InputKey is empty"}))

InputKeys = {}


def EventSpec(request, Response):
    Template = "Event"
    ###
    Program = request.json['program']
    EventKeys = request.json['key_file']
    ValidationKeys = request.json['validation_keys']
    ########## Reading additional validation csv file ###########
    try:
        df_validation = pd.read_csv(root_path+ "/key_files/" + ValidationKeys)
        ### Dataframe empty check
        if len(df_validation) == 0:
            return Response(json.dumps({"Message": ValidationKeys + " is empty"}))
        ### collecting validation values to list
        ValidationItems = df_validation.values.tolist()
        ### converting Validation_key_file values into key value pair
        ValidationColList = []
        ValidationList = []
        for item in ValidationItems:
            ValidationColList.append(item[0])
            ValidationList.append(item[1])
        validation_dict = (dict(zip(ValidationColList, ValidationList)))

        ########## Reading  Eventtkeys csv file #################
        df_event = pd.read_csv(root_path + "/key_files/" + EventKeys)
        program_list=df_event['program'].values.tolist()
        if len(df_event) == 0:
            return Response(json.dumps({"Message": EventKeys + " is empty"}))
        if Program not in program_list:
            return Response(json.dumps({"Message": "Invalid Program name", "Program": Program}))
        df_event = df_event.loc[df_event['program'] == Program]
        E_keys = df_event.keys().tolist()
        event_items = df_event.values.tolist()
        for value in event_items:
            event = (dict(zip(E_keys, value)))
            EventName = event['event_name']
            EventColumn = [x.strip() for x in event['event_col'].split(',')]
            DataTypes = [x.strip() for x in event['event_datatype'].split(',')]
            EventDict = dict(zip(EventColumn, DataTypes))
            ColumnsDataType = []
            for event_col in EventColumn:
                if 'date' in event_col.casefold():
                    ColumnsDataType.append({"type":EventDict[event_col].strip(), "shouldnotnull": True, "format": "date"})
                elif (event_col.casefold() == 'grade') | (event_col.casefold() == 'class'):
                    ColumnsDataType.append({"type": EventDict[event_col].strip(), "shouldnotnull": True, "minimum": 1, "maximum": 12})
                elif 'year' in event_col.casefold():
                    ColumnsDataType.append(
                        {"type":EventDict[event_col].strip(), "shouldnotnull": True, "minimum": ((todays_date.year) - 5),
                         "maximum": int(todays_date.year)})
                elif event_col in ValidationColList:
                    ColumnsDataType.append({"type": EventDict[event_col].strip(), "shouldnotnull": True,"pattern": "^[a-z,A-Z,0-9]{"+str(validation_dict[event_col])+"}$"})
                else:
                    ColumnsDataType.append({"type": EventDict[event_col].strip(), "shouldnotnull": True})
            InputKeys.update({"EventName": json.dumps(EventName),
                              "EventObject": json.dumps(dict(zip(EventColumn, ColumnsDataType))),
                              "EventList": json.dumps(EventColumn)})
            KeysMaping(Program, InputKeys, Template, EventName, Response)
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Spec not created", "SpecFiles":EventName, "code": 400}))
    return Response(json.dumps({"Message": "Spec created successfully", "SpecFiles": CreatedSpecList, "code": 200}))



def DimensionSpec(request, Response):
    Template = "Dimension"
    Program = request.json['program']
    DimensionKeys = request.json['key_file']
    ValidationKeys = request.json['validation_keys']

    ########## Reading additional validation csv file ###########
    try:
        df_validation = pd.read_csv(root_path+"/key_files/" + ValidationKeys)
        ### Dataframe empty check
        if len(df_validation) == 0:
            return Response(json.dumps({"Message": ValidationKeys + " is empty"}))
        ### collecting validation values to list
        ValidationItems = df_validation.values.tolist()
        ### converting Validation_key_file values into key value pair
        ValidationColList = []
        ValidationList = []
        for item in ValidationItems:
            ValidationColList.append(item[0])
            ValidationList.append(item[1])
        ValidationDict = (dict(zip(ValidationColList, ValidationList)))

        ########## Reading  DimensionKey csv file #################
        df_dimension = pd.read_csv(root_path + "/key_files/" + DimensionKeys)
        if len(df_dimension) == 0:
            return Response(json.dumps({"Message": DimensionKeys + " is empty"}))
        program_list=df_dimension['program'].drop_duplicates().tolist()
        if Program not in program_list:
             return Response(json.dumps({"Message":"Invalid program name","Program":Program}))
        df_dimension = df_dimension.loc[df_dimension['program'] == Program]
        DimensionCol = df_dimension.keys().tolist()
        DimensionValues = df_dimension.values.tolist()
        for value in DimensionValues:
            DimensionDict = (dict(zip(DimensionCol, value)))
            DimensionName = DimensionDict['dimension_name']
            DimensionColumn = [x.strip() for x in DimensionDict['dimension_col'].split(',')]
            DataTypes = [x.strip() for x in DimensionDict['dimension_datatype'].split(',')]
            TargetTable = [x.strip() for x in DimensionDict['target_table'].split(',')]
            DimensionDict = dict(zip(DimensionColumn, DataTypes))
            ColumnsDataType = []
            for dimension_col in DimensionColumn:
                if (dimension_col.casefold() == 'grade') | (dimension_col.casefold() == 'class'):
                    ColumnsDataType.append({"type":DimensionDict[dimension_col].strip(), "shouldnotnull": True, "minimum": 1, "maximum": 12})
                elif dimension_col in ValidationColList:
                    ColumnsDataType.append({"type": DimensionDict[dimension_col].strip(), "shouldnotnull": True,"pattern": "^[a-z,A-Z,0-9]{"+str(ValidationDict[dimension_col])+"}$"})
                else:
                    ColumnsDataType.append({"type": DimensionDict[dimension_col].strip(), "shouldnotnull": True})
            InputKeys.update({"DimensionName": json.dumps(DimensionName),
                              "DimensionObject": json.dumps(dict(zip(DimensionColumn, ColumnsDataType))),
                              "DimensionList": json.dumps(DimensionColumn),
                              "TargetTable": json.dumps(','.join(TargetTable))})
            KeysMaping(Program, InputKeys, Template, DimensionName, Response)
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Spec not created", "SpecFiles":DimensionName, "code": 400}))
    return Response(json.dumps({"Message": "Spec created successfully", "SpecFiles": CreatedSpecList, "code": 200}))


def DatasetSpec(request, Response):

    ### reading request body
    DatasetKeys = request.json['key_file']
    Program = request.json['program']
    ValidationKeys = request.json['validation_keys']
    ########## Reading additional validation csv file ###########
    df_validation = pd.read_csv(root_path + "/key_files/" + ValidationKeys)
    ### Dataframe empty check
    if len(df_validation) == 0:
        return Response(json.dumps({"Message": ValidationKeys + " is empty"}))
    ### collecting validation values to list
    ValidationItems = df_validation.values.tolist()
    ### converting Validation_key_file values into key value pair
    ValidationColList = []
    ValidationList = []
    for item in ValidationItems:
        ValidationColList.append(item[0])
        ValidationList.append(item[1])
    ValidationDict = (dict(zip(ValidationColList, ValidationList)))

    ########## Reading  Datasetkeys csv file #################
    df_dataset = pd.read_csv(root_path + "/key_files/" + DatasetKeys)
    program_list=df_dataset['program'].values.tolist()
    ### Dataframe empty check
    if len(df_dataset) == 0:
        return Response(json.dumps({"Message": DatasetKeys + " is empty"}))
    if Program not in program_list:
        return Response(json.dumps({"Message": "Invalid Program name", "Program": Program}))
    df_dataset = df_dataset.loc[df_dataset['program'] == Program]
    try:
        ### converting dataset_key_file (colums and rows)into key value pair
        DatasetColList = df_dataset.keys().tolist()
        DatasetValue = df_dataset.values.tolist()
        for value in DatasetValue:
            dataset = (dict(zip(DatasetColList, value)))
            DatasetName = dataset['dataset_name']
            SpecTemplate=dataset['spec_template']
            if SpecTemplate not in SpecTemplateList:
                return Response(json.dumps({"Message": "Template name is not correct", "Template": SpecTemplate, "Dataset": DatasetName}))
            ### Reading data from dataset_keys file and assigning to variables
            DimensionCol = [x.strip() for x in dataset['dimension_col'].split(',')]
            DimensionTable = [x.strip() for x in dataset['dimension_table'].split(',')]
            MergeOnCol = [x.strip() for x in dataset['merge_on_col'].split(',')]
            DatasetColumn = [x.strip() for x in dataset['dataset_col'].split(',')]
            DataTypes = [x.strip() for x in dataset['dataset_datatype'].split(',')]
            DatasetDict = dict(zip(DatasetColumn, DataTypes))  ## creating dict of datasetcol and datatypes
            GroupByCol = [x.strip() for x in str(dataset['group_by_col']).split(',')]
            AggFunction = [x.strip() for x in str(dataset['agg_function']).split(',')]
            TargetTable = [x.strip() for x in str(dataset['target_table']).split(',')]
            UpdateCol = [x.strip() for x in str(dataset['update_col']).split(',')]
            AggCol = [x.strip() for x in str(dataset['agg_col']).split(',')]
            AggColTable = [x.strip() for x in dataset['agg_col_table'].split(',')]
            FilterCol = [x.strip() for x in str(dataset['filter_col']).split(',')]
            FilterType = [x.strip() for x in str(dataset['filter_type']).strip('{}').split(',')]
            Filter = [x.strip() for x in str(dataset['filter']).split(',')]
            Numerator = [x.strip() for x in str(dataset['numerator']).split(',')]
            Denominator = [x.strip() for x in str(dataset['denominator']).split(',')]
            ColumnsDataType = []
            if len(DatasetColumn)!=len(DataTypes):
                return Response(json.dumps({'Message':'Length of dataset columns and datatypes are not matching '+DatasetName}))

            ### creating validation format
            for datasetcol in DatasetColumn:
                if 'date' in datasetcol.casefold():
                    ColumnsDataType.append({"type": DatasetDict[datasetcol].strip(), "shouldnotnull": True, "format": "date"})
                elif (datasetcol.casefold() == 'grade') | (datasetcol.casefold() == 'class'):
                    ColumnsDataType.append({"type":DatasetDict[datasetcol].strip(), "shouldnotnull": True, "minimum": 1, "maximum": 12})
                elif 'year' in datasetcol.casefold():
                    ColumnsDataType.append(
                        {"type": DatasetDict[datasetcol].strip(), "shouldnotnull": True, "minimum": ((todays_date.year) - 5),
                         "maximum": int(todays_date.year)})
                elif datasetcol in ValidationColList:
                    ColumnsDataType.append({"type": DatasetDict[datasetcol].strip(), "shouldnotnull": True, "pattern": "^[a-z,A-Z,0-9]{"+str(ValidationDict[datasetcol])+"}$"})
                else:
                    ColumnsDataType.append({"type": DatasetDict[datasetcol].strip(), "shouldnotnull": True})

            ### collecting mapping keys
            InputKeys.update({"DatasetName": json.dumps(DatasetName),"DatasetList": json.dumps(DatasetColumn),
                 "DatasetObject": json.dumps(dict(zip(DatasetColumn, ColumnsDataType))),"TargetTable":json.dumps(','.join(TargetTable)),
                 "DimensionTable": json.dumps(','.join(DimensionTable)),"DimensionCol": json.dumps(DimensionCol),"MergeOnCol":json.dumps(','.join(MergeOnCol)),
                 "GroupByCol": json.dumps(GroupByCol),"AggFunction": json.dumps(AggFunction),"AggCol": json.dumps(AggCol),
                 "UpdateCol": json.dumps(UpdateCol),"NumeratorCol":json.dumps(','.join(Numerator)),"DenominatorCol":json.dumps(','.join(Denominator))})

            if SpecTemplate in ["EventToCube","Dataset"]:
                InputKeys.update(InputKeys)
            elif SpecTemplate == "EventToCubeFilter":
                InputKeys.update({"FilterCol": json.dumps(','.join(FilterCol)),
                     "FilterType": json.dumps(','.join(FilterType)), "Filter": json.dumps(','.join(Filter))})
            elif SpecTemplate == "CubeToCube":
                InputKeys.update({"AggColTable":json.dumps(','.join(AggColTable))})
            elif SpecTemplate == "CubeToCubeFilter":
                InputKeys.update({"AggColTable":json.dumps(','.join(AggColTable)),"FilterCol":json.dumps(','.join(FilterCol)),
                                  "FilterType":json.dumps(','.join(FilterType)),"Filter":json.dumps(','.join(Filter))})
            else:
                return Response(json.dumps({"Message": "Template name is not correct", "Template": SpecTemplate}))
            KeysMaping(Program, InputKeys, SpecTemplate, DatasetName, Response)
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Spec not created", "SpecFiles":DatasetName, "code": 400}))
    return Response(json.dumps({"Message": "Spec created successfully", "SpecFiles": CreatedSpecList, "code": 200}))
