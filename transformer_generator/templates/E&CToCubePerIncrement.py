import pandas as pd
from db_connection import *
from file_tracker_status import *
from datetime import date

con,cur=db_connection()

def aggTransformer(valueCols={ValueCols}):
    file_check('{KeyFile}','event')
    df_event= pd.read_csv(os.path.dirname(root_path)+"processing_data/{KeyFile}")
    df_dataset = pd.read_sql('select * from {Table};', con=con)
    {DateFilter}
    {YearFilter}
    string_list = [col for col, dt in df_dataset.dtypes.items() if dt == object]
    df_dataset.update(df_dataset[string_list].applymap("'{Values}'".format))
    df_dimension = pd.read_sql('select {DimensionCols} from {DimensionTable}', con=con)
    df_dimension.update(df_dimension[{DimColCast}].applymap("'{Values}'".format))
    event_dimension_merge = df_event.merge(df_dimension, on=['{MergeOnCol}'], how='inner')
    event_dimension_merge = event_dimension_merge.groupby({GroupBy}, as_index=False).agg({AggCols})
    event_dimension_merge['{RenameCol}'] = event_dimension_merge['{eventCol}']
    merge_col_list = []
    for i in event_dimension_merge.columns.to_list():
        if i in df_dataset.columns.to_list():
            merge_col_list.append(i)
    df_agg = event_dimension_merge.merge(df_dataset, on=merge_col_list, how='inner')
    df_agg['percentage'] = ((df_agg['{NumeratorCol}'] / df_agg['{DenominatorCol}']) * 100)  ### Calculating Percentage
    df_snap = df_agg[valueCols]
    print(df_snap)
    try:
        for index, row in df_snap.iterrows():
            values = []
            for i in valueCols:
                values.append(row[i])
            query = ''' INSERT INTO {TargetTable} As main_table({InputCols}) VALUES ({Values}) ON CONFLICT ({ConflictCols}) DO UPDATE SET {IncrementFormat},percentage=(({QueryNumerator})/({QueryDenominator}))*100;'''.format(','.join(map(str, values)),{UpdateCols})
            print(query)
            cur.execute(query)
            con.commit()
        status_track('{KeyFile}', 'event', 'Completed_{DatasetName}')

    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if con is not None:
            con.close()

aggTransformer()
