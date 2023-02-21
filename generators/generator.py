import json

from flask import Flask, request, Response
from transformer_keys_mapping import *
from spec_key_mapping import *
from add_static_processor_group import *

app = Flask(__name__)


@app.route('/static_processor_group_creation', methods=['POST'])
def create_processor_group():
    try:
        add_pg_file_moving = call_file_moving_pg('File_moving')
        add_pg_update_api = call_update_api_status_pg('update_api_status')
        add_pg_s3_archive = s3_configuration('UploadToArchiveS3')
        add_pg_s3_error = s3_configuration('UploadToErrorS3')
        result = {"Message": "Processor groups successfully created"}
        return add_pg_file_moving,add_pg_update_api,add_pg_s3_archive,add_pg_s3_error,Response(json.dumps(result))
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Processor group not created"}))

@app.route('/generator', methods=['POST'])
def TransformerGenerator():
    operation = request.json['operation']
    try:
        if operation == 'dataset':
            return collect_dataset_keys(request, Response)
        elif operation == 'dimension':
            return collect_dimension_keys(request, Response)
        else:
            return Response(json.dumps({"Message": "Invalid operation"}))
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Transformer not created"}))
@app.route('/generator/spec',methods=['POST'])
def SpecGenerator():
    spec_type=request.json['spec_type']
    try:
        if(spec_type=='EventSpec'):
            return EventSpec(request,Response)

        elif(spec_type=='DimensionSpec'):
            return DimensionSpec(request,Response)

        elif (spec_type == 'DatasetSpec'):
             return DatasetSpec(request,Response)
        else:
            return Response(json.dumps({"Message": "Spec Type is not correct"}))
    except Exception as error:
        print(error)
        return  Response(json.dumps({"Message": "Given Input Is Not Correct"}))


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=3003)
