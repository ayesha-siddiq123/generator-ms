from flask import Flask, request, Response
from transformer_keys_mapping import *
from spec_key_mapping import *

app = Flask(__name__)


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
