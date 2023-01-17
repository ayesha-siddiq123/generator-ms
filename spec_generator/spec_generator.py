import json
from flask import Flask,request,Response
from spec_key_mapping import EventSpec,DimensionSpec,DatasetSpec
app = Flask(__name__)


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


if (__name__ == "__main__"):
    app.run(debug=True,port=3002)
