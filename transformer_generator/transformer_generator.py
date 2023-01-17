import json
from flask import Flask, request, Response
from transformer_keys_mapping import collect_keys, dimension_data_insert

app = Flask(__name__)


@app.route('/generator', methods=['POST'])
def TransformerGenerator():
    try:
        if request.json['operation'] == 'dataset':
            return collect_keys(request, Response)
        elif request.json['operation'] == 'dimension':
            return dimension_data_insert(request, Response)
    except Exception as error:
        print(error)
        return Response(json.dumps({"Message": "Transformer not created"}))


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=3003)
