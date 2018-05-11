from flask import Flask, request
from flask_cors import CORS
import json
import requests
from elasticsearch import Elasticsearch, NotFoundError

app = Flask(__name__)
CORS(app)

es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

@app.route('/')
def home():
    res = json.loads(requests.get('http://localhost:9200').content)
    response = app.response_class(
        response=json.dumps(res),
        status=200,
        mimetype='application/json'
    )
    return response


@app.route('/pipeline', methods=['POST'])
def pipeline():
    es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
    data = request.get_json()

    pipeline = data['pipeline']

    # Here we gonna try to create a pipeline, this is one unique time.
    body = {
        "description": "Extract attachment information.",
        "processors": [
            {
                "attachment": {
                    "field": "data"
                }
            }
        ]
    }

    try:
        response = es.index(index='_ingest', doc_type='pipeline', id='attachment', body=body)
        json_response = app.response_class(
            response=json.dumps(response),
            status=200,
            mimetype='application/json'
        )
        return json_response
    except NotFoundError:
        json_response = app.response_class(
            response='Not found',
            status=400,
            mimetype='application/json'
        )
        return json_response


@app.route('/new_index', methods=['POST'])
def new_index():
    # Try create an index,
    # First check if pipeline exists

    # Connect to our cluster
    es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])
    exists = es.exists_source(index='apsys', doc_type='test')
    if exists:
        json_response = app.response_class(
            response='',
            status=418,
            mimetype='application/json'
        )
        return json_response
    else:
        data = request.get_json()
        response = es.index(index='apsys', doc_type=data['doc_type'], body={'data': data['data']})
        print(response)
        jsonResponse = app.response_class(
            response=json.dumps(response),
            status=200,
            mimetype='application/json'
        )
        return jsonResponse


@app.route('/index_attachment', methods=['POST'])
def index_attachment():
    # Indexing new PDF Base64 string
    data = request.get_json()
    result = es.index(index='company', doc_type=data['doc_type'], pipeline='attachment', body={'data': data['base64']})
    print(result)
    json_response = app.response_class(
        response=json.dumps(result),
        status=200,
        mimetype='application/json'
    )
    return json_response
    pass


@app.route('/search', methods=['POST'])
def search():
    query = request.get_json()
    print(query['index'])
    print(query['doc_type'])
    print(query['search'])

    if query['index'] == '' or query['doc_type'] == '' or query['search'] == '':
        print("Some value is empty:", query['index'], query['doc_type'], query['search'])
        if query['index'] == '':
            print('index is empty')
        elif query['doc_type'] == '':
            print("doc type is empty")
            body = {
                "query": {
                    "prefix": {
                        "content": query['search']
                    }
                }
            }
            search_result = es.search(index=query['index'], body=body)
            print(search_result)
            json_response = app.response_class(
                response=json.dumps(search_result),
                status=200,
                mimetype='application/json'
            )
            return json_response


        elif query['search'] == '':
            print('Search is empty')

            search_result = es.search(index=query['index'], doc_type=query['doc_type'])

            json_response = app.response_class(
                response=json.dumps(search_result),
                status=200,
                mimetype='application/json'
            )
            return json_response
    else:
        search_result = es.search(index=query['index'], doc_type=query['doc_type'], q=query['search'], _source_exclude=['data'])
        print(search_result)
        jsonResponse = app.response_class(
            response=json.dumps(search_result),
            status=200,
            mimetype='application/json'
        )
        return jsonResponse


if __name__ == '__main__':
    app.run(threaded=True, host='0.0.0.0')
