import json
from flask import Flask, jsonify, request
import time
app = Flask(__name__)


@app.route('/input', methods=['POST'])
def service():
    data = json.loads(request.data)
    text = data.get("text", None)
    print('Downloading File')
    print(text)
    time.sleep(5)
    message = 'S3 File downloaded from ' + text + ' Now triggering Airflow DAG...'
 
    return jsonify(message)

if __name__ == '__main__':
    app.run(host ='0.0.0.0', port = 5000)
