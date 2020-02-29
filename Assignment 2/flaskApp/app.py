import json

import boto3
import numpy as np
import tensorflow as tf
from flask import Flask, jsonify, request
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.preprocessing.text import Tokenizer

app = Flask(__name__)


@app.route('/input', methods=['POST'])
def service():
    data = json.loads(request.data)
    text = data.get("text", None)
    print(text)

    json_file = open('model.json', 'r')
    loaded_model_json = json_file.read()
    json_file.close()

    loaded_model = tf.keras.models.model_from_json(loaded_model_json)
    # classifierLoad = tf.keras.models.load_model('model.h5')

    loaded_model.load_weights('model.h5')

    loaded_model.compile(loss='binary_crossentropy',
                         optimizer='adam',
                         metrics=['accuracy'])

    # model parameters

    vocab_size = 1000
    embedding_dim = 16
    max_length = 120
    trunc_type = 'post'
    padding_type = 'post'
    oov_tok = "<OOV>"
    training_portion = .7

    listOfSent = []
    listOfSent.append(text)
    samples = np.asarray(listOfSent)

    tokenizer = Tokenizer(num_words=vocab_size, oov_token=oov_tok)
    tokenizer.fit_on_texts(samples)
    word_index = tokenizer.word_index

    sample_seq = tokenizer.texts_to_sequences(samples)
    sample_padded = pad_sequences(sample_seq, padding=padding_type, maxlen=max_length)

    pred = loaded_model.predict(sample_padded)

    if text is None:
        return jsonify({"message": "text not found"})
    else:
        return jsonify(str(pred[0]))


@app.route('/download')
def service2():
    s3 = boto3.resource('s3',
                        aws_access_key_id='',
                        aws_secret_access_key='')

    s3.Bucket('assignment2swarna').download_file(
        'model/model.json', 'model.json')

    s3.Bucket('assignment2swarna').download_file(
        'model/model.h5', 'model.h5')

    print('Models Downloaded')
    return 'Done'


if __name__ == '__main__':
    app.run(host ='0.0.0.0', port = 5001)
