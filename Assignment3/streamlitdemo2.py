import streamlit
import streamlit as st
import pandas as pd
import numpy as np
import boto3
import os
from configparser import ConfigParser

from sdgym.synthesizers import UniformSynthesizer, IndependentSynthesizer, IdentitySynthesizer, CLBNSynthesizer

config = ConfigParser()
config.read('config.ini')
access_key = config.get('aws', 'access_key')
secret_access_key = config.get('aws', 'secret_access_key')
session_token = config.get('aws', 'session_token')

st.title('Data Explorer & Synthesizer')


user_input = st.text_input("Link to Data File", 'https://csye7245-1.s3.amazonaws.com/data/data.csv')


def load_data():
    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIAIL7ELCOTK5XDIBWQ',
                        aws_secret_access_key='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')

    s3_path = 'analyze/' + os.path.basename(user_input)
    s3.Bucket('csye7245-1').download_file(
        s3_path, 'down_data.csv')

    s3.Bucket('csye7245-1').download_file(
        s3_path, 'down_data.csv')

    print('Input Data Downloaded')

    data = pd.read_csv('down_data.csv', header=None)
    return data


def uniform():
    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)
    categorical_columns = []
    ordinal_columns = []
    synthesizer = UniformSynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)
    sampled = synthesizer.sample(4000)
    np.savetxt("41_uniform.csv", sampled, delimiter=",")
    data = pd.read_csv('41_uniform.csv', header=None)

    return data


def evaluatefun():
    from sdgym.synthesizers import IndependentSynthesizer
    from sdgym.evaluate import evaluate

    from sdgym.data import load_dataset
    train, test, meta, categoricals, ordinals = load_dataset('adult', benchmark=True)
    synthesizer = IndependentSynthesizer()
    synthesizer.fit(train, categoricals, ordinals)
    sampled = synthesizer.sample(300)
    print('Sampled Data for 300 records\n')
    scores = evaluate(train, test, sampled, meta)
    scores['Synth'] = 'IdentitySynthesizer'
    scores2 = (evaluate(train, test, sampled, meta))
    scores2['Synth'] = 'Uniform'
    scores3 = (evaluate(train, test, sampled, meta))
    scores3['Synth'] = 'Identity'
    scores4 = (evaluate(train, test, sampled, meta))
    scores4['Synth'] = 'CLBN'
    print('\nEvaluation Scores from evaluate function:\n')

    result = scores.append(scores2)
    result = result.append(scores3)
    result = result.append(scores4)

    a = (result[result['accuracy'] == result['accuracy'].max()])

    st.write('Best Performing Synthsizer: ' + str(a['Synth'].item()))
    st.write('Accuracy: ' + str(a['accuracy'].item()))

    st.dataframe(result)


def independent():
    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)
    categorical_columns = []
    ordinal_columns = []
    synthesizer = IndependentSynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)

    sampled = synthesizer.sample(4000)
    np.savetxt("41_independent.csv", sampled, delimiter=",")

    data = pd.read_csv('41_independent.csv', header=None)

    return data


def identity():
    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)
    categorical_columns = []
    ordinal_columns = []
    synthesizer = IdentitySynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)

    sampled = synthesizer.sample(4000)
    np.savetxt("41_identity.csv", sampled, delimiter=",")

    data = pd.read_csv('41_identity.csv', header=None)

    return data


def CLBN():
    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)
    categorical_columns = []
    ordinal_columns = []
    synthesizer = CLBNSynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)
    sampled = synthesizer.sample(4000)
    np.savetxt("41_clbn.csv", sampled, delimiter=",")

    data = pd.read_csv('41_clbn.csv', header=None)
    return data


if st.button('Load Data'):
    # Create a text element and let the reader know the data is loading.
    data_load_state = st.text('Loading data...')
    # Load 10,000 rows of data into the dataframe.
    data = load_data()
    # Notify the reader that the data was successfully loaded.
    data_load_state.text('Loading data...done!')
    st.dataframe(data)


import streamlit as st

st.sidebar.title("Synthesize Data")
radio = st.sidebar.radio(label="", options=["Uniform", "Independent", "Identity", "CLBN"])

st.subheader('Synthesized Output')
if radio == "Uniform":
    synth_state = st.text('Generating Data...')
    a = uniform()
    synth_state.text('Completed!')
    if st.checkbox('Show Synthesized Data'):
        st.dataframe(a)

if radio == "Independent":
    synth_state = st.text('Generating Data...')
    a = independent()
    synth_state.text('Completed!')
    if st.checkbox('Show Synthesized Data'):
        st.dataframe(a)

if radio == "Identity":
    synth_state = st.text('Generating Data...')
    a = identity()
    synth_state.text('Completed!')
    if st.checkbox('Show Synthesized Data'):
        st.dataframe(a)

if radio == "CLBN":
    synth_state = st.text('Generating Data...')
    a = CLBN()
    synth_state.text('Completed!')
    if st.checkbox('Show Synthesized Data'):
        st.dataframe(a)

st.subheader("Benchmarking")
if st.checkbox('Benchmark Data'):
    synth_state = st.text('Benchamarking Data...')
    streamlit.spinner(text='In progress...')
    evaluatefun()
    synth_state.text('Benchmarking Completed!')
