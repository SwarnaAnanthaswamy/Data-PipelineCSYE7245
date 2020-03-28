import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def start():
    import time

    def initializing():
        time.sleep(10)
        print("Done")

    initializing()

def getData():
    import boto3

    from configparser import ConfigParser

    # config = ConfigParser()
    # config.read('config.ini')

    # access_key = 'ASIA6FLRIBYZ6W254X4N'
    # secret_access_key = 'btjelUWEGgJNe9aSwvFVu3ryXI1vYBsJEvpgPMJX'
    # session_token = 'FwoGZXIvYXdzELD//////////wEaDASt6s7PS2tyec2vAyLHAaMtBIIQkTI8fOjNK/5rxKwTvXGY4ZNohhcxiRICpUNuXQCRSbqJUjL3MGkEDkk7vpaX1yM2xlmeZNyrz6yi342darSs066Bn2TXkyCQpbnBpflRizqqygGEqg3P8uE7hPgCJgnTD3G4X1PdIbFt7jFQPrPCApfoz7qVxA1sOgHmgKVp5vcSKCxJrQzWFT7dlp4MC7AW4P3pKCiKV3NnBOPxkou23UcO2/yG6UALb9+Dn9MeFEs7I3FBGsKbw332mfaSdkqWCqkoi9T08wUyLSWEMSD4s5f34cuD2pPe6PbACkLQ5iVfZTQfn6cGiByqbgvOMf0aWLuFTLObSA=='

    # access_key = config.get('aws', 'access_key')
    # secret_access_key = config.get('aws', 'secret_access_key')
    # session_token = config.get('aws', 'session_token')
    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIAXS4YFCA64NXDUSGG',
                        aws_secret_access_key='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',
                        )

    s3.Bucket('csye7245-1').download_file(
        'data/data.csv', 'down_data.csv')

    print('Input Data Downloaded')


def generate_metadata():
    import csv
    import numpy as np
    import pandas as pd
    from numpy import genfromtxt
    import json
    import boto3

    df = pd.read_csv('down_data.csv')

    jsonParse = '{ "columns": ['

    for (columnName, columnData) in df.iteritems():
        print('Colunm Name : ', columnName)
        #    print('Column Contents : ', columnData.values)
        if pd.to_numeric(df[columnName], errors='coerce').notnull().all():
            print('Numeric')
            jsonParse = jsonParse + '{"max": ' + str(columnData.max()) + ',' + '"min": ' + str(
                columnData.min()) + ',' + '"name": "' + columnName + '" , "type": "continuous"},'
            print(columnData.max())
            print(columnData.min())

        else:
            print('NonNumeric')
            jsonParse = jsonParse + ' {"i2s": ['
            for i in columnData.unique():
                print(i)
                jsonParse = jsonParse + '"' + i + '",'

            jsonParse = jsonParse[:-1] + '],' + '"name": "' + columnName + '", "size": ' + str(
                len(columnData.unique())) + ' , "type": "categorical"},'

    jsonParse = jsonParse[:-1] + '], "problem_type": "binary_classification"}'

    final_dictionary = eval(jsonParse)

    with open('generated_metadata.json', 'w') as outfile:
        json.dump(final_dictionary, outfile)


def uploadData():
    # from configparser import ConfigParser
    import boto3

    # config = ConfigParser()
    # config.read('config.ini')

    # access_key = config.get('aws', 'access_key')
    # secret_access_key = config.get('aws', 'secret_access_key')
    # session_token = config.get('aws', 'session_token')

    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIAXS4YFCA64NXDUSGG',
                        aws_secret_access_key='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',
                        )

    s3.Bucket('csye7245-1').upload_file(
        'generated_metadata.json', 'meta/generated_metadata.json')

    print('Metadata Uploaded to S3')


def uniformSynth():
    import numpy as np
    from sdgym.constants import CATEGORICAL, ORDINAL
    import json
    from sdgym.synthesizers import UniformSynthesizer
    # from configparser import ConfigParser
    import boto3

    # config = ConfigParser()
    # config.read('config.ini')

    # access_key = config.get('aws', 'access_key')
    # secret_access_key = config.get('aws', 'secret_access_key')
    # session_token = config.get('aws', 'session_token')
    with open('generated_metadata.json') as data_file:
        data = json.load(data_file)

    categorical_columns = list()
    ordinal_columns = list()

    for column_idx, column in enumerate(data['columns']):

        if column['type'] == CATEGORICAL:
            print(column)
            print('Classified as Categorical')
            categorical_columns.append(column_idx)
        elif column['type'] == ORDINAL:
            ordinal_columns.append(column_idx)
            print(column)
            print('Classified as Ordinal')

    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)
    synthesizer = UniformSynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)

    sampled = synthesizer.sample(4000)
    np.savetxt("41_uniform.csv", sampled, delimiter=",")
    print(sampled)

    print('Data Synthesized using Uniform Synthesizer')

    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIAXS4YFCA64NXDUSGG',
                        aws_secret_access_key='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',
                        )

    s3.Bucket('csye7245-1').upload_file(
        '41_uniform.csv', 'synth/41_uniform.csv')

    print('Synthesized(Uniform) Data Uploaded to S3')


def independentSynth():
    import numpy as np
    from sdgym.constants import CATEGORICAL, ORDINAL
    import json
    from sdgym.synthesizers import IndependentSynthesizer
    from configparser import ConfigParser
    import boto3

    with open('generated_metadata.json') as data_file:
        data = json.load(data_file)

    categorical_columns = list()
    ordinal_columns = list()

    for column_idx, column in enumerate(data['columns']):

        if column['type'] == CATEGORICAL:
            print(column)
            print('Classified as Categorical')
            categorical_columns.append(column_idx)
        elif column['type'] == ORDINAL:
            ordinal_columns.append(column_idx)
            print(column)
            print('Classified as Ordinal')

    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)
    synthesizer = IndependentSynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)

    sampled = synthesizer.sample(4000)
    np.savetxt("42_independent.csv", sampled, delimiter=",")
    print(sampled)

    print('Data Synthesized using Independent Synthesizer')

    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIAXS4YFCA64NXDUSGG',
                        aws_secret_access_key='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',
                        )

    s3.Bucket('csye7245-1').upload_file(
        '42_independent.csv', 'synth/42_independent.csv')

    print('Synthesized(Independent) Data Uploaded to S3')


def identitySynth():
    import numpy as np
    from sdgym.constants import CATEGORICAL, ORDINAL
    import json
    from sdgym.synthesizers import IdentitySynthesizer
    from configparser import ConfigParser
    import boto3

    with open('generated_metadata.json') as data_file:
        data = json.load(data_file)

    categorical_columns = list()
    ordinal_columns = list()

    for column_idx, column in enumerate(data['columns']):

        if column['type'] == CATEGORICAL:
            print(column)
            print('Classified as Categorical')
            categorical_columns.append(column_idx)
        elif column['type'] == ORDINAL:
            ordinal_columns.append(column_idx)
            print(column)
            print('Classified as Ordinal')

    # return categorical_columns, ordinal_columns

    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)
    synthesizer = IdentitySynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)

    sampled = synthesizer.sample(4000)
    np.savetxt("43_identity.csv", sampled, delimiter=",")
    print(sampled)

    print('Data Synthesized using Identity synthesizer')

    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIAXS4YFCA64NXDUSGG',
                        aws_secret_access_key='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',
                        )

    s3.Bucket('csye7245-1').upload_file(
        '43_identity.csv', 'synth/43_identity.csv')

    print('Synthesized(Identity) Data Uploaded to S3')


def CLBNSynth():
    import numpy as np
    from sdgym.constants import CATEGORICAL, ORDINAL
    import json
    from sdgym.synthesizers import CLBNSynthesizer
    from configparser import ConfigParser
    import boto3

    with open('generated_metadata.json') as data_file:
        data = json.load(data_file)

    categorical_columns = list()
    ordinal_columns = list()

    for column_idx, column in enumerate(data['columns']):

        if column['type'] == CATEGORICAL:
            print(column)
            print('Classified as Categorical')
            categorical_columns.append(column_idx)
        elif column['type'] == ORDINAL:
            ordinal_columns.append(column_idx)
            print(column)
            print('Classified as Ordinal')

    # return categorical_columns, ordinal_columns

    data = np.loadtxt('down_data.csv', delimiter=',', skiprows=1)
    synthesizer = CLBNSynthesizer()
    synthesizer.fit(data, categorical_columns, ordinal_columns)

    sampled = synthesizer.sample(4000)
    np.savetxt("44_CLBN.csv", sampled, delimiter=",")
    print(sampled)

    print('Data Synthesized using CLBN Synthesizer')

    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIAXS4YFCA64NXDUSGG',
                        aws_secret_access_key='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',
                        )

    s3.Bucket('csye7245-1').upload_file(
        '44_CLBN.csv', 'synth/44_CLBN.csv')

    print('Synthesized(CLBN) Data Uploaded to S3')

def benchUnifrom():
    from sdgym.synthesizers import IndependentSynthesizer, UniformSynthesizer
    from sdgym.evaluate import evaluate


    from sdgym.data import load_dataset
    train, test, meta, categoricals, ordinals = load_dataset('adult', benchmark=True)
    synthesizer = UniformSynthesizer()
    synthesizer.fit(train, categoricals, ordinals)
    sampled = synthesizer.sample(300)
    scores = evaluate(train, test, sampled, meta)
    scores = scores.append(evaluate(train, test, sampled, meta))
    scores = scores.append(evaluate(train, test, sampled, meta))
    print('\nEvaluation Scores from evaluate function:\n')
    print (scores)
    scores['Synth'] = 'Uniform'
    scores.to_csv('UniformBench.csv')

def benchIndependentSynthesizer():
    from sdgym.synthesizers import IndependentSynthesizer, UniformSynthesizer
    from sdgym.evaluate import evaluate


    from sdgym.data import load_dataset
    train, test, meta, categoricals, ordinals = load_dataset('adult', benchmark=True)
    synthesizer = IndependentSynthesizer()
    synthesizer.fit(train, categoricals, ordinals)
    sampled = synthesizer.sample(300)
    scores = evaluate(train, test, sampled, meta)
    scores = scores.append(evaluate(train, test, sampled, meta))
    scores = scores.append(evaluate(train, test, sampled, meta))
    print('\nEvaluation Scores from evaluate function:\n')
    print (scores)
    scores['Synth'] = 'IndependentSynthesizer'
    scores.to_csv('IndependentBench.csv')

def benchIdentitySynthesizer():
    from sdgym.synthesizers import IdentitySynthesizer
    from sdgym.evaluate import evaluate


    from sdgym.data import load_dataset
    train, test, meta, categoricals, ordinals = load_dataset('adult', benchmark=True)
    synthesizer = IdentitySynthesizer()
    synthesizer.fit(train, categoricals, ordinals)
    sampled = synthesizer.sample(300)
    scores = evaluate(train, test, sampled, meta)
    scores = scores.append(evaluate(train, test, sampled, meta))
    scores = scores.append(evaluate(train, test, sampled, meta))
    print('\nEvaluation Scores from evaluate function:\n')
    print (scores)
    scores['Synth'] = 'IdentitySynthesizer'
    scores.to_csv('IdentityBench.csv')

def benchCLBNSynthesizer():
    from sdgym.synthesizers import CLBNSynthesizer
    from sdgym.evaluate import evaluate


    from sdgym.data import load_dataset
    train, test, meta, categoricals, ordinals = load_dataset('adult', benchmark=True)
    synthesizer = CLBNSynthesizer()
    synthesizer.fit(train, categoricals, ordinals)
    sampled = synthesizer.sample(300)
    scores = evaluate(train, test, sampled, meta)
    scores = scores.append(evaluate(train, test, sampled, meta))
    scores = scores.append(evaluate(train, test, sampled, meta))
    print('\nEvaluation Scores from evaluate function:\n')
    print (scores)
    scores['Synth'] = 'CLBNSynthesizer'
    scores.to_csv('CLBNBench.csv')

def compareBenchmarks():
    import pandas as pd
    import glob
    import boto3

    s3 = boto3.resource('s3',
                        aws_access_key_id='AKIAXS4YFCA64NXDUSGG',
                        aws_secret_access_key='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',
                        )

    s3.Bucket('csye7245-1').download_file(
        'data/data.csv', 'down_data.csv')

    all_files = glob.glob("*Bench*.csv")

    print(all_files)
    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    print(frame)

    a = (df[df['accuracy'] == df['accuracy'].max()])

    bestSynth = a['Synth'].item()

    print(bestSynth)

    if bestSynth == 'Uniform':
        s3.Bucket('csye7245-1').upload_file(
            '41_uniform.csv', 'output/41_uniform.csv')

    elif bestSynth == 'IndependentSynthesizer':
        s3.Bucket('csye7245-1').upload_file(
            '42_independent.csv', 'output/42_independent.csv')

    elif bestSynth == 'IdentitySynthesizer':
        s3.Bucket('csye7245-1').upload_file(
            '43_identity.csv', 'output/43_identity.csv')

    elif bestSynth == 'CLBNSynthesizer':
        s3.Bucket('csye7245-1').upload_file(
            '44_CLBN.csv', 'output/44_CLBN.csv')

    frame.to_csv('output_benchmarkedResults.csv')
    s3.Bucket('csye7245-1').upload_file(
        'output_benchmarkedResults.csv', 'output/output_benchmarkedResults.csv')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'concurrency': 1,
    'retries': 0,
    'depends_on_past': False,
    # 'email': ['ananthaswamy.s@northeastern.edu'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
}

with DAG('Assignment3-Final',
         catchup=False,
         default_args=default_args,
         schedule_interval='@once',
         ) as dag:
    t0_start=PythonOperator(task_id='start',
                            python_callable=start)
    t1_getData = PythonOperator(task_id='getData',
                                python_callable=getData)
    t2_generatemetadata = PythonOperator(task_id='generateMetadata',
                                         python_callable=generate_metadata)
    t3_uploadmetadata = PythonOperator(task_id='uploadMetadata',
                                       python_callable=uploadData)
    t4_1_uniformSynth = PythonOperator(task_id='uniformSynth',
                                       python_callable=uniformSynth)
    t4_2_independentSynth = PythonOperator(task_id='independentSynth',
                                           python_callable=independentSynth)
    t4_3_identitySynth = PythonOperator(task_id='identitySynth',
                                        python_callable=identitySynth)
    t4_4_CLBNSynth = PythonOperator(task_id='CLBNSynth',
                                    python_callable=CLBNSynth)
    t5_1_benchUnifrom = PythonOperator(task_id='benchUnifrom',
                                    python_callable=benchUnifrom)
    t5_2_benchIndependent = PythonOperator(task_id='benchIndependent',
                                    python_callable=benchIndependentSynthesizer)
    t5_3_benchIdentity = PythonOperator(task_id='benchIdentity',
                                    python_callable=benchIdentitySynthesizer
                                        )
    t5_4_benchCLBN = PythonOperator(task_id='benchCLBN',
                                    python_callable=benchCLBNSynthesizer)
    t6_compareBenchmarks=PythonOperator(task_id='compareBenchmarks',
                                        python_callable=compareBenchmarks)



t0_start >> t1_getData >> t2_generatemetadata >> t3_uploadmetadata >> [t4_1_uniformSynth  , t4_2_independentSynth , t4_3_identitySynth ,t4_4_CLBNSynth]
t4_1_uniformSynth >> t5_1_benchUnifrom
t4_2_independentSynth >> t5_2_benchIndependent
t4_3_identitySynth >> t5_3_benchIdentity
t4_4_CLBNSynth >> t5_4_benchCLBN
[t5_1_benchUnifrom,t5_2_benchIndependent,t5_3_benchIdentity,t5_4_benchCLBN] >> t6_compareBenchmarks