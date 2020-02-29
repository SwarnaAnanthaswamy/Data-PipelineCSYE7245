from metaflow import FlowSpec, step
from bs4 import BeautifulSoup
import os
import re
import boto3
import nltk
import nltk.data
import pandas as pd
import csv
import requests
import json

class ScrapeEdgar(FlowSpec):

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """
        print("Starting Inference Pipeline")
        self.next(self.downloadfile)

    @step
    def downloadfile(self):
        """
        Step 1 : The input file(with CIK,Year) and the Metadata(CIK Ticker look-up) file are downloaded from AWS S3 bucket
        """
        print('Connecting to S3 Bucket ')
        s3 = boto3.resource('s3',
                            aws_access_key_id='',
                            aws_secret_access_key='')

        # Download Input file

        s3.Bucket('assignment2swarna').download_file(
            'input/input.csv', 'input.csv')
        print('Input file downloaded')

        # Download Metadata file
        s3.Bucket('assignment2swarna').download_file(
            'metadata/cik_ticker.csv', 'cik_ticker.csv')
        print('CIK Metadata file downloaded')
        self.next(self.checkinput)

    @step
    def checkinput(self):

        """
        Step 2 : To check if an input file is empty. If it is empty, the pipeline is terminated
        """
        df = pd.read_csv('input.csv')
        # if df.empty:
        #    print('The input file seems to be empty. Exiting pipeline')

        self.next(self.findrecords)

    @step
    def findrecords(self):

        """
        Step 3 : To map the input CIK to the ticker Meatadata file and upload the filtered records to S3 bucket
        """
        s3 = boto3.resource('s3',
                            aws_access_key_id='',
                            aws_secret_access_key='')

        usersDf = pd.read_csv('cik_ticker.csv', sep='|', engine='python')

        inputDF = pd.read_csv('input.csv')
        CIKList = inputDF['Company'].to_list()
        print(CIKList)

        df2 = (usersDf.loc[usersDf['Company'].isin(CIKList)])
        df3 = pd.merge(inputDF, df2, on='Company')
        print('Filtered Records:')
        print(df3)

        # if df2.empty:
        #   print('The CIK could not be found, exiting pipeline')
        # else:
        df3.to_csv('filteredRecords.csv', index=False)
        s3.Bucket('assignment2swarna').upload_file(
            'filteredRecords.csv', 'stage/filteredRecords.csv')
        print('Filtered Records - Uploaded to S3')
        self.next(self.scrape)

    @step
    def scrape(self):

        """
        Step 4 : To scrape data from all the Earning Call links for a given CIK and store the results to the S3 bucket
        """
        os.system('docker run e27e86479e0e') # The scraped data ia tored in the Staging folder in the s3 bucket and is named 'result.txt'
        self.next(self.cleanfile)

    @step
    def cleanfile(self):

        """
        Step 5 : To clean the scraped text file by removing blank spaces and special characters
        """

        s3 = boto3.resource('s3',
                            aws_access_key_id='',
                            aws_secret_access_key='')
        s3.Bucket('assignment2swarna').download_file(
            'stage/result.txt', 'result.txt')
        print('Scraped file downloaded')
        print('Cleaning file...')

        filename = 'result.txt'
        tmpFile = 'tmp_' + filename
        # Strip all blank lines and write to a temp output file
        with open(filename, encoding="ISO-8859-1") as infile, open(tmpFile, 'w', encoding='ISO-8859-1') as outfile:
            for line in infile:
                if not line.strip(): continue  # skip the empty line
                outfile.write(line)  # non-empty line. Write it to output
        print('File cleaned')
        self.next(self.callmicroservice)

    @step
    def callmicroservice(self):
        """
        Step 6 : Tokenize the text file into sentences and pass each sentence to the microservice to predict the sentiment
        """
        file1 = open('tmp_result.txt', 'r', encoding='ISO-8859-1')
        # This part is to get rid of the garbage text at the end of the file
        tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
        # Parse document and remove all HTML tags
        soup = BeautifulSoup(file1, "html.parser")
        all_text = ''.join(soup.findAll(text=True))
        all_text.translate('\n\t\r')
        # Convert all text to sentences
        sentences = (tokenizer.tokenize(all_text))
        sentenceList = []
        for sent in sentences:
            # append all sentences to the list
            # Remove all numbers and special characters
            if len(sent.split(' ')) >= 4:
                sentenceList.append(re.sub('\W+', ' ', sent))
                # print ('Ignoring: ' + sent)
        headers = {
            'Content-type': 'application/json',
        }
        print('Calling the microservice')
        with open('inference_output.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['text', 'score', 'sentiment'])
            for sent in sentenceList[:50]:
                # i = i + 1
                jsonReq = "{\"data\": [" + "\"" + sent + "\"]}"
                # print (jsonReq)
                response = requests.post('http://0.0.0.0:5000/predict', headers=headers, data=jsonReq)
                print(response.text)
                data = response.json()
                key = data['input']['data']
                value = data['pred']
                val2 = str(value).replace('[', '').replace(']', '')
                senti = ''
                if float(val2) > 0.5:
                    senti = 'Positive'
                else:
                    senti = 'Negative'
                writer.writerow([str(key).replace('[', '').replace(']', ''), val2, senti])
        print('Uploading the inference output file to the Output folder of the s3 bucket')
        s3 = boto3.resource('s3',
                            aws_access_key_id='',
                            aws_secret_access_key='')
        s3.Bucket('assignment2swarna').upload_file(
            'inference_output.csv', 'output/inference_output.csv')
        print('Output File Uploaded to S3')
        self.next(self.end)

    @step
    def end(self):
        """
            This is the 'end' step. All flows must have an 'end' step, which is the
            last step in the flow.
            """
        print("Congratulations! Your Inference pipeline ran successfully!")
        # Sending a Slack notification
        message = 'Inference pipeline run successfully!'
        webhook_url = 'https://hooks.slack.com/services/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'
        slack_data = {"attachments": [
            {
                "text": message,
                "color": "#ffce33",
                "title": "Status: Completed",
                "footer": "Team1"
            }
        ], 'username': 'CaseStudy2', 'icon_emoji': ':blue_book:'}
        # slack_data['text'] = message
        json.dumps(slack_data)
        requests.post(
            webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )


if __name__ == '__main__':
    ScrapeEdgar()
