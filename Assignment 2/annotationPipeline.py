from metaflow import FlowSpec, step
from bs4 import BeautifulSoup
import os
import re
import boto3
import json
import nltk
import nltk.data
import csv
import requests


class ScrapeEdgar(FlowSpec):

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """
        print("Starting the Annotation Pipeline")
        self.next(self.scrape)

    @step
    def scrape(self):

        """
        Step 1 : To scrape EDGAR Earnings Call Transcripts data
        """
        print('Scraping data...')
        os.system('docker run e5e2f4ee74ea')
        # exec(open("02_scrape.py").read())
        self.next(self.cleanfile)

    @step
    def cleanfile(self):

        """
        Step 2 : To clean the scraped text file by removing blank spaces and special characters
        """

        s3 = boto3.resource('s3',
                            aws_access_key_id='',
                            aws_secret_access_key='')
        s3.Bucket('assignment2swarna').download_file(
            'stage/annotation_scrape.txt', 'annotation_scrape.txt')
        print('Scraped text for Annotation pipeline downloaded')

        filename = 'annotation_scrape.txt'
        tmpFile = 'tmp_' + filename
        print('Cleaning the file...')
        # Strip all blank lines and write to a temp output file
        with open(filename, encoding="ISO-8859-1") as infile, open(tmpFile, 'w', encoding='ISO-8859-1') as outfile:
            for line in infile:
                if not line.strip(): continue  # skip the empty line
                outfile.write(line)  # non-empty line. Write it to output
        self.next(self.awscomprehend)

    @step
    def awscomprehend(self):
        """
        Step 3 : Tokenize the text into sentences and send each sentence to AWS Comprehend to predict the sentiment for that sentence
        """
        print('Calling AWS Comprehend API...')
        # read cleaned file
        file1 = open('tmp_annotation_scrape.txt', 'r')

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

        comprehend = boto3.client(service_name='comprehend', region_name='us-east-2',
                                  aws_access_key_id='',
                                  aws_secret_access_key='')

        for text in sentenceList[:20]:

            a = json.dumps(comprehend.detect_sentiment(Text=text, LanguageCode='en'), sort_keys=True, indent=4)
            json1_data = json.loads(a)
            print(json1_data)
            with open('comprehendOutput.csv', 'a', newline='') as employee_file:
                employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                if json1_data["Sentiment"] == 'POSITIVE':
                    employee_writer.writerow(
                        [str(text), json1_data["Sentiment"], json1_data["SentimentScore"]["Positive"]])
                elif json1_data["Sentiment"] == 'NEGATIVE':
                    employee_writer.writerow(
                        [str(text), json1_data["Sentiment"], json1_data["SentimentScore"]["Negative"]])
                elif json1_data["Sentiment"] == 'NEUTRAL':
                    employee_writer.writerow(
                        [str(text), json1_data["Sentiment"], json1_data["SentimentScore"]["Neutral"]])
                elif json1_data["Sentiment"] == 'MIXED':
                    employee_writer.writerow(
                        [str(text), json1_data["Sentiment"], json1_data["SentimentScore"]["Mixed"]])
        print('Uploading Labeled Dataset to s3 bucket')
        s3 = boto3.resource('s3',
                            aws_access_key_id='',
                            aws_secret_access_key='')
        s3.Bucket('assignment2swarna').upload_file(
            'comprehendOutput.csv', 'stage/comprehendOutput.csv')
        print('Labeled dataset uploaded to S3 bucket successfully')
        # Sending a Slack notification
        message = 'Labeled Dataset generated and uploaded to s3 bucket (Annotation pipeline)'
        webhook_url = 'https://hooks.slack.com/services/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'
        slack_data = {"attachments": [
            {
                "text": message,
                "color": "#ffce33",
                "title": "Status: Process Completed",
                "footer": "Team1"
            }
        ], 'username': 'CaseStudy2', 'icon_emoji': ':blue_book:'}
        # slack_data['text'] = message
        json.dumps(slack_data)
        requests.post(
            webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        print("Congratulations! Your Annotation pipeline ran successfully!")

        # Sending a Slack notification
        message = 'Annotation pipeline run successfully!'
        webhook_url = 'https://hooks.slack.com/services/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'
        slack_data = {"attachments": [
            {
                "text": message,
                "color": "#ffce33",
                "title": "Status: Process Completed",
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
