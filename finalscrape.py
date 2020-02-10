import logging
# import apache beam library
import apache_beam as beam

# import pipeline options.
from apache_beam.options.pipeline_options import PipelineOptions

# Set log level to info
root = logging.getLogger()
root.setLevel(logging.INFO)

# Create a pipeline
plOps = beam.Pipeline(options=PipelineOptions())

# Read the file from Google Cloud Storage
filePath = (plOps
            | 'Read Input File'
            >> beam.io.ReadFromText('gs://demo-temp-bucket/input/input.csv')
            )


class splitInput(beam.DoFn):

    def process(self, element):
        strArray = element.split(',')
        logging.info(strArray)
        return strArray


class getIDXFile(beam.DoFn):

    def process(self, element):

        import urllib
        from urllib2 import Request, urlopen, URLError, HTTPError
        import glob
        import time
        import csv
        import sys
        import string
        import pandas as pd
        import os.path
        from google.cloud import storage
        # import logging.info library
        import logging
        from apache_beam.io import ReadFromText
        from apache_beam.io import WriteToText
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.options.pipeline_options import SetupOptions

        # import apache beam library
        import apache_beam as beam

        # import pipeline options.
        from apache_beam.options.pipeline_options import PipelineOptions

        import os

        logging.info('\n\n***getIDXFile**')
        strArray = element.split(',')
        logging.info(strArray)
        string_match1 = 'edgar/data/'

        if (strArray[0] <> 'Company' and strArray[1] <> 'Year' and strArray[2] <> 'Filing'):
            # Go through each line of the master index file and find given CIK
            # byte_response = response.encode('utf-8')

            url = 'https://www.sec.gov/Archives/edgar/full-index/%s/QTR1/master.idx' % (strArray[1])
            a = 0
            filename = ('idx_%s.csv') % strArray[0]

            try:
                response = urlopen(url)
                # logging.info (response)
                logging.info("Getting data from %s" % url)


            except Exception as e:
                # Exit if the URL is invalid or if the year cannot be found
                logging.info(e)
                logging.info("Year: %s is either invalid or the URL: %s could not be opened" % (strArray[1], url))
                # exit(404)

            # Generate a CSV File for the given year
            with open(filename, mode='w') as file:
                writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                emptyList = ['Company', 'CompanyName', 'Filing', 'DateFiled', 'Link']
                writer.writerow(emptyList)

                for line in response:
                    # logging.info (line)
                    a = a + 1
                    if a > 13:
                        emptyList = []

                        for element in line.decode().split('|'):
                            emptyList.append(element.rstrip('\r\n'))
                            # logging.info (element)

                        # logging.info(emptyList)
                        writer.writerow(emptyList)

            logging.info('IDX Data for year: %s Obtained' % strArray[0])
            return [(filename, strArray[0], strArray[2])]


class downloadFiles(beam.DoFn):

    def process(self, element):
        import urllib
        from urllib2 import Request, urlopen, URLError, HTTPError
        import glob
        import time
        import csv
        import sys
        import string
        import pandas as pd
        import os.path
        from google.cloud import storage
        # import logging.info library
        import logging
        from apache_beam.io import ReadFromText
        from apache_beam.io import WriteToText
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.options.pipeline_options import SetupOptions

        # import apache beam library
        import apache_beam as beam

        # import pipeline options.
        from apache_beam.options.pipeline_options import PipelineOptions

        import os

        logging.info('\nSTAGE 3 : This is an Empty Function\n')
        # logging.info (element)

        # strArray = list(element)
        # logging.info (strArray)
        # # Establish Connection
        # storage_client = storage.Client()
        # bucket = storage_client.get_bucket('demo-temp-bucket')
        #
        # # for index, row in df.iterrows():
        # url3 = 'https://www.sec.gov/Archives/' + strArray[3]
        # logging.info (url3)
        # response3 = urlopen(url3)
        #
        # with open(strArray[3].replace('/', '-'), 'w') as f:
        #     f.write(response3.read().decode('utf-8'))
        #     logging.info ('%s file generated' % element[2].replace('/', '-'))
        #
        # blob = bucket.blob(
        #     str(strArray[0]) + '/' + str(strArray[1]) + '/' + str(strArray[2]) + '/' + strArray[3].replace('/',
        #                                                                                                    '-'))
        # blob.upload_from_filename(strArray[3].replace('/', '-'))
        #
        # # logging.info ('File Uploaded')
        #
        # # Generate a CSV File for the given year
        # metaWriteRow = []
        # with open('metadata_' + str(strArray[0]) + '.csv', mode='w') as file:
        #     writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        #     header = ['Company', 'Year', 'Filing', 'Link']
        #     writer.writerow(header)
        #
        #     metaWriteRow = [strArray[0], strArray[1], strArray[2], strArray[3]]
        #     writer.writerow(metaWriteRow)
        #
        #     blob = bucket.blob(
        #         str(strArray[0]) + '/' + str(strArray[1]) + '/' + str(
        #             strArray[2]) + '/' + 'metadata_' + str(
        #             strArray[0]) + '.csv')
        #     blob.upload_from_filename('metadata_' + str(strArray[0]) + '.csv')
        #     logging.info ('Metadata file generated')
        #
        # logging.info ((strArray[3].replace('/', '-')))
        # logging.info ((strArray[3].replace('/', '_')))

        # runCommand = 'python beamWordcountUpper.py --input %s --output %s' % (strArray[3].replace('/', '-'), strArray[3].replace('/', '_'))
        #
        # stream = os.popen(runCommand)
        # output = stream.read()
        # output

        return 'Demo'


class queryData(beam.DoFn):

    def process(self, element):

        import urllib
        from urllib2 import Request, urlopen, URLError, HTTPError
        import glob
        import time
        import csv
        import sys
        import string
        import pandas as pd
        import os.path
        from google.cloud import storage
        # import logging.info library
        import logging
        from apache_beam.io import ReadFromText
        from apache_beam.io import WriteToText
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.options.pipeline_options import SetupOptions

        # import apache beam library
        import apache_beam as beam

        # import pipeline options.
        from apache_beam.options.pipeline_options import PipelineOptions

        import os

        logging.info('\n**STAGE 2: queryData**\n')
        # logging.info (element)

        # strArray = element.split(',')
        strArray = list(element)

        # Read the IDX file into a Pandas Dataframe
        data = pd.read_csv(strArray[0])

        if data.empty:
            logging.info("Nothing Found - Shutting Down")
            exit(100)

        CIK = strArray[1]
        FILE = strArray[2]

        # Query the IDX File - for the interested CIK
        queryText = 'Company == "' + str(CIK) + '"'
        data = data.query(queryText)

        a = 0

        if data.empty:
            logging.info("Company %s could not be found\nMoving to next record" % CIK)
            a = 1
            # exit(100)

        # Search the Filing
        queryText = 'Filing == "' + str(FILE) + '"'
        data = data.query(queryText)

        if data.empty:
            logging.info("Company %s does not have any %s Filing\nShutting Down" % (CIK, FILE))
            a = 1
            # exit(200)

        # Extract year from the DateFiled field
        data['Year'] = pd.DatetimeIndex(data['DateFiled']).year
        # logging.info(data[['Company', 'Year', 'Filing', 'Link']])

        logging.info("\nRecords Found\n")
        logging.info(data)
        logging.info("\n")

        storage_client = storage.Client()
        bucket = storage_client.get_bucket('demo-temp-bucket')

        # Combine all Files and generate one big file for a given CIK

        # with open(data.iloc[0]['Company'], 'w') as outfile:
        #     for fname in filenames:
        #         with open(fname) as infile:
        #             for line in infile:
        #                 outfile.write(line)

        if (a==0):

            # For the resultant dataset - find the files and download the files
            for index, row in data.iterrows():
                url3 = 'https://www.sec.gov/Archives/' + row[4]
                logging.info('Downloading Data from the follwoing URL:')
                logging.info(url3)
                response3 = urlopen(url3)

                with open(row[4].replace('/', '-'), 'w') as f:
                    f.write(response3.read().decode('utf-8'))
                    logging.info('%s file Generated' % row[4].replace('/', '-'))

                blob = bucket.blob(
                    str(row[0]) + '/' + str(row[5]) + '/' + str(row[2]) + '/' + row[4].replace('/',
                                                                                               '-'))
                blob.upload_from_filename(row[4].replace('/', '-'))

                logging.info('%s file Uploaded to Google Bucket' % row[4].replace('/', '-'))

            # Write the master file - combine if there are more than two files > into one file
            masterFile = str(data.iloc[0]['Company']) + '.txt'

            with open(masterFile, 'w') as outfile:
                for index, row in data.iterrows():
                    with open(row[4].replace('/', '-'), 'r') as infile:
                        for line in infile:
                            outfile.write(line)

            # Write the CSV Metadata file
            with open('metadata_' + str(data.iloc[0]['Company']) + '.csv', mode='w') as file:
                writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                header = ['Company', 'Year', 'Filing', 'Link']
                writer.writerow(header)

                for index, row in data.iterrows():
                    metaWriteRow = []
                    metaWriteRow = [row[0], row[2], row[5], row[4]]
                    writer.writerow(metaWriteRow)

            blob = bucket.blob(
                str(str(data.iloc[0]['Company']) + '/' + str(data.iloc[0]['Year']) + '/' + str(
                    data.iloc[0]['Filing']) + '/' + 'metadata_' + str(
                    data.iloc[0]['Company']) + '.csv'))
            blob.upload_from_filename('metadata_' + str(data.iloc[0]['Company']) + '.csv')
            logging.info('Metadata file generated!')

            yearRet = str(data.iloc[0]['Year'])
            # linkUrl = str(data.iloc[0]['Link'])

            return [(CIK, yearRet, FILE, str(data.iloc[0]['Company']) + '.txt')]

        else:
            print ('Invalid CIK, Year, Filing Combination')




class wordAnalysis(beam.DoFn):

    def process(self, element):
        import urllib
        from urllib2 import Request, urlopen, URLError, HTTPError
        import glob
        import time
        import csv
        import sys
        import string
        import pandas as pd
        import os.path
        from google.cloud import storage
        # import logging.info library
        import logging
        from apache_beam.io import ReadFromText
        from apache_beam.io import WriteToText
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.options.pipeline_options import SetupOptions

        # import apache beam library
        import apache_beam as beam

        # import pipeline options.
        from apache_beam.options.pipeline_options import PipelineOptions

        import os

        logging.info('*******WordAnalysis*******')
        strArray = list(element)

        # inputPattern = 'counts_' + strArray[0] + '.txt-*'
        # filePaths = glob.glob(inputPattern)

        # df = pd.DataFrame()

        # df = pd.read_csv(file, names=colnames, header=None)
        # print (df)

        runCommand = 'python beamWordcountUpper.py --input %s --output counts_%s' % (strArray[3], strArray[3])

        stream = os.popen(runCommand)
        print (stream)
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('demo-temp-bucket')

        colnames = ['word', 'Count']

        inputPattern = 'counts_' + strArray[0] + '.txt-*'

        filePaths = glob.glob(inputPattern)

        df = pd.DataFrame()

        for file in filePaths:
            print ('\n')
            print (file)
            colnames = ['word', 'Count']
            df = pd.read_csv(file, names=colnames, header=None)
            # print (df)
        # print (df)

        wordSentiments = pd.read_csv('wordSentiments.csv')
        # print (wordSentiments)

        # df1 = df.merge(wordSentiments[['word', 'count']], on=['weeknum'])

        mergeDF = pd.merge(df, wordSentiments, on='word')
        mergeDF['CIK'] = strArray[0]
        mergeDF['Year'] = strArray[1]
        mergeDF['Type'] = strArray[2]

        cols = ['CIK', 'Year', 'Type', 'word', 'sentiment', 'Count']
        mergeDF = mergeDF[cols]
        mergeDF = mergeDF.sort_values('word')

        outputFile = 'output_' + strArray[0] + '.csv'
        mergeDF.to_csv(outputFile, index=False)

        blob = bucket.blob(
            str(strArray[0]) + '/' + str(strArray[1]) + '/' + str(strArray[2]) + '/' + outputFile)
        blob.upload_from_filename(outputFile)

        print (strArray)

        return '**************'


idxReturn = (filePath
             | 'getIDX'
             >> beam.ParDo(getIDXFile()))

queryOutput = (idxReturn
               | 'extractData'
               >> beam.ParDo(queryData()))

# fileInfo = (queryOutput
#             | 'downloadStage'
#             >> beam.ParDo(downloadFiles()))

demoOP = (queryOutput
          | 'analyzeWords'
          >> beam.ParDo(wordAnalysis()))

# Run the pipeline
result = plOps.run()
#  wait until pipeline processing is complete
result.wait_until_finish()
