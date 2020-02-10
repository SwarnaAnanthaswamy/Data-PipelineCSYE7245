import logging
# import apache beam library
import apache_beam as beam

# import pipeline options.
from apache_beam.options.pipeline_options import PipelineOptions

# Set log level to info
root = logging.getLogger()
root.setLevel(logging.INFO)

# Create a pipeline
runner = beam.Pipeline(options=PipelineOptions())

# Read the file from Google Cloud Storage
filePath = (runner
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


            return 'Demo'


class downloadFiles(beam.DoFn):


    def process(self, element):



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



        logging.info('\n**STAGE 2: queryData**\n')


        logging.info("\nRecords Found\n")
        # logging.info(data)
        logging.info("\n")



        return 'Demo'


class wordAnalysis(beam.DoFn):


    def process(self, element):


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
result = runner.run()
#  wait until pipeline processing is complete
result.wait_until_finish()
