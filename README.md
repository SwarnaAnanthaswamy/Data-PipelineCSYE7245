# CSYE7245 33701 Big Data Sys & Int Analytics ( Product Grade Pipelines )

### Assignment 1
CodeLab Link for Assignment - 1 : https://codelabs-preview.appspot.com/?file_id=1UTP09POdcHQ4_5H12JsMnNe_6pcy2shLs6fOHz786tQ#0

#### Objective
To build and deploy a data pipeline for scraping EDGAR data for given CIK, year, filing and counting the number of unique words in accordance with the reference file provided. The output produced is to be stored in the google Bucket with the following hierarchy - CIK > Year > Filing > Output file

#### Tools
 1) Apache Beam with Python SDK: Data pre-processing, cleansing and data pipelining
 2) Google Cloud Dataflow
 3) Google Buckets: Input and target file storage
 4) Google Compute engine
 5) Pandas - to read the contents of the input CSV file, merge output and reference files together and CSV generation

#### Flow Diagram

![image](https://lh5.googleusercontent.com/wXMLExJ3q7RqoUH3BuakAnMwtL-l46UxZPGL2dUG6V5UfTvxQquUZCXqr0sXaso9TwEi0Q6-3IQp8hsVN7NDRxeA5RqEGg0GkiZE7CToiQdJ20FMYRArr6B4UNpSD6GFJSt9gvU2)

 #### Future Enhancements
 1) Invoke the process automatically as soon as a file is placed in the target folder on Google Bucket
 2) Connect reporting & visualization tools like Google Data Studio to easily analyze and visualize the output
 3) Enable integration with Slack to deliver real time notifications on process kick-off/completion
 
 
 #### Steps to run this assignment
 
 1) Project setup
    GCP organizes resources into projects. This allows you to collect all of the related resources for a single application in     one place. Begin by creating a new project for this assignment.
   
 2) Set up Cloud Dataflow
    To use Cloud Dataflow, enable the Cloud Dataflow APIs and open Cloud Shell.
    Enable Cloud APIs
    Cloud Dataflow processes data in many GCP data stores and messaging services, including BigQuery, Cloud Storage, and Cloud     Pub/Sub. To use these services, you must first enable their APIs.
    
 3) Open Cloud Shell
    In this assignment, you do much of your work in Cloud Shell, which is a built-in command-line tool for the GCP Console.
    Open Cloud Shell by clicking the Activate Cloud Shell button.
    
 4) Set up authentication:
    In the Cloud Console, go to the Create service account key page.

    Go to the Create Service Account Key page. From the Service account list, select New service account. In the Service           name field, enter a name. From the Role list, select Project > Owner.
    
    Click Create. A JSON file that contains your key downloads to your computer.
    
    Set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the JSON file that contains your service           key. This variable only applies to your current shell session, so if you open a new session, set the variable again.

 5) Create a Cloud Storage bucket:
    In the Cloud Console, go to the Cloud Storage Browser page.
    Go to the Cloud Storage Browser page

    Click Create bucket.
    In the Create bucket dialog, specify the following attributes:
    
    Name: A unique bucket name. Do not include sensitive information in the bucket name.
    Default storage class: Standard
    A location where bucket data will be stored.
    Click Create.
    
    Create an "Input" bucket and place the input .csv file here. In the code, replace the input path with the path of this         bucket.
    
 6) Set up Python environment
    Use the Apache Beam SDK for Python with pip and Python version 2.7, 3.5, 3.6 or 3.7. Check that you have a working             Python and pip installation by running:
    
    python --version
    python -m pip --version
    
 7) Get the Apache Beam SDK
    The Apache Beam SDK is an open source programming model for data pipelines. You define these pipelines with an Apache Beam     program and can choose a runner, such as Dataflow, to execute your pipeline.
    
    Install the latest version of the Apache Beam SDK for Python by running the following command from a virtual environment:
    
    pip install apache-beam[gcp]
    
 8) Run run.sh on the Dataflow service
    
    You can run the script file (run.sh) on the Dataflow service by specifying DataflowRunner in the runner field.
    
    python 0131_finalscrape.py \
	   --project encoded-site-265218 \
	   --runner DataflowRunner \
	   --staging_location gs://demo-temp-bucket/staging \
	   --temp_location gs://demo-temp-bucket/temp \
	   --output gs://demo-temp-bucket/output2
    
  9) View your results using GCP
     When you run a pipeline using Dataflow, your results are located in a Cloud Storage bucket.
     
 10) Clean up
     To avoid incurring charges to your Google Cloud account for the resources used in this quickstart, follow these steps.

     In the Cloud Console, go to the Cloud Storage Browser page.
     Go to the Cloud Storage Browser page

     Click the checkbox for the bucket you want to delete.
     To delete the bucket, click Delete delete.
 
****************************************************************************************************************************************
### Assignment 2
CodeLab Link for Assignment - 1 : https://docs.google.com/document/d/1d_5CKQYNGdQ2LycFFG-gkZrkSer-Rhtw61zuu1IGYjo/edit

#### Objective

To build three NLP pipelines, starting from scraping data, building ML models, trying out transfer learning, building APIs ready for deployment using Tensorflow and dockerized for reproducibility.

#### Flow Diagram
![image](https://user-images.githubusercontent.com/47194856/75589373-95777300-5a48-11ea-85c7-805dd7ac0cfc.png)

#### Tools
- Pipeline: Metaflow
- AWS Tools: AWS S3 and AWS Comprehend
- Web App: Docker + Flask

#### Annotation Pipeline

- Step 1 : Scrape data
Get a list of all available Proxies from https://www.sslproxies.org/ ; The proxy list is used to make requests to Seeking Alpha. This is done in order to avoid 403 requests. Scrape around 50 EDGAR Earnings Call data from various companies from Seeking Alpha. This scraped data is stored as a text file on the Amazon S3 bucket.
- Step 2: Pre-process data
The scraped data is downloaded from the S3 bucket and pre-processed to remove empty lines and special characters
- Step 3 : Call AWS Comprehend API
The pre-processed data is tokenized and broken down into sentences. Each sentence is sent to the AWS Comprehend API, which returns the sentiment of the sentence.
- AWS Comprehend returns sentiment in the following format :

{
SENTIMENT : POSITIVE
{
Sentiment score 
{
		Positive : 
		Negative :
		Mixed :
		Neutral :
	}
}
}

These scores are normalized to a scale of -1 to 1, with -1 being negative and +1 being positive
The results are stored in a csv file on the s3 bucket in the following format 

Statement
Sentiment
The weather is amazing today!
0.9

#### Training Pipeline
- Step 1 : Fetch the I/P 
Download the output labelled data from the S3 Bucket 
- Step 2 : Transfer Learning
Use the IMDB Dataset as the base model - and apply transfer learning to train the model, by removing one or two layers. Store the resulting model as .h5 and .json file(s), for the network and the weights respectively. The model is then saved on a S3 Bucket

#### Microservice - Dockerized Flask App - running the Generated Model to predict sentiments

- Step 1: The Dockerized Flask App is designed to always fetch the latest model from the S3 Bucket on running. A request made to http://0.0.0.0:5000/download - enables the app to download the latest model from the S3 Bucket, the .h5 and .json files are downloaded and the model is initialized, ready to predict the sentiments. 

- Step 2: Any JSON data POSTed to http://0.0.0.0:5000/predict will return a sentiment score. Even if there are subsequent changes to the model, the entire docker image - need not be stopped and re-run. A request to http://0.0.0.0:5000/download will automatically load the latest available model on the S3 Bucket.

#### Inference Pipeline

- Step 1: Download the input .CSV file and metadata file from S3 Bucket
- Step 2: Search for the Company’s Ticker on the metadata file
- Step 3: Get a list of all available Proxies from https://www.sslproxies.org/ ; The proxy list is used to make requests to     Seeking Alpha. This is done in order to avoid 403 requests
- Step 4: Scrape Earning Calls for a given company, for the given year. This scraped data is stored as a text file on the       Amazon S3 bucket
- Step 5: The scraped data is downloaded from the S3 bucket and pre-processed to remove empty lines and special characters
- Step 6: For each sentence, sent a request to the Flask App - with the JSON Data, and collect the sentiment scores
- Step 7: Generate .CSV and upload the file on S3 Bucket.

#### Future Enhancements

- Using metadata frameworks like Hopsworks, DataHub & Amundsen in our AI and ML applications.
- Maintain model versioning on S3 Bucket, in order to revert to older models (if necessary) when the latest model is not         performing as expected.
- Connect reporting & visualization tools like Google Data Studio to easily analyze and visualize the output.

 #### Steps to run this assignment
 
 1. AWS Comprehend Set up :

Step 1 :If you already have an active key created , configure AWS in the Command Line Interface using the following command :

$ aws configure
AWS Access Key ID [None]: <enter Access Key>
AWS Secret Access Key [None]: <enter Secret Access Key?
Default region name [None]: us-east-2
Default output format [None]: 

Step 2 :If your AWS account is not set up, follow the below mentioned steps :

- Create a new user under IAM Services (in AWS Console) if not created already. Select both Programmatic access and AWS Management Console for Access Type
- Make sure to assign the following policy to the user - ComprehendFullAccess
- When the user is successfully created, the Access ID key and the Secret access key are displayed.( NOTE : The secret access key is displayed just once at the time of creation. If you forget the Secret access key, you can always create a new Access Key)
- Use these keys to configure AWS as described in Step 1.
Reference - https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-started_create-admin-group.html

Step 3 :The libraries to be imported are :
- Boto3 - pip install boto3
- Json - pip install json



******************************************************************************************************************************
