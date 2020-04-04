****************************************************************************************************************************************
### Assignment 2
CodeLab Link for Assignment - 2 : https://docs.google.com/document/d/1d_5CKQYNGdQ2LycFFG-gkZrkSer-Rhtw61zuu1IGYjo/edit

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
 
 #### 1. AWS Comprehend Set up :

Step 1 :If you already have an active key created , configure AWS in the Command Line Interface using the following command :

$ `aws configure`
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
- Boto3 - `pip install boto3`
- Json - `pip install json`

 #### 2. Run Metaflow pipelines :
 
 There are two Metaflow pipelines present in this project - annotationPipeline.py and inferencePipeline.py
- To run the pipelines, make sure you navigate to the folder wheere the scripts reside in your CLI.
- To view the steps of the pipeline,run the following command depending on which pipeline you intend to run
	`python annotationPipeline.py show` or `python inferencePipeline.py show`
- To start and execute the pipeline,run the following command depending on which pipeline you intend to run
	`python annotationPipeline.py run` or `python inferencePipeline.py run`




