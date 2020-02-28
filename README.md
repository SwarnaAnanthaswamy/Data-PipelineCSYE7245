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

#### Flow Diagram
![image](https://user-images.githubusercontent.com/47194856/75589373-95777300-5a48-11ea-85c7-805dd7ac0cfc.png)
