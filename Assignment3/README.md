# CSYE7245 33701 Big Data Sys & Int Analytics ( Product Grade Pipelines )



******************************************************************************************************************************

### Assignment 3
CodeLab Link for Assignment - 1 : https://codelabs-preview.appspot.com/?file_id=1ZUL_SuGVHo_5cqvFfjFtUQQmX3SZhX4w-oFnoEHchhg#0

#### Objective
Developed a scalable data pipeline to train and benchmark multiple synthetic data generators and then deploy them in production.
Designed the infrastructure that can be used irrespective of the number of datasets and generalized the application so that it can be used with new datasets. 


#### Tools
 - Pipeline: Airflow
 - AWS Tools: AWS S3 
 - Web App: Docker + Flask for API Management, Apache Airflow for Pipeline, Streamlit

#### Flow Diagram
![image](https://user-images.githubusercontent.com/47194856/77825152-dc0acc80-70dd-11ea-8287-d052ef09e308.png)

- The API call is made along with the link to the Data File stored on S3 
- API request yields the data from S3 Bucket and the file is used as an input for the Apache Airflow pipeline 
- API request invokes the Airflow pipeline to generate synthetic data
- The pipeline takes the input data and generates the JSON metadata - which has the list of fields, unique/continuous values and  	   categorizes the fields into two buckets:
	Categorical 
	Ordinal
- The JSON is pushed to the S3 bucket and used in subsequent benchmarking and data synthesis processes
- Multiple Data Synthesizers are invoked in parallel - to synthesize data using the base dataset and the generated JSON file 
- The outputs from all synthesizers are converted to CSVs and pushed to S3
- The benchmarking process is invoked in parallel and the outputs are collated from all the benchmarking processes
- An aggregator function collates all the benchmarked results and chooses the synthesizer with the highest score
- The output from the best performing synthesizer and the benchmarking results are pushed to S3 

#### Streamlit

Setup: Install streamlit via pip 

`pip install streamlit` 

Any streamlit app can be run by invoking the following command `streamlit run MyPythonScript.py`

![image](https://user-images.githubusercontent.com/47194856/77825276-91d61b00-70de-11ea-95d4-8d722a3ead38.png)

- The user can link the CSV file of choice on the Streamlit WebApp
- The underlying code fetches the CSV file from the S3 Bucket 
- JSON metadata is generated for the CSV file
- On choosing the synthesizer of choice - Data is synthesized and displayed on the WebApp
- On choosing the Benchmarking option - the user is presented with the benchmarking results

****************************************************************************************************************************************

#### Running the Pipeline 


##### Streamlit 

Start the Streamlit application by running the folllwing command `streamlit run streamlitdemo2`

##### Airflow Pipelines

Start the scheduler:

Run the Pipelines:


Python Dependencies: 

```
streamlit
boto3==1.9.253
botocore==1.12.253
copulas==0.2.5
cycler==0.10.0
decorator==4.4.2
docutils==0.14
exrex==0.10.5
Faker==4.0.2
graphviz==0.13.2
jmespath==0.9.5
joblib==0.14.1
kiwisolver==1.1.0
matplotlib==3.2.0
networkx==2.4
numpy==1.16.6
pandas==0.24.2
Pillow==7.0.0
pomegranate==0.11.2
pyparsing==2.4.6
python-dateutil==2.8.1
pytz==2019.3
PyYAML==5.3
rdt==0.2.1
s3transfer==0.2.1
scikit-learn==0.21.3
scipy==1.4.1
sdgym==0.1.0
sdv==0.3.2
six==1.14.0
sklearn==0.0
text-unidecode==1.3
torch==1.3.1
torchvision==0.4.2
urllib3==1.25.8
```

