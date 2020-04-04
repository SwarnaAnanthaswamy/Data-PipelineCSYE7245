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

#Streamlit

![image](https://user-images.githubusercontent.com/47194856/77825276-91d61b00-70de-11ea-95d4-8d722a3ead38.png)

- The user can link the CSV file of choice on the Streamlit WebApp
- The underlying code fetches the CSV file from the S3 Bucket 
- JSON metadata is generated for the CSV file
- On choosing the synthesizer of choice - Data is synthesized and displayed on the WebApp
- On choosing the Benchmarking option - the user is presented with the benchmarking results

****************************************************************************************************************************************



