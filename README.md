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
 
 ****************************************************************************************************************************************
