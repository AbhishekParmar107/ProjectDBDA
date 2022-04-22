# Data Cleaning, Schema Normalisation, Data Processing and Data Ingestion for Big Data Using Spark

### ABSTRACT

    1. ETL, which stands for Extract, Transform, and Load, is the process data engineers use to
    extract data from different sources in different formats, transform the data into a usable and
    trusted resource, and load that data into the systems end-users can access and use downstream
    to solve business problems. 
    
    2. After extraction of uncleaned data from different sources, like email attachments, Kaggle, 
    and various other sources. 
    
    First step of the pipeline is to clean the data. 
    In the transformation part, certain irrelevant attributes were removed from the original data, 
    as well as some necessary attributes were generated from the original data. 
    Finally, this usable format of data is stored on HDFS for application use.
    
### WORKFLOW : AIRFLOW
    Airflow is a platform to program workflows, including the creation, scheduling, and
    monitoring of workflows. Airflow isn't an ETL tool per se. But it manages, structures, and
    organizes ETL pipelines using Directed Acyclic Graphs (DAGs).
    
#### Graph View of ETL pipeline    
![image](https://user-images.githubusercontent.com/76062197/164268833-3e46ea72-dba3-497c-ab70-59bf348b5291.png)

### Reports
    The total number of invalid data and the percentage of invalids in the dataset was also found
    out and reported.
![image](https://user-images.githubusercontent.com/76062197/164270363-ce804777-8d27-48bc-938f-8b778e37a475.png)

