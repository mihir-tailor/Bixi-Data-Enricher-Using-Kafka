Develop ETL Pipeline using Spark and Kafka to process “Trip” data with “Station Information” from Bixi open data available on https://bixi.com/en/open-data 

To prepare data, go to the BiXi website (link provided in the documentation section) and under “Trip History” section, you will find links to the data from previous year all the way back to 2014. Download the data of the previous year. There is one file per month. Take the last month. The files are quite big, so we just take a sample. Extract the first 100 lines of the data after skipping the first line which is the CSV header. 

In Linux or Mac, you can run:

  head -n 101 <filename> | tail -n 100 > 100_trips.csv
  

For Implementation about ETL for system information and station information, please visit repository: 

https://github.com/mihir-tailor/etl-hdfs-hive
  
Implement a program to run ETL for system information and station information automatically as
  1. Drop table
  2. Transform JSON files to CSV
      a. JSON files are stored in your local filesystem
      b. Pick an appropriate library to parse JSON documents
  3. Enrich stations information data with system information
  4. Load CSV files into a Hive table
  
  Implementation of Spark Application to curate trips with the enriched station information. 
  In this project, I used spark streaming to stream Kafka topic of trip into Spark DStream using Spark Streaming component. For each RDD in DStream, joined it with the enriched station information RDD created in the previous step. The result will be a curated trip in CSV format. 
  

Schema Used: 

<img width="357" alt="image" src="https://user-images.githubusercontent.com/16378473/206752187-bc9438dc-ac03-438f-b405-13e881e6beec.png"> 

<img width="357" alt="image" src="https://user-images.githubusercontent.com/16378473/206752292-fc47dd34-9019-4f42-9422-f8b757a36663.png">

<img width="357" alt="image" src="https://user-images.githubusercontent.com/16378473/206752447-ebfeec4d-8ef4-451a-9c97-5d4a8ffecded.png">


