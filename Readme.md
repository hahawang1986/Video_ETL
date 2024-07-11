# Project Description

This project aims to build a data pipeline using ETL technology (Pyspark) to process 1 million video play records generated over two days from a video streaming website (CSV files). The main objectives of the project include establishing a data warehouse for basic video information and semi-structured data (JSON format) for registered user information, forming a data lake. This data will be used for business queries and processing (SQL), while also achieving data visualization.

## Specific Steps Include:

### Data Collection and Cleaning:
- Extract play records from CSV files, process and clean the data to ensure data quality.
- Extract registered user information from JSON files, process and clean the data to ensure data consistency and integrity.

### Data Storage:
- Establish a data warehouse for basic video information, including key fields such as video ID, title, type, and release date.
- Store registered user information as semi-structured data to associate it with play records.

### Data Lake Construction:
- Store the cleaned data in a data lake to support flexible business queries and analysis needs.

### Business Query and Processing:
- Develop a series of queries and processing workflows to extract valuable information from the data, such as video play counts and user viewing behavior analysis.

### Data Visualization:
- Use data visualization tools to display key business metrics and analysis results to support decision-making.

By implementing this project, we will achieve efficient management and analysis of video play records and user information for the video streaming website, aiding in business decision-making and operational optimization.

# Project Implementation

## Data Quality Check or Layout Check
- file format
- column name ['DateTime','VideoTitle','events','id']
- Data-type check
- 'VideoTitle' structure checking

## Data Clean
- None value
- space trim in column 'VideoTitle'
- Outlier Check
  1. Numerical value check. 
  2. String length check. 
  3. Date value check. 
  4. Boolean value check

## Build Data Warehouse
![alt text](<picture/star schema.png>)

## Build Date Lake
Date Lake was splited into four parts, as the figure shown. The establishment of Data Lake leveraged ETL technology by AWS Glue Job.

![alt text](<picture/data lake.png>)

Use Airflow or Lambda to automatically schedule Glue jobs to create a data lake. Data scientists or data analysts can load data from the video-curated-bucket for analysis and store the output in the analytics-sandbox-bucket.


## User Information (Semi-structure Data)
Json Format

```plaintext
root
 |-- id: string (nullable = true)
 |-- profile: struct (nullable = true)
 |    |-- firstName: string (nullable = true)
 |    |-- jobHistory: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- fromDate: string (nullable = true)
 |    |    |    |-- location: string (nullable = true)
 |    |    |    |-- salary: long (nullable = true)
 |    |    |    |-- title: string (nullable = true)
 |    |    |    |-- toDate: string (nullable = true)
 |    |-- lastName: string (nullable = true)

```
## User Profile
Based on user Information to build user profile, including:

+ "jobDuration_weeks_max": The longest duration of employment in weeks.
+ "max_job_duration_title": The longest job title held during the career.
+ "avg_salary": Average salary earned.
+ "max_salary": Highest salary earned.
+ "max_job_salary_title": Job title associated with the highest earnings.
+ "num_jobTitle": Number of different job titles held.
+ "location": City of employment.
+ "num_jobLocation": Number of different cities worked in.

### some query for user information

+ Q1 What is the average salary for each profile? Display the first 10 results, ordered by lastName in descending order
```plaintext
+--------+---------+
|lastName|     mean|
+--------+---------+
|  Ronyak| 97714.29|
| Mumford|55333.332|
| Townsel|78666.664|
+--------+---------+
```
+ Q2 What is the average salary across the whole dataset
```plaintext
+-----------------+
|       avg_salary|
+-----------------+
|97461.87312420631|
+-----------------+
```
+ Q3 On average, what are the top 5 paying jobs? Bottom 5 paying jobs? If there is a tie, please order by title, location
```plaintext
+--------------------+---------+-----------+
|               title| location|mean_salary|
+--------------------+---------+-----------+
|procurement speci...|Melbourne|      99246|
|financial counsellor|Melbourne|      99161|
|safety superinten...|   Hobart|      99085|
|             trimmer| Brisbane|      99022|
|admin support off...|    Perth|      98975|
+--------------------+---------+-----------+
```
+ Q4 Who is currently making the most money

```plaintext
+---------+------------+--------------------+----------+----------+------+
|firstName|    lastName|               title|  fromDate|    toDate|salary|
+---------+------------+--------------------+----------+----------+------+
|    Ronda|     Zuidema|     devops engineer|2013-05-23|2024-07-10|159000|
|     Lori|     Zortman|Warehouse Storepe...|2018-11-23|2024-07-10|159000|
|    Mayme|        Zorn|    sales consultant|2017-02-23|2024-07-10|159000|
|      Kim|        Zahn|          specialist|2018-01-23|2024-07-10|159000|
|   Joseph|   Zaenglein|procurement speci...|2015-04-23|2024-07-10|159000|
|  Pauline|      Wylder|clinical psycholo...|2014-05-23|2024-07-10|159000|
|    Keith|      Wright|  pharmacy assistant|2018-11-23|2024-07-10|159000|
|  Matthew|       Woods|sales representative|2014-05-23|2024-07-10|159000|
|    James|        Wood|          hr advisor|2015-01-23|2024-07-10|159000|
...
|   Steven|       Shore|admin support off...|2016-09-23|2024-07-10|159000|
+---------+------------+--------------------+----------+----------+------+
```

+ Q5 What was the most popular job title started in 2019

```plaintext
+--------------------+---------+
|               title|num_title|
+--------------------+---------+
|sales representative|      197|
|admin support off...|      189|
|           paralegal|      185|
|  enrolments officer|      184|
|registration officer|      184|
|     counter manager|      179|
|     project manager|      179|
|     physiotherapist|      177|
|       sales manager|      175|
|  pharmacy assistant|      174|
+--------------------+---------+
```

+ Q6 How many people are currently working
df_user.withColumn("job_explode",explode("profile.jobHistory"))\
       .withColumn("toDate",col("job_explode.toDate"))\
       .withColumn("fromDate",col("job_explode.fromDate"))\
       .withColumn("salary",col("job_explode.salary"))\
       .withColumn("lastName",col("profile.lastName"))\
       .filter(col("toDate").isNull()&col("fromDate").isNotNull())\
       .count()

85187
    
+ Q7 For each person, list only their latest job. Display the first 10 results, ordered by lastName descending, firstName ascending order

```plaintext
+--------------------+---------+---------+--------------------+
|                  id|firstName| lastName|               title|
+--------------------+---------+---------+--------------------+
|91df4770-97b6-402...| Michelle|   Zysman|     physiotherapist|
|6068e4a5-4116-4ae...|    Brant|  Zylstra|    sales consultant|
|ebe590ab-5ef8-41d...|    Derek|  Zylstra|  service technician|
|231d590d-0117-475...|   Kelley|    Zylla|safety superinten...|
|4f903976-f7c0-414...|Katherine|  Zygadlo|   cosmetic injector|
|46c28eef-561c-429...|   Steven|Zwolinski|corporate consultant|
|7823aba8-4387-4d3...|    Roger|  Zwinger|Administration Of...|
|9255973c-f072-40c...|   Connie|   Zwiers|          technician|
|4988a110-27ce-44f...|     Mike|   Zwiers|           paralegal|
|67d75e21-71c9-4c3...|    Susan|    Zwieg|  pharmacy assistant|
+--------------------+---------+---------+--------------------+
```

+ Q8  For each person, list their highest paying job along with their first name, last name, salary and the year they made this salary. Store the results in a dataframe, and then print out 10 results

```plaintext
+----------+---------+------+----+
| firstName| lastName|salary|year|
+----------+---------+------+----+
|      Mark|      Roy| 83000|2024|
|     Ramon|   Willis|147000|2019|
|Evangeline|  Manning|110000|2019|
|    Samuel| Koesters| 95000|2019|
|     Lloyd|  Patrick|121000|2024|
|     James|   Harmon|113000|2024|
|       Ken|Biagiotti|153000|2024|
|    Harold|  Spooner|108000|2019|
|      Lois|   Sutton|110000|2024|
|      Paul|   Ortega|114000|2024|
+----------+---------+------+----+
```


