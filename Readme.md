# Project Description

This project aims to build a data pipeline using ETL technology to process 1 million video play records generated over two days from a video streaming website (CSV files). The main objectives of the project include establishing a data warehouse for basic video information and semi-structured data (JSON format) for registered user information, forming a data lake. This data will be used for business queries and processing, while also achieving data visualization.

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

## Data Warehouse
![alt text](<picture/star schema.png>)

## User Information
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
Based on user Information to build user profile




