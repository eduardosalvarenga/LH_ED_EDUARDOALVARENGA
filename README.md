# Data Pipeline with Postgres

**Name:** Eduardo da Silva Alvarenga

**Final file:** The final file is located in `data/final_query_results/1996-07-04/1996-07-04_final_query_result.csv`

## Introduction
This project sets up a basic data pipeline which extracts data from a Postgres database (the orders is extracted based on the date that was mentioned, it's important to notice here that the orders table are only extracted if the date matches exactly, it does not extract for a range of dates for example), transforms the data as required, and then loads it into another Postgres database. The source database is connected using a docker-compose.yml file.

## Prerequisites
Before you begin, ensure you have met the following requirements:

- You have installed Docker.
- You have installed Docker Compose.
- You have installed Python 3.6 or later.
- You have installed necessary Python libraries: psycopg2, pandas.


## Why Postgres?
Considering the nature of our data and the needs of our pipeline, we have chosen Postgres as the destination of our data.

The data we are handling in our pipeline is structured and relational in nature. It also requires consistency, integrity, and the ability to manage complex queries, which are the strengths of Postgres.

It also facilitates that the initial database was also in Postgres, so we can use the same library that we used for manipulation for both of the databases.

However, if our data pipeline were dealing with extremely high write loads or unstructured data, then a NoSQL database might be a more suitable choice due to its ability to scale horizontally and handle unstructured data.

## Why Python?
Python was chosen for scripting this data pipeline because of its simplicity, readability, and extensive support for libraries that make handling and manipulating data easy, such as pandas and psycopg2. Given the size and complexity of our data, Python provides the needed flexibility and tools for efficient data processing. It also helps that it's the programming language that I have more familiarity with.

However, if the size of the data were in the order of petabytes, then we might have to consider a more performant language or a dedicated data processing system like Apache Spark or Hadoop.

## Why CSV File Format?
We chose CSV as the file format for the intermediary data storage due to its simplicity and wide support across various tools and languages. CSV files are human-readable, easy to manipulate, and can be loaded into almost any kind of database or data processing tool.

However, CSV files do have limitations. They lack the type safety of formats like JSON. They also can't handle complex nested data structures. In our pipeline, we don't deal with such complexities, making CSV a suitable choice.

## Usage

1. Clone this repository to your local system.

`git clone <repository_link>
`

2. Navigate to the project directory.

`cd <project_directory>
`

3. Start the Postgres source database using Docker Compose.

`docker-compose up -d
`

This command reads the docker-compose.yml file and starts the services defined there. In this case, it should start the Postgres database.

4. Execute the Python script to start the data pipeline with an optional date argument for data extraction.

`python pipeline.py --date 10/07/2023
`

This script connects to the source database, extracts data, transforms it, and then loads it into the destination database. The --date argument specifies the date from which the data should be extracted. If not provided, the script will extract all data.

The date should be in the format DD/MM/YYYY

## Output and Logs

The final transformed data is saved into `data/final_query_results/{date}/{date}_final_query_result.csv`. The details about the process, including any possible errors, are logged into a file located called `file_handling.log` in the project directory.

Please ensure appropriate permissions are set for the script to write logs into this file.

## Conclusion
The decisions made in the construction of this data pipeline reflect a balance of performance, simplicity, and practicality. They are suited to the needs of most general data pipeline applications, but should be reevaluated if specific requirements or constraints arise.
