# open_lib_analysis_case_study

This program is intended to process an extract from the Open Library web store, clean the dataset and then perform analytics to generate insights from the data.

## Fetching data

```bash
wget --continue https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json -O /content/drive/MyDrive/Adidas/ol_cdump.json
```

## Tech-stack

1. PySpark - for reading the input data into a Spark Dataframe in order to run analytics on the same\

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install PySpark

```bash
pip install pyspark
```

## Approach/steps
1. Data Fetch
    >The first step is to download the data file from Open Library web store and store it in a local directory. The file is in JSON format.

2. Data Ingestion
    >The data file is ingested using the JSON reader within PySpark.

3. Data Cleansing
    >Upon successful ingestion, we perform exploratory analysis and realize the JSON schema is highly nested and the data is not clean. This step is intended to flatten the JSON schema and apply necessary filters to enable data for further processing.

4. Metric Calculation
    >Post successful data cleansing, we run series of queries on the processed data to calculate metrics and generate insights.


# Future steps
1. Dashboarding and BI
    > The calculated metrics are then refreshed on external Hive tables for the end user to query the dataset

2. Schema Design
    > The input data can be normalized into smaller tables for easier access and storage. Consequently we will have to design a data model so that the input data can be refreshed in the data warehouse on a periodic basis

3. Deeper investigation into data cleansing
    > Since the input data is not clean, we need to perform deeper analysis to ascertain missing data and then take corrective steps to recover the same. This will increase accuracy in metric calculation.

4. Workflow automation and orchestration
    > We can use CA Autosys or Apache Airflow or similar tools to schedule and automate the steps. This will enable greater control over the process flow and seamless operational execution on a daily basis.
