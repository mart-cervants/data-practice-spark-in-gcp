# Spark in GCP

This repo is intended to put in practice some of concepts of Pyspark and some services provided by the Google Cloud Platform.

Based on: https://github.com/nikhase/spark-on-gcp-intro

## File Description:
- `create_dataframes.py`: file to create the dataframes used.
- `create_transaction_data.py`: file to perform a cross join over the created dataframes and convert the final dataframe in CSV and Parquet.
- `spark_analysis.py`: file to perform an analysis of features of spark using the dataframes created.   
- `move_data_to_gcs.sh`: script to copy your data into a gcs bucket. 