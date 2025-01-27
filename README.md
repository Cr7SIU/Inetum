# Inetum

The architecture described involves an automated data pipeline that integrates AWS Lambda, Amazon S3, and a data warehouse solution like Redshift, all orchestrated through Apache Airflow.

In this setup, the data processing workflow starts with AWS Lambda functions, which are triggered by Apache Airflow. These Lambda functions are responsible for fetching data from external sources, transforming it, and then saving it as Parquet files to Amazon S3. Each Lambda function corresponds to a specific data category, such as articles, blogs, or reports. These categories are processed separately, with each Lambda function handling the appropriate dataset.

Once the data is stored in S3, Airflow kicks in to handle the next phase of the process. Airflow uses the AwsLambdaInvokeFunctionOperator to trigger the corresponding Lambda function based on the dataset category. After the Lambda function successfully processes the data and uploads it to S3, Airflow proceeds to move the data into a data warehouse, specifically Redshift or Snowflake, using the S3ToRedshiftOperator or S3ToSnowflakeOperator.

This solution leverages the scalability and flexibility of AWS Lambda for data processing and transformation, while Airflow provides the orchestration layer to ensure smooth execution of tasks and data flows. With each data category having its own set of operations, the system is modular, allowing for easy extensions or modifications. The combination of these tools provides a robust, automated, and scalable pipeline to handle large volumes of data and ensure seamless integration with cloud data warehouses for analysis and reporting.


![Descripción de la imagen](https://github.com/Cr7SIU/Inetum/blob/main/Architecture.PNG)
