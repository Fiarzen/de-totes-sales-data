The **Totesys ETL Pipeline** is a data engineering solution that extracts, transforms, and loads data into an OLAP data warehouse for analytical purposes. The project incorporates AWS services to build a robust, automated pipeline and provides insights through **Tableau** dashboards.


- **Data Ingestion**: Extracts raw data from the Totesys database and ingests it into an AWS S3 ingestion bucket.
- **Data Transformation**: Processes raw data into a structured schema suitable for the data warehouse.
- **Data Loading**: Loads transformed data into fact and dimension tables in the data warehouse.
- **Automation**: Event-driven architecture that triggers processes using AWS Lambda and S3 events.
- **Monitoring and Logging**: AWS CloudWatch monitors the pipeline for operational visibility.
- **Visualization**: Tableau provides interactive dashboards to analyze the data.
**S3**: Ingestion and processed buckets.
- **Lambda**: Python-based ETL scripts for data processing.
- **CloudWatch**: Monitoring and logging.
**Ingestion**:
- Data is extracted from the Totesys database and placed in the S3 ingestion bucket.
- Find the file in src/extract_lambda directory
2. **Transformation**:
- AWS Lambda processes data upon ingestion and transforms it into the defined schema.
- Processed data is stored in Parquet format in the S3 processed bucket.
- Find the file in src/transform_lambda directory
3. **Loading**:
- Transformed data is loaded into a prepared data warehouse at defined intervals.
4. **Visualization**:
- Tableau to generate dashboards.

Setup instructions :
1. Create credential files named db_credentials.json and warehouse_credentials.json in the root directory with required credentials
2. Run "make layer-dependencies" in terminal from root to create the layer dependency files.
3. Run terraform init, terraform plan, and terraform apply from terraform directory after connecting to AWS





