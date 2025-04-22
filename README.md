The **Totesys ETL Pipeline** is a data engineering solution that extracts, transforms, and loads data into an OLAP data warehouse for analytical purposes. The project incorporates AWS services to build a robust, automated pipeline and provides insights through **Tableau** dashboards.

Features of the project include:
- **Data Ingestion**: Extracts raw data from the Totesys database and ingests it into an AWS S3 ingestion bucket.
- **Data Transformation**: Processes raw data into a structured schema suitable for the data warehouse.
- **Data Loading**: Loads transformed data into fact and dimension tables in the data warehouse.
- **Automation**: Event-driven architecture that triggers processes using AWS Lambda and S3 events.
- **Monitoring and Logging**: AWS CloudWatch monitors the pipeline for operational visibility.
- **Visualisation**: Tableau provides interactive dashboards to analyse the data.
- **S3**: Ingestion and processed buckets.
- **Lambda**: Python-based ETL scripts for data processing.
- **CloudWatch**: Monitoring and logging.

Setup instructions :
1. Create credential files named db_credentials.json and warehouse_credentials.json in the root directory with required credentials
2. Run "make layer-dependencies" in terminal from root to create the layer dependency files.
3. Run terraform init, terraform plan, and terraform apply from terraform directory after connecting to AWS





