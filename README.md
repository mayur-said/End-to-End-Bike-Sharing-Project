# End-to-End-Bike-Sharing-Project
Hello everyone,

I'm excited to share my latest End-to-End Bike Sharing Project developed using Azure Databricks, Azure Data Lake Gen 2, and Delta Lake. The main purpose of this project is to showcase my skills in data engineering and data analysis.

### Solution Architecture
The project follows the "Azure Databricks Modern Analytics Architecture" and is organized into three layers: bronze, silver, and gold. These layers represent different stages of data refinement in the pipeline, with the data's value increasing as it progresses from bronze to gold.
    Bronze Layer: The bronze layer serves as the initial landing zone for raw and unprocessed data. It stores the ingested data in its original format, such as raw files or streaming data. It maintains the data's fidelity, ensuring data lineage and providing a historical record of the ingested data.
    ![Alt text](https://github.com/mayur-said/End-to-End-Bike-Sharing-Project/blob/master/data%20models%20and%20solution%20architecture/Azure%20Architecture%20Solution.png)

    Silver Layer: The silver layer acts as an intermediate stage where data undergoes cleaning, transformation, and structuring. It serves as a staging area for data refinement and preparation for downstream analytics. Data deduplication, filtering, standardization, and enrichment take place in this layer. It includes data quality checks, schema validation, and data integration processes to ensure data consistency and reliability for analysis.

    Gold Layer: The gold layer represents the final stage of data refinement. It contains a well-structured, clean, and validated dataset optimized for efficient querying and analysis. The data in the gold layer is further transformed, aggregated, joined, and optimized for specific analytical use cases. It represents a trusted and high-quality dataset ready for consumption by business users, data scientists, or analytical applications.

### Analysis using Spark SQL
One of the key highlights of this solution is the ability to analyze data in each layer using Spark SQL. This allows users to leverage the power of Spark SQL in conjunction with PySpark for comprehensive data analysis.
### Project Outcome: 
The end result of this project is a Power BI dashboard that utilizes data from the gold layer to track Key Performance Indicators (KPIs) derived from the bike sharing data.
### Tools Used: 
The project utilizes various tools, including Azure Databricks, Azure Data Lake Gen 2, Delta Lake, PySpark, Spark SQL, Power BI, VSCode, and Lucid Chart.
For detailed information about this project and code, please check the comment section for the GitHub link.

Thank you!
