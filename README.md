# [GLAMIRA BEHAVIOUR](www.glamira.com) DATA PIPELINE

 ![image](https://scamminder.com/include/uploads/2024/06/glamira.pt.png)
<!-- Start Document Outline -->
* Batch data pipeline
  * [:briefcase: Business Case and Requirement](#briefcase-business-case-and-requirement)
  * [:bookmark_tabs:Step by step this project](#step-by-step)
    * [Architecture](#architecture)
      * [ETL flow](#etl-flow)
    * [How to run](#how-to-run)
      * [Data collection](#data-collection)
      * [Data ingestion](#data-ingestion)
      * [Data transform](#data-transform)

---
## :briefcase: Business Case and Requirement
### 🔳Business case
In this project, we use raw data(behaviour user) in [www.glamira.com](www.glamira.com) transform into a more accessible format for extracting insights. 
### Presiquites
In this project, we'll use dbt (Data Build Tool) and SQL on Google BigQuery for data transformation. dbt, an open-source tool, will help us effectively transform data in our warehouses. We'll use SQL for data management and Google BigQuery, a fully-managed, serverless data warehouse, for super-fast SQL queries.

## :bookmark_tabs:Step by step this project
### Architecture
The pipeline take data from [www.glamira.com](www.glamira.com) and transform into insight data
 - **Google cloud platform**: Cloud storage(load raw data),bigquery (load schema data and transform shema data), cloud function (trigger data in Cloud storage into bigquery), 
 - **Looker studio**: visualize data
 ![architecture diagram](https://davidnotio101.notion.site/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2Fccfa8b66-a3f6-43a1-8348-a8ac806c0223%2F4db388b6-2cfa-427c-ba52-fa66f1c5b297%2FUntitled.png?table=block&id=55cd18e5-b521-4bbe-8b57-bb7de81f5332&spaceId=ccfa8b66-a3f6-43a1-8348-a8ac806c0223&width=1440&userId=&cache=v2)
# ETL flow
- You run a code in `product_name_soup.ipynb` and `IP_glamira-python.py` to crawl all properties relate to data(product name and ip detail)
- Raw data , product name, ip location save in Google Cloud Storage
    - raw data 
    ![raw data](photo_readme\raw_data.png)
    - product name
    ![product name](photo_readme\product_name.png)
    - ip location
    ![ip location](photo_readme\ip_location.png)
  - create cloud function to trigger from cloud storage to bigquery use `schema`
  - use `dbt` to transform raw data to datawarehouse
  ![schema](photo_readme\schema.png)
  - use Looker Studio to visualize
### How to run
#### Data collection
- As data lack of information, so we need to crawl more inforamtion in Glamira web, especially **product name**.(Crawl product name in file `crawl_data`)
- Suplement data with extending ip(use file `IPLocation-Python-master`) 
- push data to **mongodb** and config **connect_db**
```python
from pymongo import MongoClient

def connect_db():
    """Kết nối đến cơ sở dữ liệu MongoDB."""
    client = MongoClient("your local host")
    db = client['database']
    collection = db['collection']
    return collection
```
#### Data ingestion
- Put all data into data lake(Cloud Storage)
- Trigger data from data lake to data warehouse(Cloud Storage to Bigquery)
  - We need to transform data to jsonl format.(file in `transform_jsonl`)
  - Use Cloud Function to trigger from Cloud Storage to Bigquery(file in `schema`)

#### Data transform
Use SQL and dbt to transform raw data to Dimensional model in Bigquery. Save in layer glamira_transform. Please survey your dataset first and Filter collection key = "checkout_success" for fact table(file in `dbt`)

### Data visualization
- [ ]  Import data model into Looker and Visualize data to answer below questions
- Which products (product_name) generate the most revenue?
- How do the total sales (line_total) trend over different months?
- How are sales distributed across different countries (country_name)?
You can reference my report [Looker Studio](https://lookerstudio.google.com/reporting/e939302d-c057-4e8e-8c97-8a173e2bb19d)


