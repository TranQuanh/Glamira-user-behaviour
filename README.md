# GLAMIRA BEHAVIOUR DATA

 ![image](https://scamminder.com/include/uploads/2024/06/glamira.pt.png)

## :books: Table of Contents <!-- omit in toc -->

- [:briefcase: Business Case and Requirement](#briefcase-business-case-and-requirement)
- [:bookmark_tabs:Step by step this project](#bookmark_tabsexample-product-dictionay)


- [📃 What can you practice with this case study?](#what-can-you-practice-with-this-case-study)

---
## :briefcase: Business Case and Requirement
### 🔳Business case
In this project, we use raw data(behaviour user) in [www.glamira.com](www.glamira.com) transform into a more accessible format for extracting insights. 
### Presiquites
In this project, we'll use dbt (Data Build Tool) and SQL on Google BigQuery for data transformation. dbt, an open-source tool, will help us effectively transform data in our warehouses. We'll use SQL for data management and Google BigQuery, a fully-managed, serverless data warehouse, for super-fast SQL queries.

## :bookmark_tabs:Step by step this project
### Data collection
- As data lack of information, so we need to crawl more inforamtion in Glamira web, especially **product name**.(Crawl product name in file *produc_name_glamira*)
- Suplement data with extending ip(use file *IPLocation-Python-master*) 
### Data ingestion
- Put all data into data lake(Cloud Storage)
- Trigger data from data lake to data warehouse(Cloud Storage to Bigquery)
  - We need to transform data to jsonl format.(file in *transform_jsonl*)
  - Use Cloud Function to trigger from Cloud Storage to Bigquery(file in *schema*)

### Data transform
Use SQL and dbt to transform raw data to Dimensional model in Bigquery. Save in layer glamira_transform. Please survey your dataset first and Filter collection key = "checkout_success" for fact table(file in *dbt*)

### Data visualization
- [ ]  Import data model into Looker and Visualize data to answer below questions
- Which products (product_name) generate the most revenue?
- How do the total sales (line_total) trend over different months?
- How are sales distributed across different countries (country_name)?
You can reference my report [Looker Studio](https://lookerstudio.google.com/reporting/e939302d-c057-4e8e-8c97-8a173e2bb19d)


