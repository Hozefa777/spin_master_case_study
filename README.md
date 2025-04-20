Spin Master Analytics Engineer Case Study

This repository contains the dbt project developed as part of my case study submission for the Analytics Engineer role at Spin Master.

📁 Project Overview

This project demonstrates a complete end-to-end data pipeline solution:

Ingest raw Excel/CSV files from SharePoint

Use Fivetran to move data into Google BigQuery

Transform data using dbt (Cloud) with a medallion architecture

Analyze using Power BI dashboards

🔄 Architecture Overview

SharePoint → Fivetran → BigQuery (raw) → dbt (bronze/silver/gold) → Power BI (import mode)

Medallion Architecture

Bronze Layer: Raw ingested data (raw_pos, raw_shipment, raw_fin_data)

Silver Layer: Cleaned & standardized staging models (stg_*)

Gold Layer: Business-ready fact models (fct_inventory, fct_finance)

🛠 Tools Used

SharePoint (data source)

Fivetran (data ingestion)

Google BigQuery (data warehouse)

dbt Cloud (data transformation & modeling)

GitHub (version control)

Power BI (dashboard creation & public sharing)

💡 Key Features & Logic

Delayed shipment landing logic:

FOB = 8 weeks delay

DOM = 4 weeks delay

Month-end inventory from POS

Cumulative projected inventory using shipments

Metrics: GPS, NPS, GM, Allowance, Revenue

DAX measures for sell-through, stockout risk, inventory coverage

📊 Dashboards

Live Power BI dashboard includes:

Financial overview (Revenue, NPS, GM)

POS & Inventory comparison

Dynamic metric toggles (Units/Amount)

Visual breakdowns by brand, region, type

🔗 Power BI Dashboard

📁 Folder Structure

├── models
│   ├── raw
│   ├── staging
│   └── marts (fact models)
├── snapshots (not used)
├── dbt_project.yml
├── README.md

📬 Contact

Hozefa Ajmerwalahozefaajmerwala@gmail.com
LinkedIn

Thank you for reviewing this project. Feedback is welcome!