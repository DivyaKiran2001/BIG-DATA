# Healthcare Revenue Cycle Management (RCM) Data Engineering Project

## 📘 Project Overview

**Revenue Cycle Management (RCM)** is the process hospitals use to manage financial aspects—from the time a patient schedules an appointment to the point when the healthcare provider gets paid.

In this project, we aim to build a **metadata-driven data pipeline** to transform raw healthcare data into clean and structured **fact and dimension tables**, enabling the reporting team to generate key performance indicators (KPIs).

---

## 🚀 Technologies Used

- **Azure Databricks** – For data processing and transformation using Spark
- **Azure Data Factory** – For orchestrating and automating pipelines
- **Azure Data Lake Storage** – For storing flat files (Claims data)
- **Azure SQL Database** – For storing EMR (Electronic Medical Records) data
- **Public APIs** – For fetching external data (NPI and ICD codes)

---

## 🧑‍💻 Role of a Data Engineer

As a Data Engineer in this project, the responsibilities include:

- Ingesting data from multiple sources (SQL databases, flat files, and APIs)
- Building and orchestrating a metadata-driven pipeline in Azure Data Factory
- Transforming raw data into structured **fact** and **dimension** tables
- Enabling self-service BI and dashboarding for the reporting team

---

## 🗃️ Datasets Overview

### 1. EMR (Electronic Medical Records) – **Stored in Azure SQL Database**

Each hospital maintains its own SQL database (e.g., `HospitalA_DB`, `HospitalB_DB`).

- `Patients` – Patient master data
- `Providers` – Healthcare professionals
- `Department` – Hospital departments
- `Transactions` – Billing and financial transactions
- `Encounter` – Initial visit details of the patient

---

### 2. Claims Data – **Flat files from Insurance Companies**

- Format: CSV/Delimited files
- Frequency: Monthly
- Landing Zone: `Data Lake > Landing Zone > Claims`
- Tool: Ingested using Azure Data Factory pipelines

---

### 3. NPI Data – **Fetched via Public API**

- **NPI** stands for **National Provider Identifier**
- Used to validate and enrich provider information

---

### 4. ICD Data – **Fetched via Public API**

- **ICD** stands for **International Classification of Diseases**
- Used to standardize diagnosis and procedure codes across systems

---

## 📊 Output

The final output of the data pipeline will be:

- **Fact Tables** – Containing measurable business data (e.g., billing, visits)
- **Dimension Tables** – Containing descriptive attributes (e.g., patient details, provider info)
- These tables will be used by the reporting team to generate KPIs and dashboards

---

## 📈 Example KPIs

- Patient Visit Trends
- Average Claim Processing Time
- Revenue by Department or Provider
- Claim Denial Rates
- Provider Performance Metrics

---

## 📌 Project Highlights

- ✅ End-to-end ETL implementation using Azure services
- ✅ Modular and metadata-driven design for pipeline flexibility
- ✅ Integration of structured, semi-structured, and API-based data
- ✅ Scalable design for onboarding new hospitals or data sources

---



