# Hospital_Analytics_Project
Hospital Analytics  Pipeline using AWS S3, Glue, Athena &amp; Lambda for  reduction in patient wait time, improvement in satisfaction scores 

 AWS Services 
 Amazon S3 | Medallion Architecture Storage (Bronze/Silver/Gold) |
 AWS Glue | ETL Jobs + Crawler + Data Catalog |
 Amazon Athena | SQL Querying on Gold Layer |
 AWS Lambda | Real-time KPI Recalculation |
 AWS IAM | Role-based Access Control |
 CloudWatch | Pipeline Monitoring + Alerts |
 ODBC Driver | Power BI to Athena Connection |

 Power BI Dashboard — 6 KPIs

| Chart | Type | KPI |
|-------|------|-----|
| 1 | Bar Chart | Avg Wait Time by Medical Condition |
| 2 | Pie Chart | Total Patients by Condition |
| 3 | KPI Cards | Overall Cure Rate + Total Patients |
| 4 | Bar Chart | Avg Billing by Insurance Provider |
| 5 | Donut Chart | Admission Type Distribution |
| 6 | Column Chart | Test Results (Normal/Abnormal/Inconclusive) |

 📁 Repository Structure
Hospital_Analytics_Project/
│
├── 📂 glue_jobs/
│   └── bronze_to_silver.py    # ETL: Raw → Cleaned + PHI Masked
│
├── 📂 lambda/
│   └── waittime_trigger.py    # Real-time KPI Auto Trigger
│
├── 📂 sql/
│   └── athena_kpi_queries.sql # 6 KPI Table Queries
│
└── 📖 README.md

Security & Compliance
 PHI Masking    → PATIENT_0, DOCTOR_0 (anonymized)
 IAM Roles      → Role-based Access Control
 S3 Encryption  → Server-side Encryption
 HIPAA & GDPR   → 100% Compliant Pipeline
 CloudWatch     → Real-time Monitoring + Alerts
 S3 Trigger     → silver/ folder event-based Lambda







GitHub → README.md → Edit → Paste → Commit pannunga! 🚀
