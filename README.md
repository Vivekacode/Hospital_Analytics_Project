# Hospital_Analytics_Project

#Problem Statement
Hospitals generate large amounts of operational data but struggle to understand  patient flow, and efficiency. This project analyzes hospital data to extract meaningful business insight

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

 

Security & Compliance
 PHI Masking    → PATIENT_0, DOCTOR_0 (anonymized)
 IAM Roles      → Role-based Access Control
 S3 Encryption  → Server-side Encryption
 HIPAA & GDPR   → 100% Compliant Pipeline
 CloudWatch     → Real-time Monitoring + Alerts
 S3 Trigger     → silver/ folder event-based Lambda

This project demonstrates how healthcare data can be used to improve operational efficiency and financial performance.





