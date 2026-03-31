-- ================================================
--  Hospital Analytics
-- Athena KPI Queries
-- ================================================

-- Step 1: Database Create
CREATE DATABASE IF NOT EXISTS healthcare_gold;

-- Step 2: Table 1 - Wait Time KPI
CREATE EXTERNAL TABLE IF NOT EXISTS
healthcare_gold.kpi_wait_time (
    Medical_Condition STRING,
    Avg_Length_of_Stay DOUBLE,
    Total_Patients INT
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 's3://healthprojectbucket321/gold/kpi_wait_time/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Step 3: Table 2 - Billing KPI
CREATE EXTERNAL TABLE IF NOT EXISTS
healthcare_gold.kpi_billing (
    Insurance_Provider STRING,
    Avg_Billing DOUBLE,
    Total_Billing DOUBLE,
    Total_Patients INT
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 's3://healthprojectbucket321/gold/kpi_billing/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Step 4: Table 3 - Admission KPI
CREATE EXTERNAL TABLE IF NOT EXISTS
healthcare_gold.kpi_admission (
    Admission_Type STRING,
    Total_Patients INT,
    Avg_Billing DOUBLE,
    Avg_Stay DOUBLE
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 's3://healthprojectbucket321/gold/kpi_admission/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Step 5: Table 4 - Test Results KPI
CREATE EXTERNAL TABLE IF NOT EXISTS
healthcare_gold.kpi_test_results (
    Test_Results STRING,
    Total_Patients INT,
    Avg_Billing DOUBLE
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 's3://healthprojectbucket321/gold/kpi_test_results/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Step 6: Table 5 - Cure Rate KPI
CREATE EXTERNAL TABLE IF NOT EXISTS
healthcare_gold.kpi_cure_rate (
    Medical_Condition STRING,
    Total_Patients INT,
    Cured_Patients INT,
    Active_Patients INT,
    Cure_Rate_Percent DOUBLE
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 's3://healthprojectbucket321/gold/kpi_cure_rate/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Step 7: Table 6 - Overall Cure KPI
CREATE EXTERNAL TABLE IF NOT EXISTS
healthcare_gold.kpi_overall_cure (
    Total_Patients INT,
    Total_Cured INT,
    Total_Active INT,
    Overall_Cure_Rate DOUBLE,
    Avg_Wait_Days DOUBLE,
    Total_Monthly_Load_Mins DOUBLE
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 's3://healthprojectbucket321/gold/kpi_overall_cure/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Step 8: KPI SELECT Queries
SELECT Medical_Condition,
       ROUND(Avg_Length_of_Stay, 2) AS Avg_Days,
       Total_Patients
FROM healthcare_gold.kpi_wait_time
ORDER BY Avg_Length_of_Stay DESC;

SELECT * FROM healthcare_gold.kpi_billing
ORDER BY Avg_Billing DESC;

SELECT * FROM healthcare_gold.kpi_admission
ORDER BY Total_Patients DESC;

SELECT * FROM healthcare_gold.kpi_test_results
ORDER BY Total_Patients DESC;

SELECT * FROM healthcare_gold.kpi_cure_rate
ORDER BY Cure_Rate_Percent DESC;

SELECT * FROM healthcare_gold.kpi_overall_cure;

-- Step 9: All Tables Row Count Verify
SELECT 'kpi_wait_time' AS table_name,
        COUNT(*) AS row_count
FROM healthcare_gold.kpi_wait_time
UNION ALL
SELECT 'kpi_billing', COUNT(*)
FROM healthcare_gold.kpi_billing
UNION ALL
SELECT 'kpi_admission', COUNT(*)
FROM healthcare_gold.kpi_admission
UNION ALL
SELECT 'kpi_test_results', COUNT(*)
FROM healthcare_gold.kpi_test_results
UNION ALL
SELECT 'kpi_cure_rate', COUNT(*)
FROM healthcare_gold.kpi_cure_rate
UNION ALL
SELECT 'kpi_overall_cure', COUNT(*)
FROM healthcare_gold.kpi_overall_cure;
