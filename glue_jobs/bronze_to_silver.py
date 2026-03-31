import boto3
import pandas as pd
import io

print("Starting Bronze to Silver transformation...")

# ============================================
# STEP 1: S3 Connect + CSV Read
# ============================================
s3 = boto3.client('s3')
bucket = 'healthprojectbucket321'
bronze_key = 'bronze/healthcare_dataset.csv'

response = s3.get_object(Bucket=bucket, Key=bronze_key)
df = pd.read_csv(io.BytesIO(response['Body'].read()))
print(f"Bronze records loaded: {len(df)}")

# ============================================
# STEP 2: Remove Null Values
# ============================================
df_clean = df.dropna(subset=[
    'Name', 'Age', 'Gender',
    'Medical Condition',
    'Date of Admission',
    'Discharge Date',
    'Billing Amount'
])
print(f"After null removal: {len(df_clean)}")

# ============================================
# STEP 3: Remove Duplicates
# ============================================
df_clean = df_clean.drop_duplicates()
print(f"After deduplication: {len(df_clean)}")

# ============================================
# STEP 4: Fix Date Format
# ============================================
df_clean['Date of Admission'] = pd.to_datetime(
    df_clean['Date of Admission']
)
df_clean['Discharge Date'] = pd.to_datetime(
    df_clean['Discharge Date']
)
print("Date format fixed!")

# ============================================
# STEP 5: Calculate Length of Stay
# ============================================
df_clean['Length_of_Stay'] = (
    df_clean['Discharge Date'] -
    df_clean['Date of Admission']
).dt.days
print("Length of Stay calculated!")

# ============================================
# STEP 6: PHI Masking — HIPAA Compliance
# ============================================
df_clean['Name'] = [
    f'PATIENT_{i}' for i in range(len(df_clean))
]
df_clean['Doctor'] = [
    f'DOCTOR_{i}' for i in range(len(df_clean))
]
print("PHI Masking done!")

# ============================================
# STEP 7: Treatment Status Add
# ============================================
df_clean['Treatment_Status'] = df_clean[
    'Discharge Date'
].apply(
    lambda x: 'CURED' if pd.notnull(x) 
    else 'ACTIVE_TREATMENT'
)
print("Treatment status added!")

# ============================================
# STEP 8: Visit Load Calculate
# ============================================
df_clean['Visit_Duration_Mins'] = df_clean[
    'Treatment_Status'
].apply(
    lambda x: 10 if x == 'CURED' else 45
)

df_clean['Visits_Per_Month'] = df_clean[
    'Treatment_Status'
].apply(
    lambda x: 0.33 if x == 'CURED' else 4
)

df_clean['Monthly_Load_Mins'] = (
    df_clean['Visit_Duration_Mins'] *
    df_clean['Visits_Per_Month']
).round(2)
print("Visit load calculated!")

# ============================================
# STEP 9: Remove Outliers
# ============================================
df_clean = df_clean[
    (df_clean['Age'] >= 0) & 
    (df_clean['Age'] <= 120)
]
df_clean = df_clean[
    (df_clean['Length_of_Stay'] >= 0) & 
    (df_clean['Length_of_Stay'] <= 365)
]
df_clean = df_clean[
    (df_clean['Billing Amount'] >= 0) & 
    (df_clean['Billing Amount'] <= 1000000)
]
print(f"After outlier removal: {len(df_clean)}")

# ============================================
# STEP 10: Save to Silver Layer
# ============================================
csv_buffer = io.StringIO()
df_clean.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=bucket,
    Key='silver/healthcare_silver.csv',
    Body=csv_buffer.getvalue()
)

print(f" Silver layer written!")
print(f" Total clean records: {len(df_clean)}")
print(" Bronze → Silver Complete!")


