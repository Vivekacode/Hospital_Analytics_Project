import boto3
import io
import json
import csv

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = 'healthprojectbucket321'

    # ============================================
    # STEP 1: Read Silver Layer with Error Handling
    # ============================================
    try:
        response = s3.get_object(
            Bucket=bucket,
            Key='silver/healthcare_silver.csv'
        )
    except s3.exceptions.NoSuchKey:
        return {'statusCode': 404, 'body': 'Silver file not found'}

    content = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    # ============================================
    # STEP 2: KPI — Total, Cured, Active, Cure Rate
    # ============================================
    total = len(rows)
    cured = sum(1 for r in rows
                if r.get('Treatment_Status') == 'CURED')
    active = total - cured
    cure_rate = round((cured / total) * 100, 2) if total > 0 else 0

    # ============================================
    # STEP 3: KPI — Avg Stay Days (from real data)
    # ============================================
    stays = []
    for r in rows:
        try:
            stays.append(float(r.get('Length_of_Stay', 0)))
        except:
            pass
    avg_stay_days = round(sum(stays) / len(stays), 2) if stays else 0

    # ============================================
    # STEP 4: KPI — Monthly Load (from real column)
    # ============================================
    total_load = round(
        sum(float(r.get('Monthly_Load_Mins', 0)) for r in rows), 2
    )

    # ============================================
    # STEP 5: Build Result
    # ============================================
    result = {
        'cure_rate': cure_rate,
        'avg_stay_days': avg_stay_days,       
        'total_monthly_load_mins': total_load,  
        'total_patients': total,
        'cured_patients': cured,
        'active_patients': active
    }

    # ============================================
    # STEP 6: Write to Gold Layer
    # ============================================
    s3.put_object(
        Bucket=bucket,
        Key='gold/realtime_kpi/latest_kpi.json',
        Body=json.dumps(result)
    )

    print(f"Cure Rate: {cure_rate}%")
    print(f"Avg Stay Days: {avg_stay_days}")
    print(f"Monthly Load: {total_load} mins")

    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
