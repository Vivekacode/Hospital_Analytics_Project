import boto3
import io
import json
import csv

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = 'healthprojectbucket321'
    
    response = s3.get_object(
        Bucket=bucket,
        Key='silver/healthcare_silver.csv'
    )
    
    content = response['Body'].read().decode('utf-8')
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)
    
    total = len(rows)
    cured = sum(1 for r in rows 
                if r.get('Treatment_Status') == 'CURED')
    active = total - cured
    cure_rate = round((cured / total) * 100, 2) if total > 0 else 0
    
    stays = []
    for r in rows:
        try:
            stays.append(float(r.get('Length_of_Stay', 0)))
        except:
            pass
    avg_wait = round(sum(stays) / len(stays), 2) if stays else 0
    
    active_load = active * 45 * 4
    cured_load = cured * 10 * 0.33
    total_load = round(active_load + cured_load, 2)
    
    result = {
        'cure_rate': cure_rate,
        'avg_wait_days': avg_wait,
        'total_monthly_load_mins': total_load,
        'total_patients': total,
        'cured_patients': cured,
        'active_patients': active
    }
    
    s3.put_object(
        Bucket=bucket,
        Key='gold/realtime_kpi/latest_kpi.json',
        Body=json.dumps(result)
    )
    
    print(f"Cure Rate: {cure_rate}%")
    print(f"Avg Wait Days: {avg_wait}")
    print(f"Monthly Load: {total_load} mins")
    
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
