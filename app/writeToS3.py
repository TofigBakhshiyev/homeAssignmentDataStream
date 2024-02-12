import boto3

def write(resultsPandas): 
    s3_bucket = "sparkstreamtocreditstar"
    s3_key = "futures.csv" 
    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=resultsPandas)