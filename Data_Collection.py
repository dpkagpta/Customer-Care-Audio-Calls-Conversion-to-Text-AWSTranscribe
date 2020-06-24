# Importing required libraries
import boto3
import json
import pandas as pd

# Specifying required information for access to AWS
AWS_ACCESS_KEY_ID = 'AKIAU2R6AAK3JXLNC4KF'
AWS_SECRET_ACCESS_KEY = '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'
input_bucket = 'dialer-recording'


# Creating connection with s3 server and required bucket
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')
bucket = s3.Bucket(input_bucket)

# scraping audio urls for 90+ defaulters from s3 bucket
all_recordings = {}
for key in bucket.objects.all():
    a = key.key
    b = a.replace(' ', '')
    if [i for i in cust_id if i in b]:
        all_recordings[a] = key.size


# finding customer ids with call recordings with us
relevant_cust_id = []
calllist_for_each_custid = {}
for i in all_recordings:    
    for k in cust_id:
        p = i.replace(' ', '')
        if k in p:
            relevant_cust_id.append(k)
            
            try:
                calllist_for_each_custid[k].append(i)
            except KeyError:
                calllist_for_each_custid[k] = [i]

# Saving them locally
pd.Series(all_recordings).reset_index().to_csv(r'E:\project\final_recordings_and_sizes.csv', index=False)
pd.DataFrame(relevant_cust_id, columns=['customer_ids']\
            ).to_csv(r'E:\project\transcribe-project\Data\relevant_customer_ids.csv', index=False)
json.dump(calllist_for_each_custid, open(r'E:\project\transcribe-project\Data\\all_cust_ids_records.json', 'w'))