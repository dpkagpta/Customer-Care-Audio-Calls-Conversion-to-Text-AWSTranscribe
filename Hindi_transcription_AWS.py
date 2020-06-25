# Importing required libraries
import boto3
import re
import json
import pandas as pd
import csv
# import logging
 
  
#Create and configure logger 
# logging.basicConfig(filename=r"../Hindi_transcription_logs.log", 
#                     format='%(asctime)s %(message)s', 
#                     filemode='w') 
  
# #Creating an object 
# logger=logging.getLogger() 
  
# #Setting the threshold of logger to DEBUG 
# logger.setLevel(logging.DEBUG) 
  

# Specifying required information for access to AWS
AWS_ACCESS_KEY_ID = 'AKIAU2R6AAK3JXLNC4KF'
AWS_SECRET_ACCESS_KEY = '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'
output__name = 'sttash-transcribe-result-bucket'
input_bucket = 'dialer-recording'


# Creating connection with s3 server and required bucket
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')
bucket = s3.Bucket(input_bucket)


# # 90 days defaulters data preparation
# cust_id = pd.read_csv(r'..\Data\90pluscustomers.csv')
# cust_id = cust_id['customer_id'].unique().tolist()
# cust_id = ['-' + str(i) + '-' for i in cust_id]


# loading the above saved audio file links data
df = pd.read_csv(r'.\Data\final_recordings_and_sizes.csv')
df = df.rename({'0': 'sizes', 'index': 'recordings'}, axis=1)
all_recordings = df['recordings'].unique().tolist()
print('We have total of ', len(all_recordings), ' recorded calls of 90+ days defaulter customers with us')


relevant_customers_ids = pd.read_csv(r'.\Data\relevant_customer_ids.csv')
relevant_customers_ids = relevant_customers_ids['customer_ids'].unique().tolist()
print('We have a total of ', len(relevant_customers_ids), ' 90+ days defaulters with us')

# transcribing for first 5000 customers
calls_aftr_10000_custid = relevant_customers_ids[10000:]

with open(r'.\Data\all_cust_ids_records.json', 'r') as j:
     contents = json.loads(j.read())

customers_calls_5000_batch = []
for k in calls_aftr_10000_custid:
    customers_calls_5000_batch.append(contents[str(k)])

customers_calls_5000_batch = [val for i in customers_calls_5000_batch for val in i]
customers_calls_5000_batch = list(set(customers_calls_5000_batch))

# data preparation for first 5000 customer ids
df1 = df[df['recordings'].isin(customers_calls_5000_batch)]
# filtering
df1 = df1[df1['sizes'] >= 300]

# print('We will be transcribing ', len(df1), ' calls for first 5000 customer ids batch.')

df1['recording_url'] = 'https://dialer-recording.s3.ap-south-1.amazonaws.com/' + df1['recordings']
df1['job_ids'] = df1['recordings'].str.replace('.wav', '')
df1['job_ids'] = df1['job_ids'].str.replace('/', '-')
df1 = df1.drop('recordings', axis=1)


transcribe_data_dict = dict(zip(df1.job_ids, df1.recording_url))

# making connection with AWS transcribe
transcribe = boto3.client('transcribe', aws_access_key_id=AWS_ACCESS_KEY_ID, 
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')

# defining function for transcribing audio files
def hindi_transcribe_data(job_name, job_uri, output_bucket_name):
    
    transcribe.start_transcription_job(
        
        TranscriptionJobName=job_name, 
        Media = {'MediaFileUri': job_uri}, 
        MediaFormat='wav', 
        LanguageCode='hi-IN',
        OutputBucketName = output_bucket_name,
        Settings={
                      'ShowSpeakerLabels': True,
                      'MaxSpeakerLabels': 2,
                      'ShowAlternatives': True,
                      'MaxAlternatives': 2,                                

                })



for k, v in transcribe_data_dict.items():
    try:
        
        hindi_transcribe_data(job_name='Hindi-' + k, job_uri=v, output_bucket_name=output__name)
        # logger.info("Transcription Done: ", v) 

        
    except:
        try:
            hindi_transcribe_data(job_name='Hindi-' + k + '_Exception_handle', job_uri=v, output_bucket_name=output__name)
            # logger.info("Transcription Done in Exception: ", v) 
        except:
            with open('.\Hindi_not_transcribed_recordings.csv', 'a') as file:
                writer = csv.writer(file)
                writer.writerow(v)

            # logger.info('Could not transcribe', v)

