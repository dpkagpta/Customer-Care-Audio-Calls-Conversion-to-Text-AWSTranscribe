# Importing required libraries
import boto3
import re
import json
import pandas as pd
import logging
import csv

#Create and configure logger (optional)
# logging.basicConfig(filename=r"./English_transcription_logs.log", 
#                     format='%(asctime)s:%(message)s', 
#                     filemode='a',
#                     level=logging.DEBUG) 
  
# #Creating an object 
# logger=logging.getLogger() 
  
#Setting the threshold of logger to DEBUG 
# logger.setLevel(logging.DEBUG) 


# Specifying required information for access to AWS
AWS_ACCESS_KEY_ID = '*********'
AWS_SECRET_ACCESS_KEY = '*********'

output__name = '*********'
input_bucket = '*********'


# Creating connection with s3 server and required bucket
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, 
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')
bucket = s3.Bucket(input_bucket)


# data preprocessing
customer_id = pd.read_csv(r'../../customers.csv')
customer_id = customer_id['customer_id'].unique().tolist()
customer_id = ['-' + str(i) + '-' for i in customer_id]


# loading the dataset containing scraped audio urls from s3
df = pd.read_csv(r'../../final_recordings_and_sizes.csv')
df = df.rename({'0': 'sizes', 'index': 'recordings'}, axis=1)
all_recordings = df['recordings'].unique().tolist()
print('We have total of ', len(all_recordings), ' recorded calls of customers with us')


# Loading the dataset with customer ids
relevant_customers_ids = pd.read_csv(r'../../relevant_customer_ids.csv')
relevant_customers_ids = relevant_customers_ids['customer_ids'].unique().tolist()
print('We have a total of ', len(relevant_customers_ids), ' with us')


# Loading scraped audio filed for customers
with open(r'../../all_cust_ids_records.json', 'r') as j:
     contents = json.loads(j.read())

    
# some further processing of the dataset
customers_calls = []
for k in relevant_customers_ids:
    relevant_customers_ids.append(contents[str(k)])

customers_calls = [val for i in customers_calls for val in i]
customers_calls = list(set(customers_calls))


# final data preparation for customer ids
final_data = final_data[final_data['recordings'].isin(customers_calls)]
# filtering
final_data = final_data[final_data['sizes'] >= 300]

# let's keep a track of all the numbers by perinting them
print('We will be transcribing ', len(final_data), ' calls for customer ids.')


# Creating a new column with the name final URL of the audio
final_data['recording_url'] = 'https://{}.format(input_bucket).s3.ap-south-1.amazonaws.com/' + final_data['recordings']
final_data['job_ids'] = final_data['recordings'].str.replace('.wav', '')
final_data['job_ids'] = final_data['job_ids'].str.replace('/', '-')
final_data = final_data.drop('recordings', axis=1)


# Creating a dictionary
transcribe_data_dict = dict(zip(final_data.job_ids, final_data.recording_url))

# making connection with AWS transcribe
transcribe = boto3.client('transcribe', aws_access_key_id=AWS_ACCESS_KEY_ID, 
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')


# defining function for transcribing audio files
def transcribe_data(job_name, job_uri, output_bucket_name, language):
    
    transcribe.start_transcription_job(
        
        TranscriptionJobName=job_name, 
        Media = {'MediaFileUri': job_uri}, 
        MediaFormat='wav', 
        LanguageCode=language,
        OutputBucketName = output_bucket_name,
        Settings={
                      'ShowSpeakerLabels': True,
                      'MaxSpeakerLabels': 2,
                      'ShowAlternatives': True,
                      'MaxAlternatives': 2,                                
                })


# transcribing in English and saving the results in the output bucket on s3 (we cannot save it locally directly after transcrining)
for k, v in transcribe_data_dict.items():
  
    try:   
        english_transcribe_data(job_name='English-' + k, job_uri=v, output_bucket_name=output__name, language='en-IN')
        
    except:
        try:
          # In case the name already exists (it happens!)
            english_transcribe_data(job_name='English-' + k + '_', job_uri=v, output_bucket_name=output__name)
          
        except:
          # Keeping an track of all the recording URLs which could not get transcribed
            with open('../../English_not_transcribed_recordings.csv', 'a') as file:
                writer = csv.writer(file)
                writer.writerow(v)
           

# transcribing in Hindi and saving the results in the output bucket on s3 (we cannot save it locally directly after transcrining)
for k, v in transcribe_data_dict.items():
  
    try:   
        english_transcribe_data(job_name='Hindi-' + k, job_uri=v, output_bucket_name=output__name, language='hi-IN')
        
    except:
        try:
          # In case the name already exists (it happens!)
            english_transcribe_data(job_name='Hindi-' + k + '_', job_uri=v, output_bucket_name=output__name)
          
        except:
          # Keeping an track of all the recording URLs which could not get transcribed
            with open('../../Hindi_not_transcribed_recordings.csv', 'a') as file:
                writer = csv.writer(file)
                writer.writerow(v)
       

