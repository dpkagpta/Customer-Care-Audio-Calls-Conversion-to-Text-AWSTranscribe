#!/usr/bin/env python
# coding: utf-8

# In[2]:


# Importing required libraries
import audioop
import pandas as pd
import boto3
import json
import wave


# In[ ]:


# Putting in for general knowledge
# 0 dB: human hearing threshold
# 20 dB: rustling leaves
# 40 dB: quiet library
# 60 dB: normal conversation
# 80 dB: screaming baby
# 100 dB: chain saw
# 120 dB: live rock concert
# 140 dB: jet engine

# decibel = 20 * log10(rms) -->


# In[6]:


# Definind required variables
input_bucket = '********'

AWS_ACCESS_KEY_ID = '********'
AWS_SECRET_ACCESS_KEY = '********'

# Establishing connection with s3
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='ap-south-1')
bucket = s3.Bucket(input_bucket)


# In[39]:


# Downloading an audio file from s3 to local system
bucket.download_file('********.wav',
                     r'E:\project\transcribe-project\decibel_conversion\recordings.wav')


# In[3]:


with open(r'../../decibel_values.csv', 'a') as f:
    
    f.write('recording_name', 'sample_width', 'n_frames', 'avg', 'rms', 'avgpp', 'zero_crossings', 'maxpp', 'min_max')
    f.write(str(a, wav.getsampwidth(), 
            wav.getnframes(),
            audioop.avg(string_wav, wav.getsampwidth()),
            audioop.rms(string_wav, wav.getsampwidth()),
            audioop.avgpp(string_wav, wav.getsampwidth()),
            audioop.cross(string_wav, wav.getsampwidth()),
            audioop.maxpp(string_wav, wav.getsampwidth()),
            audioop.minmax(string_wav, wav.getsampwidth()))
           ) 


# In[28]:


wav = wave.open(r'../../recordings.wav')
string_wav = wav.readframes(wav.getnframes())

print('getsampwidth', wav.getsampwidth())

print('get n frmaes', wav.getnframes())
print('avg: ', audioop.avg(string_wav, wav.getsampwidth()))
print('rms: ', audioop.rms(string_wav, wav.getsampwidth()))
print('avgpp: ', audioop.avgpp(string_wav, wav.getsampwidth()))
print('zero_crossings: ', audioop.cross(string_wav, wav.getsampwidth()))
print('maxpp: ', audioop.maxpp(string_wav, wav.getsampwidth()))
print('max min: ', audioop.minmax(string_wav, wav.getsampwidth()))


# In[ ]:




