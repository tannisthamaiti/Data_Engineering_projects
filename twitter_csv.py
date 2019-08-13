import logging
import boto3
import json
import os
import itertools
import pandas as pd
import numpy as np

aws_access_key_id=''
aws_secret_access_key=''



def read_twitter_file(file):
	data = []
	with open(file) as f:
		for line in f:
			data.append(json.loads(line))
	return(data)


def df_empty(columns, dtypes, index=None):
    assert len(columns)==len(dtypes)
    df = pd.DataFrame(index=index)
    for c,d in zip(columns, dtypes):
        df[c] = pd.Series(np.nan, dtype=d)
    return df

df_keys = ['contributors', 'coordinates', 'created_at', 'display_text_range',
       'entities', 'extended_entities', 'extended_tweet', 'favorite_count',
       'favorited', 'filter_level', 'geo', 'id', 'id_str',
       'in_reply_to_screen_name', 'in_reply_to_status_id',
       'in_reply_to_status_id_str', 'in_reply_to_user_id',
       'in_reply_to_user_id_str', 'is_quote_status', 'lang', 'place',
       'possibly_sensitive', 'quote_count', 'quoted_status',
       'quoted_status_id', 'quoted_status_id_str', 'quoted_status_permalink',
       'reply_count', 'retweet_count', 'retweeted', 'retweeted_status',
       'source', 'text', 'timestamp_ms', 'truncated', 'user']
df_types = [np.unicode_]*36
data_df = df_empty(df_keys, dtypes=df_types)

client = boto3.client('s3', aws_access_key_id= aws_access_key_id,
	aws_secret_access_key= aws_secret_access_key)
paginator = client.get_paginator('list_objects_v2')
s3 = boto3.resource('s3', aws_access_key_id= aws_access_key_id,
	aws_secret_access_key= aws_secret_access_key)
bucket = s3.Bucket('kaggletest')
######################################################################
filename = 'twitter.csv'
file_path = '/home/titli/Documents/twitter_feed/twitter.csv'
bucket_name = 'kaggletest'
directory_name = 'twitter_loads' #it's name of your folders
dest_file_name = 'twitter.csv'
# folder created with file uploaded
client.put_object(Bucket=bucket_name, Key=(directory_name+'/'))
client.upload_file(file_path, bucket_name, '%s/%s' % (directory_name,dest_file_name))


 
