# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
# Example of working with luigi scheduling to S3 the calling the files to redshift 
# and emailing if the process is an success.
from tweepy import StreamListener,Stream,OAuthHandler
import requests
import json
import datetime
import time
import luigi
from luigi.contrib.s3 import S3Target, S3Client
from luigi.contrib.simulate import RunAnywayTarget
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date
from datetime import datetime as dt
from time import strftime
import pandas as pd
import psycopg2



date_str = dt.today().strftime('%Y-%m-%d').split('-')
Year = date_str[0]
Month = date_str[1]
Day = date_str[2]


auth=OAuthHandler('', '')
auth.set_access_token('', '')

class gmail(luigi.Config):
    id = luigi.Parameter()
    password = luigi.Parameter()

def convert_dataframe():
	tweet =[]
	timestamps_tweets = []
	userid = []
	with open('twitDB1.csv','r') as read_file:
            	for line in read_file:
            		userid.append(line.split('::')[1])
            		tweet.append(line.split('::')[2])
            		timestamps_tweets.append(line.split('::')[0])
	line_tweet = {'userid': userid, 'Timestamp': timestamps_tweets, 'Tweets': tweet}
	df = pd.DataFrame(line_tweet)
	return(df)

# initialize blank list to contain tweets
class listener(StreamListener):
    def __init__(self, time_limit=60):
        self.start_time = time.time()
        self.limit = time_limit
        self.saveFile = open('twitDB1.csv', 'a')
    def on_data(self, data):
        try:
            if (time.time() - self.start_time) < self.limit:
                #print(data)
                userid = data.split(',"id":')[1].split(',"id_str":"')[0]
                tweets = data.split(',"text":"')[1].split(',"source":"')[0]
                #print (tweets)

                saveThis = userid + '|' + str(dt.now()) + '|' + tweets  #time.time()
                self.saveFile.write(saveThis + '\n')
               # self.saveFile.write('\n')
                return True
            else:
                self.saveFile.close()
                return False         
        except BaseException as e:
            print('failed data,', str(e))
            time.sleep(5)
    def on_error(self, status):
        print(status)
stream = Stream(auth, listener(time_limit=20), timeout=1, compression=True)
hastag =  'Trump'
stream.filter(track=[hastag])
##load file to a variable
#https://www.dataquest.io/blog/streaming-data-python/
#################################################################################
class ProcessS3File(luigi.Task):

    def requires(self):
        #return MyS3File()
        return []

    def output(self):
        client = S3Client()
        #return S3Target('s3://kaggletest1/{}/weekly_tracks.tsv'.format(hastag), client=client)
        return S3Target('s3://kaggletest1/{}/{}/{}/{}/weekly_tracks.tsv'.format(hastag, Year, Month, Day), client=client)


    def run(self):
        with self.output().open('w') as f:
            with open('twitDB1.csv','r') as read_file:
                for line in read_file:
                    f.write(line)
                          
#####################################################################################

class SendWeeklyEmail(luigi.Task):

    def requires(self):
        return [ProcessS3File()]

    def output(self):
        client = S3Client()
        return S3Target('s3://kaggletest1/{}/{}/{}/{}/email_sent_status.txt'.format(hastag, Year, Month, Day), client=client)

    def run(self):
        email = gmail().id
        password = gmail().password
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Discover Weekly {}".format(str(date.today()))
        msg['From'] = email
        msg['To'] = email
        html = """
                <html>
                  <head></head>
                  <body>
                """
        with self.input()[0].open() as csv_file:
            csv_reader = csv.reader(csv_file, delimiter='\t')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                else:
                    track_info = 'done1' 
                    track_link =  'done2'
                    html += '<p><a href="{}">{}</a></p>'.format(track_link,track_info)
        html += "</body></html>"
        msg.attach(MIMEText(html, 'html'))
        with self.output().open('w') as fout:
            try:
                mail = smtplib.SMTP('smtp.gmail.com', 587)
                mail.ehlo()
                mail.starttls()
                mail.ehlo()
                mail.login(email, password)
                mail.sendmail(email, email, msg.as_string())
                fout.write("email sent successfully")
            except Exception as e:
                fout.write(str(e))
            finally:
                mail.quit()



#################################################################################################

dbname = 'dev'
port = ''
user = ''
password = ''
aws_access_key_id = ''
aws_secret_access_key = ''
host = 'redshift-cluster-1.******.us-west-2.redshift.amazonaws.com'
host1 = 'aws_iam_role=arn:aws:iam::123456789:role/RedshiftCopyUnload' #myRedshiftRole


#Amazon Redshift connect string
conn_string = "dbname='dev' port= '' user= '' password= '' \
            host= 'training007.*******.us-west-2.redshift.amazonaws.com'"
con = psycopg2.connect(conn_string);
to_table = 'tweets_{}_{}_{}_{}'.format(hastag, Year, Month, Day)
print('Created table name is %s' % to_table)
fn = 's3://kaggletest1/{}/{}/{}/{}/weekly_tracks.tsv'.format(hastag, Year, Month, Day)
#fn = 's3://kaggletest1/weekly_tracks.tsv'
delim = '|'
sql0 = """ DROP TABLE IF EXISTS %s """ %(to_table)
sql1 = """CREATE table %s(
	   USERID numeric(38,0),
	   TWEETTIME timestamp,
	   TWEET text); """ % (to_table)
sql2 ="""COPY %s FROM '%s' credentials '%s'
       delimiter '%s' region 'us-west-2';""" % (to_table, fn, host1, delim)            
################################################################################################

class connect2redshift(luigi.Task):

    def requires(self):
        return [ProcessS3File()]


    def run(self):
        cur = con.cursor()
        cur.execute(sql0)
        cur.execute(sql1)
        cur.execute(sql2)
        cur.execute("commit;")
        print("Copy executed fine!")

    def output(self):
        return RunAnywayTarget(self)
##############################################################################################
if __name__ == '__main__':
    luigi.run()

#command to run python3 twitter_extract.py ProcessS3File --local-scheduler
#https://towardsdatascience.com/building-spotify-discover-weekly-email-alert-with-luigi-ca0bc800d137
