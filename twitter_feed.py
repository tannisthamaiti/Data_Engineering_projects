from tweepy import StreamListener,Stream,OAuthHandler
import boto3
import logging
import sys


class TweetListener(StreamListener):

    def on_data(self,data):
        firehose_client = boto3.client('firehose', 'us-west-2',
                                        aws_access_key_id='',
                                        aws_secret_access_key=''
                                       )
        try:
            print ("putting data")
            response = firehose_client.put_record(DeliveryStreamName='twitter_feed_rs',
                                                  Record={
                                                           'Data': data
                                                         }

                                                  )
            logging.info(response)
            return True
        except Exception:
            logging.exception("Problem pushing to firehose")

    def on_error(self, status):
        print (status)

auth=OAuthHandler('', '')
auth.set_access_token('', '')

twitter_stream = Stream(auth, TweetListener())
hastag =  sys.argv[1] 
twitter_stream.filter(track=[hastag])
#https://medium.com/@siprem/streaming-twitter-feed-using-kinesis-data-firehose-and-redshift-745c96d04f58
#https://www.mydatahack.com/moving-data-in-s3-and-redshift-with-python/
