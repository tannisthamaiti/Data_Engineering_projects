from tweepy import StreamListener,Stream,OAuthHandler
import boto3
import logging
import sys


class TweetListener(StreamListener):

    def on_data(self,data):
        firehose_client = boto3.client('firehose', 'us-west-2',
                                        aws_access_key_id='AKIAXWWOC5M7BHL27BVS',
                                        aws_secret_access_key='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'
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

auth=OAuthHandler('7BUpIPfl7we8BBqgopUOX1iEl', 'b3RQvF1a77KRhQdAJwVlYIX108AfZDuyfSkAEUUbNQK6HYg3mH')
auth.set_access_token('485919538-J9ZK9OSw2AaukXBnspLsqGxmcVmsyYk3I8bzHbJu', 'cTlK8gLNeqSoBUGNAnBT8WITaLN6OcfpKYtYxGpTsVTbX')

twitter_stream = Stream(auth, TweetListener())
hastag =  sys.argv[1] 
twitter_stream.filter(track=[hastag])
#https://medium.com/@siprem/streaming-twitter-feed-using-kinesis-data-firehose-and-redshift-745c96d04f58
#https://www.mydatahack.com/moving-data-in-s3-and-redshift-with-python/