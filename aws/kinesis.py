import boto3
from TwitterAPI import TwitterAPI
import json
import time



consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''



client = boto3.client('kinesis', region_name='us-east-1')
response = client.create_stream(
   StreamName='twitter_bigdata', #your streamname here
   ShardCount=1
)

api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)

kinesis = boto3.client('kinesis')

r = api.request('search/tweets', {'q':'Trump'})

r2 = api.request('search/tweets', {'q':'Biden'})


for item in r:
    kinesis.put_record(StreamName="twitter_bigdata", Data=json.dumps(item), PartitionKey="filler")

#for item in r2:
    #kinesis.put_record(StreamName="twitter1", Data=json.dumps(item), PartitionKey="filler")

kinesis = boto3.client("kinesis")
shard_id = "shardId-000000000000" #only one shard!
pre_shard_it = kinesis.get_shard_iterator(StreamName="twitter", ShardId=shard_id, ShardIteratorType="LATEST")
shard_it = pre_shard_it["ShardIterator"]
while 1==1:
     out = kinesis.get_records(ShardIterator=shard_it, Limit=1)
     shard_it = out["NextShardIterator"]
     print (out)
     time.sleep(1.0)