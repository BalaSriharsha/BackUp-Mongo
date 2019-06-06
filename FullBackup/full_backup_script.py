import os
import sys
import datetime
import pymongo
from bson.timestamp import Timestamp

host = sys.argv[1]
bucket_name = sys.argv[2]

client = pymongo.MongoClient()

oplog = client.local.oplog.rs
timestamp = oplog.find().sort('$natural',pymongo.DESCENDING).limit(1).next()
timestamp = timestamp['ts']

date_time = str(datetime.datetime.now()).split(' ')[0]
#print(date_time)

#os.system("mongodump --host "+host+" -d local -c oplog.rs -o /mnt/mongo-test_backup/full_backup/1 --query '{ 'ts' : { $gt : "+str(timestamp)+" } }'")
#print(host)
os.system("mongodump --host "+host)

f = open("timestamps.txt","w")
f.write(str(timestamp))

os.system("tar cvzf db_backup"+date_time+".tar.gz ./dump")
os.system("aws s3 cp ./db_backup"+date_time+".tar.gz s3://"+bucket_name+"/")

