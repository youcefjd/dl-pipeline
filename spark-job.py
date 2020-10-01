from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import findspark
from joblibspark import register_spark
import pandas as pd
import boto3

from fastai.text.all import *

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.access.key", '')
hadoop_conf.set("fs.s3a.secret.key", '')

sc = SparkContext.getOrCreate()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', f's3-{region}.amazonaws.com')
spark = SparkSession(sc)

s3file = f's3a://{bucket}/{key}'

df = spark.read.load(s3file, sep=",", inferSchema="true", header="true", format="csv")

model_dict = torch.load(myweights)
model_arch = myModel()
model_arch.load_state_dict(model_dict)

learn = Learner.load_model('/model.pkl')

prds = [learn.predict(i) for i in df.txt]

engine = create_engine('')
preds.to_sql('', engine)


spark.stop()


