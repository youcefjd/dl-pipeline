# dl-pipeline
Sentiment analysis data pipeline to analyze tweets in real time using AWS, Spark and Deep Learning

# Problem
Social media is a reservoir of information. People converse, interact, exchange ideas, and more importantly, give their opinions about products, services, individuals, companies, etc. This data is an absolute goldmine if exploited properly. Knowing what people think about 'x' is the first step in ameliorating and enhancing 'x'. 

# Solution 
I proposed a data-processing architecutre to collect data in real time (streaming), parse it in a non-relational database (Dynamo), process it via S3, Kinesis and Spark, run it through an NLP method, and output the results in a separate database (PostgreSQL). 

# Architecture 

![Alt Text](https://bmchp-wellsense.healthtrioconnect.com/member/member_shell.cfm?xsesschk=9a75cc0bb1094602ac85fcf1af508f43)








## Tweepy 
Because things change rather quickly and instantly in social media, collecting live data was a clear path to undertake
## Dynamo
The collected data gets automatically parsed before being stored in Dynamo - 130 features down to 13 features on the JSON file 
## S3
Amazon Data Pipeline makes it rather straight forward to migrate the data from Dynamo to S3
## Kinesis 
A Lambda function is performed on the data to further eliminate unwanted fields 
## Spark
Spark is mainly used for distributed computing - invoking up to 5 workers to predict the results of the DL model on the entire dataset 
## ULMFiT
A deep learning technique based on transfer learning. I fine-tuned the last few layers on tweets. 


