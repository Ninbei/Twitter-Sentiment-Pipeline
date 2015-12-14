__author__ = 'bernardlin'
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrame, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka.client import KafkaClient
from kafka import KafkaConsumer
import datetime
import json
import requests

#url to bulk message classification service
url = "http://www.sentiment140.com/api/bulkClassifyJson?appid=ntg_ninbei@hotmail.com"
#inputs = '/users/bernardlin/Desktop/COURSE/CMPT732BigData/project/'
inputs = 'CMPT732/'
#output = '/fas-info/cs/people/GradStudents/bla96/personal/'
output = 'file:///fas-info/cs/people/GradStudents/bla96/personal/yarn/'
#output = '/user/bla96/project/'
symbol = 'S&P500'

#for the streaming rdds that comes in, load as json and compile into one bulk json message
#send bulk json to sentiment140 for classification, then condense the result and save
def doThis(rdd):
    messages = rdd.map(lambda line: json.loads(line[1]))

    print("oooooooo messages:", messages.take(10))
    if (messages.count()>0):
        bulkJSON = {"data": messages.collect()}
        print("bbbbbb bulkJSON: dddddd", bulkJSON)
        response = requests.post(url, json=bulkJSON)
        response_json = response.json()
        classified = response_json['data']
        print("$$$$$$ classified: $$$$$$", classified)

        #condense classified response, strip out neutral tweets and reduce by key
        df = sqlContext.createDataFrame(classified)
        ddf = df.rdd.map(lambda (meta, polarity, symbol, text) :
                    (symbol, ((polarity/2)-1, datetime.datetime.now(), [text])))

        ddf = ddf.filter(lambda (symbol, (polarity, datetime, text)): (polarity > 0 or polarity < 0))
        ddf.reduceByKey(add_pairs)
        #outdf = sqlContext.createDataFrame(ddf).coalesce(1)
        #outdf.write.save(output, format='parquet', mode='append')

        #output to JSON to output folder
        outjson = ddf.map(lambda (symbol, (sentiment, timestamp, texts)) :
                {"Symbol":symbol, "DateTime":timestamp, "Sentiment":sentiment, "Texts": texts})
        outdf = sqlContext.createDataFrame(outjson).coalesce(1)
        outdf.write.save(output, format='JSON', mode='append')
        #outdf.write.save(output, format='JSON', mode='overwrite')
    else:
        print("****************** messages is empty! ***************************")

#merges x and y together
def add_pairs(x,y):
    if x[0] == y[0]:
        return ((x[0]), ((x[1][0]+y[1][0]), x[1][1], (x[1][2]+y[1][2])))

#open spark context and SQL context
conf = SparkConf().setAppName('LIN twitter sentiment classifier')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 20)
sqlContext = SQLContext(sc)

#open Kafka stream
kafka = KafkaClient("rcg-hadoop-01:6667")
consumer = KafkaConsumer('STOCKS', bootstrap_servers=["rcg-hadoop-01:6667"])
kafkaStream = KafkaUtils.createDirectStream(ssc, ['STOCKS'], {"bootstrap.servers":"rcg-hadoop-01:6667"})
kafkaStream.foreachRDD(doThis)

ssc.start()
ssc.awaitTermination(timeout=1000)

