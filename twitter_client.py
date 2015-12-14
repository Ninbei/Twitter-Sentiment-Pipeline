__author__ = 'bernardlin'
#from pyspark import SparkConf, SparkContext
#from pyspark.sql import SQLContext, DataFrame, Row
#from pyspark.sql.types import StructType, StructField, StringType, IntegerType
#from datetime import datetime
from TwitterAPI import TwitterAPI
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
#import datetime
#import sys, operator
#import re, string
#import unicodedata
#import time
import json
#import math
import csv

#inputs = sys.argv[1]
#output = sys.argv[2]

#Twitter credentials
consumerKey = 'QCWFigvbCCCujqQl3tBsbd3F6'
consumerSecret = 'heZ9wPYBPBrybXap8aig4vb5TPJHdW9VivUwypZ6M8SiVO6T2o'
accessToken = '271260290-CA08IxdM3BYIoEJnTlIdq7GMeZeNfh0gq3d39Vbs'
accessTokenSecret = 'CWGBmM710AqdktRkkAE4rzKbszdO1kUVXy7G5UxtrAC0k'
appid = 'ntg_ninbei@hotmail.com'

#inputs = '/users/bernardlin/Desktop/COURSE/CMPT732BigData/project/'
#inputs = '/fas-info/cs/people/GradStudents/bla96/personal/'
inputs = 'CMPT732/'
output = '/users/bernardlin/Desktop/COURSE/CMPT732BigData/project/'
topic = 'STOCKS'
symbol = 'S&P500'

#construct tracking list from list of S&P500 companies
symbol_input = open(inputs + "s&p500constituents.csv", "r")
symbol_reader = csv.reader(symbol_input)
sp500 = list(symbol_reader)
symbols = []
company_names = []
track_term = ["$S&P500"]
for i in sp500:
    symbols.append(i[0])
    company_names.append(i[1])
    #track_term.append("$" + i[0] + " stock price")
    track_term.append(i[0] + " stock")

#pop unwanted terms out of search list
symbols.pop(0)
company_names.pop(0)
track_term.pop(1)

#start spark
#conf = SparkConf().setAppName('LIN twitter stream listener')
#sc = SparkContext(conf=conf)

#open a streaming pipeline with twitter
api = TwitterAPI(consumerKey, consumerSecret, accessToken, accessTokenSecret)

#open a kafka pipeline with cluster
kafk = KafkaClient("rcg-hadoop-01:6667")
producer = SimpleProducer(kafk)

#recieve stream from twitter and forward to kafka, can only track 400 items (Twitter rule)
r = api.request('statuses/filter', {'track': track_term[0:399]})
lastitem = None
for item in r.get_iterator():
    if 'text' in item:
        message = {"symbol":symbol, "text":item['text']}
        producer.send_messages(topic, json.dumps(message))
        print json.dumps(message)
        #lastitem = item
    elif 'limit' in item:
        print '%d tweets missed' % item['limit']['track']
    elif 'message' in item:
        print '%s (%d)' % (item['message'], item['code'])
    elif 'disconnect' in item:
        print 'disconnecting because %s' % item['disconnect']['reason']
        break
