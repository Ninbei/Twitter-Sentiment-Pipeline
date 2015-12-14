__author__ = 'bernardlin'
#from pyspark import SparkConf, SparkContext
#from pyspark.sql import SQLContext, DataFrame, Row
from flask import Flask, jsonify, render_template, request
import json
import os
import glob
import fileinput
from datetime import timedelta, datetime

input = '/fas-info/cs/people/GradStudents/bla96/personal/yarn/'
#input = '/user/bla96/project/'



#def run_once():
#    conf = SparkConf().setAppName('LIN twitter sentiment web content server')
#    sc = SparkContext(conf=conf)
#    sqlContext = SQLContext(sc)
#    twitter_sentiment = sqlContext.read.json(input)
#    twitter_sentiment.show()
#    run_once.func_code = (lambda:None).func_code

#def call_only_once(func):
#    def new_func(*args, **kwargs):
#        if not new_func._called:
#            try:
#                return func(*args, **kwargs)
#            finally:
#                new_func._called = True
#    new_func._called = False
#    return new_func

#@call_only_once
#def func():
#    print "initializing!!!!!!!!!!!!"
#    conf = SparkConf().setAppName('LIN twitter sentiment web content server')
#    sc = SparkContext(conf=conf)
#    sqlContext = SQLContext(sc)
#    twitter_sentiment = sqlContext.read.json(input)
#    twitter_sentiment.show()

#Open the JSON files as input and read every line
filelist=[]
path = '/fas-info/cs/people/GradStudents/bla96/personal/yarn/'
for filename in glob.glob(os.path.join(path, 'part-r-00000-*')):
    filelist.append(filename)

#construct datetime variables for comparison
now = datetime.now()
day = timedelta(1)
hour = timedelta(hours=1)
hourago = now - hour
yesterday = now - day
format = '%Y-%m-%d %H:%M:%S.%f'

#initialize result lists
tweetslist=[]
poslist=[]
neglist=[]
dailyposlist=[]
dailyneglist=[]
hourlyposlist=[]
hourlyneglist=[]

#build relevent result lists
for line in fileinput.input(filelist):
    current = json.loads(line)
    current_datetime = datetime.strptime(current['DateTime'], format)
    tweetslist.append(current)
    if current['Sentiment'] > 0:
        poslist.append(current)
        if current_datetime > yesterday:
            dailyposlist.append(current)
        if current_datetime > hourago:
            hourlyposlist.append(current)
    elif current['Sentiment'] < 0:
        neglist.append(current)
        if current_datetime > yesterday:
            dailyneglist.append(current)
        if current_datetime > hourago:
            hourlyneglist.append(current)

fileinput.close()

#save the output to JSON files for HTML file to access
with open('/fas-info/cs/people/GradStudents/bla96/personal/dailyposlist.JSON', 'w') as outfile:
    json.dump(dailyposlist, outfile)
with open('/fas-info/cs/people/GradStudents/bla96/personal/dailyneglist.JSON', 'w') as outfile:
    json.dump(dailyneglist, outfile)
with open('/fas-info/cs/people/GradStudents/bla96/personal/hourlyposlist.JSON', 'w') as outfile:
    json.dump(hourlyposlist, outfile)
with open('/fas-info/cs/people/GradStudents/bla96/personal/hourlyneglist.JSON', 'w') as outfile:
    json.dump(hourlyneglist, outfile)


#Flask module, originally planned to use this is serve dynamic html content, but idea is scrapped in
#favor of a better formed, nicer looking static html file
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/showdaily', methods=['GET'])
def showdaily():
    #ret_data = {"value": request.args.get('echoValue')}
    #return jsonify(ret_data)
    #twitter_sentiment = sqlContext.read.json(input)
    #return twitter_sentiment
    #twitterJSON = twitter_sentiment.toJSON()
    #return jsonify(twitterJSON)
    return dailyposlist + dailyneglist

@app.route('/showhourly', methods=['GET'])
def showhourly():
    return dailyposlist + dailyneglist

if __name__ == '__main__':
    #run_once()
    app.run(port=8081, debug=True)

