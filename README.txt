README.txt

Project Name: Twitter Sentiment Pipeline

Author: Bernard Lin, 963011034

Course: CMPT732 Programming for Big Data

Purpose: a data pipeline that will show the overall sentiment on the U.S. stock market in real-time by analyzing the relevant tweets on Twitter as they are posted.  The major premise is that, during a highly volatile market session, overall sentiment would be negative if more people are bearish on stocks while sentiment would be positive if people are generally bullish on stocks.  That is, a real-time sentiment reading is a useful technical indicator for a market participant.

Access: http://www2.cs.sfu.ca/fas-info/cs/people/GradStudents/bla96/personal/HTML/

Modules: 
twitter_client.py: connects to Twitter and opens a stream with tracking topics concerning the S&P500 index and its member stocks.  Tweets are forwarded to Kafka.  Assumes existence of a Kafka server at “rcg-hadoop-01:6667”. Run with Python.
twitter_sentiment.py: periodically reads from Kafka server the accumulated messages, and send the aggregate to sentiment140.com for analysis. Saves analysis results under ‘/fas-info/cs/people/GradStudents/bla96/personal/yarn/‘. Run with pyspark.
twitter_display.py: serves the analysis results via HTTP, uses port 8081. Run with regular Python. *Also saves parsed results in JSON files located in ‘/fas-info/cs/people/GradStudents/bla96/personal/‘
index.html: displays the results in ‘/fas-info/cs/people/GradStudents/bla96/personal/‘. Original intention to display using Flask server scrapped.
