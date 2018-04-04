# -*- coding: utf-8 -*-
"""
Created on Sat Mar 31 23:55:25 2018

@author: Onkar
"""

import sys
import csv
import os.path
from kafka import KafkaProducer
from kafka.errors import KafkaError
from genericpath import isfile

def read_file(file_obj,log_file):
    """
    Read a csv file
    """
    #reader = csv.reader(file_obj)
    #header = file_obj.readline()
    #file_obj.seek(len(header))
    #file_reader=csv.DictReader(file_obj,delimiter=',')
    text=''
    position=''
    text=text+log_file.read()
    print (text)
    currentMonthYear=''
    data=text.split("##")
    currentMonthYear=data[0]
    position=data[1]
    print(currentMonthYear)
    print(position)
    file_obj.seek(int(position))
    header=['Dummy ID','Date of Txn','Transaction type','Amount']
    file_reader=csv.reader(file_obj,delimiter=',')
    file_reader.next()
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'transaction_data'
    count=0
    for row in file_reader:
        #print row
        userId=str(row[0])
        print (userId)
        txnDate=str(row[1])
        print(txnDate)
        txnMonthYear=str(txnDate[2:])
        formattedTxnDate=str(str(txnDate[0:2])+"-"+str(txnDate[2:5])+"-"+str(txnDate[5:]))
        print(txnMonthYear)
        if currentMonthYear=='9999':
            print("Starting Transactions.")
            currentMonthYear=txnMonthYear
            log_file.seek(0)
            log_file.truncate()
            log_file.write(currentMonthYear+"##"+str(file_obj.tell()))
            #count=count+1
        elif currentMonthYear==txnMonthYear:
            print(txnMonthYear)
            print("current month continues")
            #count=count+1
        elif currentMonthYear!=txnMonthYear:
            print("Month changed")
            print(txnMonthYear)
            log_file.seek(0)
            log_file.truncate()
            log_file.write(txnMonthYear+"##"+str(file_obj.tell()))
            log_file.close()
            file_obj.close()
            break
        print(txnMonthYear)
        txnType=str(row[2])
        #print(txnType)
        amount=str(row[3])
        try:
            producer.send(topic,userId+"\t"+formattedTxnDate+"\t"+txnType+"\t"+amount)
            count=count+1
            print("Data Sent to Kafka topic is : "+userId+"\t"+formattedTxnDate+"\t"+txnType+"\t"+amount)
        except:
            print("Error occured while sending data to kafka.")
    print("No of records sent to producer : "+str(count))
        
        #print(amount)


if __name__ == '__main__':
    source_file_path=sys.argv[1]
    log_file=open("currentMonthYear.txt","r+")
    
    print source_file_path
    print isfile(source_file_path)
    with open(source_file_path, "rb") as f_obj:
        print ("source file exists. Reading data.")
        read_file(f_obj,log_file)
        
    
