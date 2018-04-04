'''
Created on Apr 1, 2018

@author: Admin
'''
import sys # used to exit
from kafka import KafkaConsumer
from kafka import TopicPartition
import MySQLdb
from math import sqrt
from time import sleep

KAFKA_TOPIC = 'transaction_data'
KAFKA_BROKERS = 'localhost:9092'
db = MySQLdb.connect(host="test-transaction-instance.cx0hq28hason.us-east-1.rds.amazonaws.com",
                     user="onkar",
                     passwd="onkar1712",
                     db="pivotchain")


def updateAccountDetails(userId,txnDate,txnType,txnAmount):
    select_sql = "SELECT * FROM ACCOUNT_DETAILS WHERE USER_ID='%s'"%(userId)

    #db1 = MySQLdb.connect(host="test-transaction-instance.cx0hq28hason.us-east-1.rds.amazonaws.com",
    #                 user="onkar",
    #                 passwd="onkar1712",
    #                 db="pivotchain")
    cursor1=db.cursor();
    cursor1.execute(select_sql)
    row_data=cursor1.fetchall()
    unique_users=0
    updated_users=0    
    print(cursor1.rowcount)
    if cursor1.rowcount==0:
        user_id=userId
        totalBalance=0
        day_closing_balance=txnAmount
        no_txn=1
        avg_txn_amount=txnAmount
        last_txn_day=txnDate
        days_of_txn=1
        month_avg_balance=txnAmount
        if txnType=='D':
            totalBalance=totalBalance+txnAmount
        if txnType=='C':
            totalBalance=totalBalance-txnAmount
        print("******************************** Inserting intial values *********************")
        insert_stmt="INSERT INTO ACCOUNT_DETAILS VALUES ('%s','%d','%d','%d','%f','%s','%d','%f','%d','%d')"%(user_id,totalBalance,day_closing_balance,no_txn,txnAmount,last_txn_day,days_of_txn,txnAmount,month_avg_balance,txnAmount)

        no_of_rows=cursor1.execute(insert_stmt)
        db.commit()
        #sleep(1)
        unique_users=unique_users+no_of_rows
        print("No. of unique users txn : "+str(unique_users))       
    else:
        for row in row_data:
            user_id=str(row[0])
            totalBalance=row[1]
            day_closing_balance=row[2]
            no_txn=row[3]
            avg_txn_amount=row[4]
            last_txn_day=row[5]
            days_of_txn=row[6]
            month_avg_balance=row[7]
            total_txn_value=row[8]
            days_txn_value=row[9]
            no_txn=no_txn+1
            if txnType=='C':
                totalBalance=totalBalance-txnAmount
                total_txn_value=total_txn_value-txnAmount
            if txnType=='D':
                totalBalance=totalBalance+txnAmount
                total_txn_value=total_txn_value+txnAmount
            
            if last_txn_day==txnDate:
                if txnType=='C':
                    day_closing_balance=day_closing_balance-txnAmount
                if txnType=='D':
                    day_closing_balance=day_closing_balance+txnAmount
                days_txn_value=day_closing_balance
            else:
                days_of_txn=days_of_txn+1
                last_txn_day=txnDate
                day_closing_balance=txnAmount
                if txnType=='C':
                    day_closing_balance=day_closing_balance-txnAmount
                if txnType=='D':
                    day_closing_balance=day_closing_balance+txnAmount
            
            avg_txn_amount=total_txn_value/no_txn
            current_diff=txnAmount-avg_txn_amount
            sq_of_diff=current_diff**2
            txn_variance=sq_of_diff/no_txn
            std_deviation=sqrt(txn_variance)
            print("current standard deviation value is %f",std_deviation)
            month_avg_balance=totalBalance/days_of_txn
            cursor2=db.cursor()

            insert_stmt1="UPDATE ACCOUNT_DETAILS SET TOTAL_BALANCE='%d',DAY_CLOSING_BALANCE='%d',NO_TXN='%d',AVG_TXN_AMOUNT='%f',LAST_TXN_DAY='%s',DAYS_OF_TXN='%d',MONTH_AVG_BALANCE='%f',TOTAL_TXN_VALUE='%d',DAYS_TXN_VALUE='%d' WHERE USER_ID='%s'"%(totalBalance,day_closing_balance,no_txn,avg_txn_amount,last_txn_day,days_of_txn,month_avg_balance,total_txn_value,txnAmount,userId)
            
            print("********************** UPDATING TRANSACTION VALUE **************************")

            cursor2.execute(insert_stmt1)
            db.commit()
            #sleep(1)





def consumeMsgs():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKERS, 
                         auto_offset_reset='earliest',client_id='client1',group_id='test-consumer-group')

    #consumer.subscribe(['transaction_data'])
    cursor=db.cursor()
    try:
        counter=0
        for message in consumer:
            data=message.value
            print(data)
            all_data=data.split('\t')
            userId=str(all_data[0])
            txnDate=str(all_data[1])
            txnType=str(all_data[2])
            txnAmount=int(all_data[3])
            print(message.value)
            consumer.commit()
            sql_stmt="insert into USER_TRANSACTION VALUES ('%s','%s','%s','%d')" %(userId,txnDate,txnType,txnAmount)
            no_rows=cursor.execute(sql_stmt)
            counter=counter+no_rows
            print("No of logs inserted : "+str(counter))
            db.commit()
            updateAccountDetails(userId,txnDate,txnType,txnAmount)
        print("No of logs inserted : "+str(counter))
        db.close()
    except KeyboardInterrupt:
        sys.exit()

if __name__ == '__main__':
    consumeMsgs()
