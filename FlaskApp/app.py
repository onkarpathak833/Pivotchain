from flask import Flask,render_template,json,request
import MySQLdb

def getConnection():
    conn = MySQLdb.connect(host="test-transaction-instance.cx0hq28hason.us-east-1.rds.amazonaws.com",
                           user="onkar",
                           passwd="onkar1712",
                           db = "pivotchain")
    c = conn.cursor()

    return c, conn



app = Flask(__name__)

@app.route("/")
def main():
    return "welcome to Flask App!"
    #return render_template("index.html")

@app.route("/transactions")
def showpage():
    return render_template("index.html")



@app.route("/data")
def getAllUsers():
    c,conn=getConnection()
    selectAllQuery="SELECT USER_ID,TOTAL_BALANCE,AVG_TXN_AMOUNT,LAST_TXN_DAY,MONTH_AVG_BALANCE  FROM ACCOUNT_DETAILS"
    c.execute(selectAllQuery)
    alldata=c.fetchall()
    return render_template("data.html",data=alldata)



@app.route("/userdata",methods=['POST'])
def getUserData():
    userid=request.form['UserId']
    print("User Id to query is :"+str(userid))
    c,conn=getConnection()
    selectUserQuery="SELECT USER_ID,TOTAL_BALANCE,AVG_TXN_AMOUNT,LAST_TXN_DAY,MONTH_AVG_BALANCE  FROM ACCOUNT_DETAILS WHERE USER_ID='%s'"%(userid)
    c.execute(selectUserQuery)
    userData=c.fetchall()
    if c.rowcount==0:
        return render_template("error.html")
    else:
        return render_template("data.html",data=userData)


@app.route("/alltransactions",methods=['POST'])
def showAllUsers():
    userid=request.form['UserId']
    c,conn=getConnection()
    selectAllQuery="SELECT USER_ID,TOTAL_BALANCE,AVG_TXN_AMOUNT,LAST_TXN_DAY,MONTH_AVG_BALANCE  FROM ACCOUNT_DETAILS"
    c.execute(selectAllQuery)
    alldata=c.fetchall()
    return render_template("data.html",data=alldata)
    


if __name__ == "__main__":
    app.run(host='ec2-54-173-60-38.compute-1.amazonaws.com',port=5000)
