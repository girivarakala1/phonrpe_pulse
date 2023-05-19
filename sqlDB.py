import mysql.connector as sql
import pandas as pd

mydb = sql.connect(
    host = "localhost",
    user = "root",
    password = "",
    #database = ""
)

print(mydb)
mycursor = mydb.cursor(buffered = True)

#mycursor.execute("CREATE DATABASE phonepe_pulse")
mycursor.execute("USE phonepe_pulse")
def agg_trans_table():
    mycursor.execute("""
    CREATE TABLE IF NOT EXISTS agg_transaction(
      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      State VARCHAR(255),
      Year INT,
      Quarter INT,
      Transaction_type TEXT,
      Transaction_count INT,
      Transaction_amount INT
    );
    """)

    df = pd.read_csv("C:\\Users\\giriv\\phonepe_pulse\\agg_transaction.csv")

    for index, row in df.iterrows():
        query1 = "INSERT INTO phonepe_pulse.agg_transaction(State,Year,Quarter,Transaction_type,Transaction_count,Transaction_amount) values(%s,%s,%s,%s,%s,%s)"
        val = (row.State, row.Year, row.Quarter, row.Transaction_type, row.Transaction_count, row.Transaction_amount)
        mycursor.execute(query1, val)

    print("data inserted successfully into table df1")
    # commit the changes
    mydb.commit()

def agg_user_table():
    mycursor.execute("""
    CREATE TABLE IF NOT EXISTS agg_users(
      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      State VARCHAR(255),
      Year INT,
      Quarter INT,
      Brand TEXT,
      Count INT,
      Percentage FLOAT 
      );
    """)

    df = pd.read_csv("C:\\Users\\giriv\\phonepe_pulse\\agg_users.csv")

    # insert data from dataframe into database
    for index, row in df.iterrows():
        query2 = "INSERT INTO phonepe_pulse.agg_users(State,Year,Quarter,Brand,Count,Percentage) values(%s,%s,%s,%s,%s,%s)"
        val2 = (row.State, row.Year, row.Quarter, row.Brand, row.Count, row.Percentage)
        mycursor.execute(query2, val2)

    mydb.commit()


def map_trans_table():
    mycursor.execute("""
           CREATE TABLE IF NOT EXISTS map_transaction(
             id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
             State VARCHAR(255),
             Year INT,
             Quarter INT,
             District VARCHAR(255),
             Count INT,
             Amount FLOAT
           );
           """)

    df = pd.read_csv("C:\\Users\\giriv\\phonepe_pulse\\map_transcation.csv")

    ## insert data from dataframe into database
    for index, row in df.iterrows():
        query3 = "INSERT INTO phonepe_pulse.map_transaction(State,Year,Quarter,District,Count,Amount) values(%s,%s,%s,%s,%s,%s)"
        val3 = (row.State, row.Year, row.Quarter, row.District, row.Count, row.Amount)
        mycursor.execute(query3, val3)

    mydb.commit()


def map_user_table():
    mycursor.execute("""
          CREATE TABLE IF NOT EXISTS map_users(
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            State VARCHAR(255),
            Year INT,
            Quarter INT,
            District VARCHAR(255),
            Users INT
          );
          """)

    df = pd.read_csv("C:\\Users\\giriv\\phonepe_pulse\\map_users.csv")

    for index, row in df.iterrows():
        query4 = "INSERT INTO phonepe_pulse.map_users(State,Year,Quarter,District,Users) values(%s,%s,%s,%s,%s)"
        val4 = (row.State, row.Year, row.Quarter, row.District, row.Users)
        mycursor.execute(query4, val4)

    mydb.commit()


def top_trans_table():
    mycursor.execute("""
    CREATE TABLE IF NOT EXISTS top_transaction(
      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
      state VARCHAR(255),
      Year INT,
      Quarter INT,
      District VARCHAR(255),
      Count INT,
      Amount INT
    );
    """)

    df = pd.read_csv("C:\\Users\\giriv\\phonepe_pulse\\top_transcation.csv")

    for index, row in df.iterrows():
        query5 = "INSERT INTO phonepe_pulse.top_transaction(State,Year,Quarter,District,Count,Amount) values(%s,%s,%s,%s,%s,%s)"
        val5 = (row.State, row.Year, row.Quarter, row.District, row.Count, row.Amount)
        mycursor.execute(query5, val5)

    mydb.commit()


def top_user_table():
    mycursor.execute("""
         CREATE TABLE IF NOT EXISTS top_Users(
           id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
           State VARCHAR(225),
           Year INT,
           Quarter INT,
           District VARCHAR(255),
           Users INT
         );
         """)

    df = pd.read_csv("C:\\Users\\giriv\\phonepe_pulse\\top_users.csv")

    for index, row in df.iterrows():
        query6 = "INSERT INTO phonepe_pulse.top_Users(State,Year,Quarter,District,Users) values(%s,%s,%s,%s,%s)"
        val6 = (row.State, row.Year, row.Quarter, row.District, row.Users)
        mycursor.execute(query6, val6)

    mydb.commit()

def main():
    agg_trans_table()
    agg_user_table()
    map_trans_table()
    map_user_table()
    top_trans_table()
    top_user_table()





