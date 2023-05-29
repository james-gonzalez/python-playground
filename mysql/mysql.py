import mysql.connector as msql
import pandas as pandas

try:
    connection = msql.connect(
        host="localhost", database="classicmodels", user="root", password="pass123"
    )

    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)  # getting the server info
        cursor = connection.cursor()
        cursor.execute("select database();")  # selecting the database diamond
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor = connection.cursor()

    cursor.execute("SELECT * from customers")
    tables_rows = cursor.fetchall()
    cursor.execute("SHOW columns FROM customers")
    columns = [column[0] for column in cursor.fetchall()]

    df = pandas.DataFrame(tables_rows, columns=columns)

    cleaned_df = df.drop(columns=["country", "customerNumber", "state"])

    print(cleaned_df)


except msql.Error as err:
    print("Error while connecting to MySQL", err)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("mysql connection is closed")
