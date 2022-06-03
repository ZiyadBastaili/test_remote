import mysql.connector
import pandas as pd


class ConnectionDB:
    # Setup MySQL connection
    def __init__(self):
        # connect to RDS Amazon
        self.db = mysql.connector.connect(host="", user="admin",
                                          password="", database="spacs")


    # Get data with query
    def get_data_df(self, query):
        cursor = self.db.cursor()  # Create a Cursor object that will let you execute all the queries you need
        cursor.execute(query)  # Use all the SQL you like
        data = pd.DataFrame(cursor.fetchall())  # Put it all to a dataframe
        data.columns = cursor.column_names
        return data

    def get_data1(self, query, instance_insert):
        cursor = self.db.cursor()
        cursor.execute(query, instance_insert)
        data = pd.DataFrame(cursor.fetchall())
        if not data.empty:
            data.columns = cursor.column_names
        return data

    # Get data with query
    def get_data(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        return data

    # insert data
    def insert_data(self, query):
        cursor = self.db.cursor()
        cursor.execute(query[0], query[1])
        self.db.commit()

    # update data
    def update_data(self, query):
        cursor = self.db.cursor()
        cursor.execute(query[0], query[1])
        self.db.commit()

    # delete data
    def delete_data(self, query):
        cursor = self.db.cursor()
        cursor.execute("SET SQL_SAFE_UPDATES = 0")
        cursor.execute(query)
        self.db.commit()

    # create table
    def create_table_if_not_exist(self, old_table, new_table):
        cursor = self.db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS " + new_table + "  LIKE " + old_table)
        self.db.commit()

    # Get columns
    def get_table_columns(self, table):
        query = 'SELECT * FROM ' + table
        cursor = self.db.cursor(buffered=True)
        cursor.execute(query)
        column_names = cursor.column_names
        return column_names

    # Get tables by database
    def get_tables(self, database_name):
        cursor = self.db.cursor()
        cursor.execute("SHOW TABLES FROM " + database_name)
        return [x[0] for x in cursor]

    # execute query
    def execute(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        self.db.commit()


db = ConnectionDB()


def getData_df(query):
    df = db.get_data_df(query)
    return df


def getColumnsTable(table):
    columns = db.get_table_columns(table)
    return columns


def createTableIfNotExist(old_table, new_table):
    db.create_table_if_not_exist(old_table, new_table)


def getTables(database_name):
    tables = db.get_tables(database_name)
    return tables


def getData(query):
    data = db.get_data(query)
    return data


def getData1(query, instance_insert):
    data = db.get_data1(query, instance_insert)
    return data


def insertData(query):
    db.insert_data(query)


def updateData(query):
    db.update_data(query)


def deleteData(query):
    db.delete_data(query)


def exec(query):
    db.execute(query)


def close_db():
    global db
    db.close()



print(getData("show tables;"))
