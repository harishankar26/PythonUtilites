import scrapy
import sqlite3
from sqlite3 import Error


class DataAccessSqlLite:

    def __init__(self, dbName):
        self.dbName = dbName
        self.create_connection(self.dbName)

    def create_connection(self, db_file):
        try:
            self.conn = sqlite3.connect(db_file)
            self.cursor = self.conn.cursor()
        except Error as e:
            print(e)
    
        return self.conn

    def close_connection(self):
        try:
            self.conn.close()
        except Error as e:
            print(e)

    def create_table(self, sql):
        try:
            self.cursor.execute(sql)
        except Error as e:
            print(e)

    def exec_query(self, sql ,query):
        self.cursor.execute(sql, query)

    def exec_query_commit(self, sql):
        cur = self.cursor
        cur.execute(sql)
        self.conn.commit()

    def exec_insert_query(self, sql, value = ''):
        cur = self.cursor
        self.exec_query(sql, value)
        self.conn.commit()
        return cur.lastrowid

    def exec_update_query(self,  sql, value):
        self.exec_query_commit(sql, value)

    def exec_delete(self, sql, value):
        self.exec_query_commit(sql, value)
    
    def exec_select(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        return rows
