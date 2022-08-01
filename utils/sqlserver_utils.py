import pyodbc
from datetime import datetime, date
import os
import random

class mysql_connector:
    def __init__(self, driver, server, database, trusted_connection='yes', autocommit=True):
        self.today = datetime.today()
        self.conn = pyodbc.connect(Driver=driver,
                                   Server=server,
                                   Database=database,
                                   Trusted_Connection=trusted_connection,
                                   autocommit=autocommit)
        self.cursor_obj = self.conn.cursor()

    def create_sql(self, file_path):
        create_file = open(file_path, 'r')
        sql_create = create_file.read()
        for x in sql_create.split(';'):
            print(x)
            self.cursor_obj.execute(x)
        create_file.close()

    def insert_sql_raw(self, file_path_csv, file_path_sql):
        if os.path.getsize(file_path_csv) > 0:
            insert_raw = open(file_path_csv, 'r')
            data_insert_raw = insert_raw.readlines()
            insert_raw.close()
            insert_file = open(file_path_sql, 'r')
            sql_insert_raw = insert_file.read()
            insert_file.close()
            file_path_csv1 = file_path_csv.split('\\')
            hour_file = random.randint(0, 23)
            for rows in data_insert_raw:
                rows = rows.replace('\n', '')
                self.cursor_obj.execute(sql_insert_raw, *rows.split(','), self.today, file_path_csv, file_path_csv1[-1],
                                        '1990-01-01', hour_file)

    def insert_sql_final(self, insert_sql_filepath):
        insert_file = open(insert_sql_filepath, 'r')
        sql_insert_final = insert_file.read()
        self.cursor_obj.execute(sql_insert_final)
        insert_file.close()
