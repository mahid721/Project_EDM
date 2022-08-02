import pyodbc
from datetime import datetime
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

    def insert_sql_file_load(self, file_path_csv, file_path_sql):
        self.file_path_csv = file_path_csv
        insert_temp = open(file_path_csv, 'r')
        data_insert_raw = insert_temp.readlines()
        insert_temp.close()
        insert_file = open(file_path_sql, 'r')
        sql_insert_temp = insert_file.read()
        insert_file.close()
        for rows in data_insert_raw:
            rows = rows.replace('\n', '')
            self.cursor_obj.execute(sql_insert_temp, *rows.split(','))

    def insert_sql_table_load(self, insert_sql_filepath, variable_dict):
        insert_file = open(insert_sql_filepath, 'r')
        sql_insert_table_load = insert_file.read()
        print(sql_insert_table_load.format(**variable_dict))
        self.cursor_obj.execute(sql_insert_table_load.format(**variable_dict))
        insert_file.close()



    def insert_sql_control_table(self, insert_sql_filepath, usecase, loadinterval, lastprocess):
        insert_file = open(insert_sql_filepath, 'r')
        sql_insert_table_load = insert_file.read()
        self.cursor_obj.execute(sql_insert_table_load,usecase_name = usecase, load_interval= loadinterval, last_process = lastprocess)
        insert_file.close()





