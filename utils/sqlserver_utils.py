import pyodbc
from datetime import datetime


class mysql_connector:
    def __init__(self, Driver, Server, Database, Trusted_Connection='yes', autocommit=True):
        self.today = datetime.today()
        self.conn = pyodbc.connect(Driver=Driver,
                                   Server=Server,
                                   Database=Database,
                                   Trusted_Connection=Trusted_Connection,
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
        insert_raw = open(file_path_csv, 'r')
        data_insert_raw = insert_raw.readlines()
        insert_raw.close()
        insert_file = open(file_path_sql, 'r')
        sql_insert_raw = insert_file.read()
        insert_file.close()
        file_path_csv1 = file_path_csv.split('\\')
        if file_path_csv1[-3][:3] == 'dt=':
            date_file = file_path_csv1[-3][3:]
            print(date_file)
        if file_path_csv1[-2][:3] == 'hr=':
            hour = file_path_csv1[-2][3:]
            print(hour)
        for rows in data_insert_raw:
            rows = rows.replace('\n', '')
            self.cursor_obj.execute(sql_insert_raw, *rows.split(','), self.today, file_path_csv, file_path_csv1[-1], date_file,
                                    hour, )

    def insert_sql_final(self, insert_sql_filepath):
        insert_file = open(insert_sql_filepath, 'r')
        sql_insert_final = insert_file.read()
        self.cursor_obj.execute(sql_insert_final)
        insert_file.close()
