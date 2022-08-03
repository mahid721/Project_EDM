import pyodbc



class mysql_connector:
    def __init__(self, driver, server, database, trusted_connection='yes', autocommit=True):
        self.conn = pyodbc.connect(Driver=driver,
                                   Server=server,
                                   Database=database,
                                   Trusted_Connection=trusted_connection,
                                   autocommit=autocommit )
        self.cursor_obj = self.conn.cursor()

    def file_read_sql(self, file_path):
        open_file = open(file_path, 'r')
        read_file = open_file.read()
        open_file.close()
        return read_file

    def file_read_csv(self, file_path):
        open_file = open(file_path, 'r')
        read_file = open_file.readlines()
        open_file.close()
        return read_file

    def create_sql(self, file_path):
        sql_create = self.file_read_sql(file_path)
        for x in sql_create.split(';'):
            print(x)
            self.cursor_obj.execute(x)

    def insert_sql_file_load(self, file_path_csv, file_path_sql):
        data_insert_raw = self.file_read_csv(file_path_csv)
        sql_insert_temp = self.file_read_sql(file_path_sql)
        for rows in data_insert_raw:
            rows = rows.replace('\n', '')
            print(sql_insert_temp, *rows.split(','))
            self.cursor_obj.execute(sql_insert_temp, *rows.split(','))

    def insert_sql_table_load(self, insert_sql_filepath, variable_dict):
        sql_insert_table_load = self.file_read_sql(insert_sql_filepath)
        print(sql_insert_table_load.format(**variable_dict))
        self.cursor_obj.execute(sql_insert_table_load.format(**variable_dict))

    def update_sql_control_table(self, update_sql_filepath, variable_dict):
        sql_update_file = self.file_read_sql(update_sql_filepath)
        print(sql_update_file.format(**variable_dict))
        self.cursor_obj.execute(sql_update_file.format(**variable_dict))
