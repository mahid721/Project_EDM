import logging
import pyodbc

class mysql_connector:
    def __init__(self, driver, server, database, trusted_connection='yes', autocommit=True):
        try:
            self.conn = pyodbc.connect(Driver=driver,
                                       Server=server,
                                       Database=database,
                                       Trusted_Connection=trusted_connection,
                                       autocommit=autocommit )
            self.cursor_obj = self.conn.cursor()
        except Exception as e:
            logging.error('Error Due to ' + str(e))
            raise Exception(e)


    def file_read_sql(self, file_path):
        try:
            logging.info('This is info message -- file_read_sql -- ' + str(file_path))
            open_file = open(file_path, 'r')
            read_file = open_file.read()
            open_file.close()
            return read_file
        except Exception as e:
            logging.error('Error Due to ' + str(e))
            raise Exception(e)


    def file_read_csv(self, file_path):
        try:
            logging.info('This is info message -- read_csv_file -- ' + str(file_path))
            open_file = open(file_path, 'r')
            read_file = open_file.readlines()
            open_file.close()
            return read_file
        except Exception as e:
            logging.error('Error Due to ' + str(e))
            raise Exception(e)


    def create_sql(self, file_path):
        try:
            sql_create = self.file_read_sql(file_path)
            for x in sql_create.split(';'):
                logging.info('create sql file/query ' + str(x))
                self.cursor_obj.execute(x)
        except Exception as e:
            logging.error('Error Due to ' + str(e))
            raise Exception(e)


    def insert_sql_file_load(self, file_path_csv, file_path_sql):
        try:
            data_insert_raw = self.file_read_csv(file_path_csv)
            logging.info('reading data/csv file' + str(data_insert_raw))
            sql_insert_temp = self.file_read_sql(file_path_sql)
            logging.info('reading sql file/query' + str(sql_insert_temp))
            for rows in data_insert_raw:
                rows = rows.replace('\n', '')
                self.cursor_obj.execute(sql_insert_temp, *rows.split(','))
                logging.info('Inserting data into temp table from csv file' + str(sql_insert_temp) + str(rows.split(',')))
        except Exception as e:
            logging.error('Error Due to ' + str(e))
            raise Exception(e)

    def insert_sql_table_load(self, insert_sql_filepath, variable_dict):
        try:
            sql_insert_table_load = self.file_read_sql(insert_sql_filepath)
            logging.info('reading sql file/query' + str(sql_insert_table_load))
            self.cursor_obj.execute(sql_insert_table_load.format(**variable_dict))
            logging.info('Inserting data into  table' + str(sql_insert_table_load.format(**variable_dict)))
        except Exception as e:
            logging.error('Error Due to ' + str(e))
            raise Exception(e)


    def update_sql_control_table(self, update_sql_filepath, variable_dict):
        try:
            sql_update_file = self.file_read_sql(update_sql_filepath)
            logging.info('reading sql file/query' + str(sql_update_file))
            self.cursor_obj.execute(sql_update_file.format(**variable_dict))
            logging.info('Updating last processed file  into control table' + str(sql_update_file.format(**variable_dict)))
        except Exception as e:
            logging.error('Error Due to ' + str(e))
            raise Exception(e)



