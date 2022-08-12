import logging
import sys
import pyodbc
from logs.log_file_name_create import log_file_name




# log_file_name('C:\\Users\\Lenovo\\PycharmProjects\\Project_EDM\\logs\\', 'sqlserver_utils')

class mysql_connector:
    log_file_name('C:\\Users\\Lenovo\\PycharmProjects\\Project_EDM\\logs\\', 'sqlserver_utils')
    try:
        def __init__(self, driver, server, database, trusted_connection='yes', autocommit=True):
            self.conn = pyodbc.connect(Driver=driver,
                                       Server=server,
                                       Database=database,
                                       Trusted_Connection=trusted_connection,
                                       autocommit=autocommit )
            self.cursor_obj = self.conn.cursor()
    except Exception as e:
        logging.info('Error Due to ', e)
        sys.exit()

    try:
        def file_read_sql(self, file_path):
            logging.info('This is info message -- file_read_sql -- ', file_path)
            open_file = open(file_path, 'r')
            read_file = open_file.read()
            open_file.close()
            return read_file
    except Exception as e:
        logging.error('Error Due to ', e)
        sys.exit()

    try:
        def file_read_csv(self, file_path):
            open_file = open(file_path, 'r')
            read_file = open_file.readlines()
            open_file.close()
            return read_file
    except Exception as e:
        logging.error('Error Due to ', e)
        sys.exit()

    try:
        def create_sql(self, file_path):
            sql_create = self.file_read_sql(file_path)
            for x in sql_create.split(';'):
                logging.info('create sql file/query ', x)
                self.cursor_obj.execute(x)
    except Exception as e:
        logging.error('Error Due to ', e)
        sys.exit()

    try:
        def insert_sql_file_load(self, file_path_csv, file_path_sql):
            data_insert_raw = self.file_read_csv(file_path_csv)
            logging.info('reading data/csv file', data_insert_raw)
            sql_insert_temp = self.file_read_sql(file_path_sql)
            logging.info('reading sql file/query', sql_insert_temp)
            for rows in data_insert_raw:
                rows = rows.replace('\n', '')
                print(rows)
                print(sql_insert_temp, *rows.split(','))
                print(rows.split(','))
                print(type(rows.split(',')))
                self.cursor_obj.execute(sql_insert_temp, *rows.split(','))
                # logging.info('Inserting data into temp table from csv file', sql_insert_temp, *rows.split(','))
    except Exception as e:
        logging.error('Error Due to ', e)
        sys.exit()

    try:
        def insert_sql_table_load(self, insert_sql_filepath, variable_dict):
            sql_insert_table_load = self.file_read_sql(insert_sql_filepath)
            logging.info('reading sql file/query', sql_insert_table_load)
            print(sql_insert_table_load.format(**variable_dict))
            self.cursor_obj.execute(sql_insert_table_load.format(**variable_dict))
            logging.info('Inserting data into  table', sql_insert_table_load.format(**variable_dict))
    except Exception as e:
        logging.error('Error Due to ', e)
        sys.exit()

    try:
        def update_sql_control_table(self, update_sql_filepath, variable_dict):
            sql_update_file = self.file_read_sql(update_sql_filepath)
            logging.info('reading sql file/query', sql_update_file)
            self.cursor_obj.execute(sql_update_file.format(**variable_dict))
            logging.info('Updating last processed file  into control table', sql_update_file.format(**variable_dict))
    except Exception as e:
        logging.error('Error Due to ', e)
        sys.exit()
