from utils.sqlserver_utils import mysql_connector
import yaml
from yaml.loader import SafeLoader
import os
from utils.check_data_files import expected_files, available_data_files, files_to_be_process, get_ts_from_path


with open("conf/incremental_paths.yml", 'r') as f:
    data = yaml.load(f, Loader=SafeLoader)

mysql_obj = mysql_connector(data['mydb']['driver'], data['mydb']['server'], data['mydb']['database'])

for file_path in data['SQLS']['DDL']:
    print(file_path)
    mysql_obj.create_sql(file_path)

mysql_obj.cursor_obj.execute("Select * From file_execution_audit_inc where usecase_name = 'EDM'")
for x in mysql_obj.cursor_obj:
    last_processed_file_ts = x[-1]

expected_files_lst = expected_files(data['base_path'], data['file_name'], data['execution_interval'], last_processed_file_ts)
available_data_files_lst = available_data_files(data['base_path'])
files_to_be_process_lst = files_to_be_process(available_data_files_lst, expected_files_lst)


for file_full_path in files_to_be_process_lst:
    if os.path.getsize(file_full_path) > 0:
        variable_dict = get_ts_from_path(file_full_path)
        for sql_file_path in data['SQLS']['DML']:
            load = sql_file_path.split('\\')
            if load[-2] == 'file_load':
                print('FILE_LOAD :: Executing SQL file  :', load[-1], 'for load dated : ', variable_dict['date_time'])
                mysql_obj.insert_sql_file_load(file_full_path, sql_file_path)
            elif load[-2] == 'table_load':
                print('TABLE_LOAD :: Executing SQL file  :', load[-1], 'for load dated : ', variable_dict['date_time'])
                mysql_obj.insert_sql_table_load(sql_file_path, variable_dict)
            elif file_full_path == files_to_be_process_lst[-1]:
                print('Updating last_processed_file_ts :: Executing SQL file  :', load[-1], 'for load dated : ', variable_dict['date_time'])
                mysql_obj.update_sql_control_table(sql_file_path, variable_dict)


