import os
from utils.sqlserver_utils import mysql_connector
import yaml
from yaml.loader import SafeLoader


with open("conf/onetime_db_paths.yml", 'r') as f:
    data = yaml.load(f, Loader=SafeLoader)
    # print(data['mydb']['driver'])

mysql_obj = mysql_connector(data['mydb']['driver'], data['mydb']['server'], data['mydb']['database'])

for file_path in data['SQLS']['DDL']:
    print(file_path)
    mysql_obj.create_sql(file_path)


for file_csv in data['data_file_path']:
    if os.path.getsize(file_csv) > 0:
        for file_path in data['SQLS']['DML']:
            load = file_path.split('\\')
            if load[-2] == 'file_load':
                print('file_load path this is :', file_path)
                mysql_obj.insert_sql_file_load(file_csv, file_path)
            elif load[-2] == 'table_load':
                print('table_load file path this is :', file_path)
                mysql_obj.insert_sql_table_load(file_path)

# for file_path in data['SQLS']['DML']:
#     if 'raw' in file_path:
#         print('raw file path this is :', file_path)
#         mysql_obj.insert_sql_raw(data['data_file_path'], file_path)
#     elif 'final' in file_path:
#         print('final file path this is :', file_path)
#         mysql_obj.insert_sql_final(file_path)
