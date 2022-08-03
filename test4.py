from utils.sqlserver_utils import mysql_connector
import yaml
from yaml.loader import SafeLoader
import os
from utils.check_data_files import expected_files, available_data_files, files_to_be_process, get_ts_from_path


with open("conf/incremental_paths.yml", 'r') as f:
    data = yaml.load(f, Loader=SafeLoader)

mysql_obj = mysql_connector(data['mydb']['driver'], data['mydb']['server'], data['mydb']['database'])


mysql_obj.cursor_obj.execute("Select * From raw_emp_table_inc")
for x in mysql_obj.cursor_obj:
    print(x)

