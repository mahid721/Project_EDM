from utils.sqlserver_utils import mysql_connector
import yaml
from yaml.loader import SafeLoader


with open("conf/onetime_db_paths.yml", 'r') as f:
    data = yaml.load(f, Loader=SafeLoader)
    # print(data['mydb']['driver'])

mysql_obj = mysql_connector(data['mydb']['driver'], data['mydb']['server'], data['mydb']['database'])

mysql_obj.cursor_obj.execute("select * From temp_emp_table")


for x in mysql_obj.cursor_obj:
    print(x)
