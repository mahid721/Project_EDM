# import turtle
#
# a = turtle.Turtle()
# a.getscreen().bgcolor("black")
#
# a.penup()
# a.goto(-200, 100)
# a.pendown()
# a.color("red")
#
# a.speed(100)
# def star(turtle, size):
#     if size <= 10:
#         return
#     else:
#         turtle.begin_fill()
#         for i in range(5):
#             turtle.forward(size)
#             star(turtle, size/3)
#             turtle.left(216)
#             turtle.end_fill()
#
# star(a, 100)
# turtle.done()
#
#
#
#
#
#
#
#
# import pyodbc
# from datetime import datetime
# import yaml
# from yaml.loader import SafeLoader
#
#
# class mysql_connector:
#     def __init__(self, Driver, Server,Database,Trusted_Connection='yes',autocommit=True):
#         conn = pyodbc.connect(Driver=Driver,
#                               Server=Server,
#                               Database=Database,
#                               Trusted_Connection='yes',
#                               autocommit=True)
#
#     def create_sql (self, file_path):
#         self.cursor_obj = self.conn.cursor()
#         create_file = open(file_path, 'r')
#         sql_create = create_file.read()
#         self.cursor_obj.execute(sql_create)
#         create_file.close()
#
#
#     def insert_sql_raw (self, file_path_csv, file_path_sql):
#         insert_raw = open(file_path_csv, 'r')
#         data_insert_raw = insert_raw.readlines()
#         insert_raw.close()
#         today = datetime.today()
#         insert_file = open(file_path_sql, 'r')
#         sql_insert_raw = insert_file.read()
#         insert_file.close()
#         for rows in data_insert_raw:
#             rows = rows.replace('\n', '')
#             self.cursor_obj.execute(sql_insert_raw, *rows.split(','), today)
#
#
#     def insert_sql_final (self, insert_sql_filepath):
#         today = datetime.today()
#         insert_file = open(insert_sql_filepath, 'r')
#         sql_insert_final = insert_file.read()
#         self.cursor_obj.execute(sql_insert_final)
#         insert_file.close()
#
#
#
# # Open the file and load the file
#
# # with open("my_db.yml", 'r') as f:
# #     data = yaml.load(f, Loader=SafeLoader)
# #     # print(data['mydb']['driver'])
# # print(data['mydb']['driver'])
#
#
#
#
#
#
# # create_sql('D:\Project_New\SQL\Onetime\create_raw_emp_tbl.sql')
#
# # insert_sql_raw('D:\Project_New\Project_emp_data\DT=2022-07-15\HR=10\min=21\Employee_V1.csv','D:\Project_New\SQL\Onetime\insert_raw_emp_tbl.sql')
#
# # create_sql('D:\Project_New\SQL\Onetime\create_final_emp_tbl.sql')
# #
# # insert_sql_final('D:\Project_New\SQL\Onetime\insert_final_emp_tbl.sql')
#
#
# # cursor_obj.execute('Select * from emp_data1')
# # for x in cursor_obj:
# #     print(x)



n = random.randint(0, 22)
print(n)