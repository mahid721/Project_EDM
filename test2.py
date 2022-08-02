from datetime import datetime, timedelta
import glob

base_path = 'E:\DATA\EDM\incremental'
file_name = 'emp_data.csv'

last_processed_file = '2022-07-29 13:00:00'
last_processed_file = datetime.fromisoformat(last_processed_file)
cur_time = datetime.today()

exepected_files = []

def hourly_it(start, finish):
    while finish > start:
        start = start + timedelta(hours=1)
        yield start

for hour in hourly_it(last_processed_file, cur_time):
    file_path = base_path + '\hourly' + '\dt=' + str(hour).split(' ')[0] + '\hr=' + str(hour).split(' ')[1].split(':')[0] +'\\' + file_name
    print(file_path)
    exepected_files.append(file_path)




base_path = 'E:\DATA\EDM\incremental\**\*.*'
files_edm = glob.glob(base_path, recursive=True)
print(files_edm)

