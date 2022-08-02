from datetime import datetime, timedelta
import glob


base_path = 'E:\DATA\EDM\incremental'
file_name = 'emp_data.csv'

last_processed_file = '2022-07-29 07:10:00' # convert this sring into datetime
last_processed_file_ts = datetime.strptime(last_processed_file, '%Y-%m-%d %H:%M:%S')
# print(last_processed_file_ts)
# print(type(last_processed_file_ts))
cur_time = datetime.today()

diff = cur_time - last_processed_file_ts
# print(diff.total_seconds())
diff_hours = int((int(diff.total_seconds()) / 60)/60)

exepected_files = []
for x in range(1,diff_hours + 1):
    exepected_files_ts = last_processed_file_ts + timedelta(hours=x)
    dt = str(exepected_files_ts.date())
    hr = str(exepected_files_ts.hour).zfill(2)
    min = str(exepected_files_ts.minute).zfill(2)
    expected_path = "{base_path}\{exec_interval}\dt={dt}\hr={hr}\{file_name}".format(
                                                                                    base_path=base_path,
                                                                                    exec_interval='hourly',
                                                                                    dt=dt,
                                                                                    hr=hr,
                                                                                    file_name=file_name
                                                                                     )
    exepected_files.append(expected_path)
# print(exepected_files)



base_path = 'E:\DATA\EDM\incremental\**\*.*'

files_edm = glob.glob(base_path, recursive=True)
# print(files_edm)

file_tobe_process = []
for file_path in files_edm:
    if file_path in exepected_files:
        file_tobe_process.append(file_path)


print(file_tobe_process)