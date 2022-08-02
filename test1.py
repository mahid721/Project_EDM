from operator import concat

path_list = ['E:\DATA\EDM\incremental\hourly\dt=2022-05-10\hr=13\emp_data.csv',
        'E:\DATA\EDM\incremental\hourly\dt=2022-05-10\hr=13\min=02\emp_data.csv', 'E:\DATA\EDM\incremental\hourly']


for full_path in path_list:
    if 'dt=' in full_path and 'hr=' in full_path and 'min=' in full_path:
        path_values = full_path.split('\\')
        dt = path_values[-4].split('=')[-1]
        hr = path_values[-3].split('=')[-1]
        min = path_values[-2].split('=')[-1]
        date_time = str(dt) + ' ' + str(hr) + ':' + str(min) + ':' + '00'
        print(date_time)
    elif 'dt=' in full_path and 'hr=' in full_path:
        path_values = full_path.split('\\')
        dt = path_values[-3].split('=')[-1]
        hr = path_values[-2].split('=')[-1]
        date_time = str(dt) + ' ' + str(hr) + ':' + '00' + ':' + '00'
        print(date_time)



#  2022-05-10 13:00:00

