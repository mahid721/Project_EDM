from os import walk
from datetime import datetime, timedelta

def expected_files(base_path, file_name, execution_interval, last_processed_file_ts):
    # last_processed_file_ts = datetime.strptime(last_processed_file_ts, '%Y-%m-%d %H:%M:%S')
    cur_time = datetime.today()
    diff = cur_time - last_processed_file_ts
    diff_hours = int((int(diff.total_seconds()) / 60)/60)
    exepected_files = []
    for x in range(1,diff_hours + 1):
        exepected_files_ts = last_processed_file_ts + timedelta(hours=x)
        dt = str(exepected_files_ts.date())
        hr = str(exepected_files_ts.hour).zfill(2)
        min = str(exepected_files_ts.minute).zfill(2)
        expected_path = "{base_path}\{exec_interval}\dt={dt}\hr={hr}\{file_name}".format(
                                                                                        base_path=base_path,
                                                                                        exec_interval=execution_interval,
                                                                                        dt=dt,
                                                                                        hr=hr,
                                                                                        file_name=file_name
                                                                                         )
        exepected_files.append(expected_path)
    return exepected_files

def available_data_files(base_path):
    files_on_disk = []
    for (dir_path, dir_names, file_names) in walk(base_path):
        if file_names != []:
            file_path = dir_path + '\\' + file_names[0]
            files_on_disk.append(file_path)
    return files_on_disk

def files_to_be_process(files_on_disk, exepected_files):
    files_to_be_process = []
    for file_path in files_on_disk:
        if file_path in exepected_files:
            files_to_be_process.append(file_path)
    return files_to_be_process

def get_ts_from_path(file_full_path):
    variable_dict = {}
    variable_dict['file_fullpath']= file_full_path
    variable_dict['file_name'] = file_full_path.split('\\')[-1]
    if 'dt=' in file_full_path and 'hr=' in file_full_path and 'min=' in file_full_path:
        path_values = file_full_path.split('\\')
        variable_dict['dt'] = path_values[-4].split('=')[-1]
        variable_dict['hr'] = path_values[-3].split('=')[-1]
        variable_dict['min'] = path_values[-2].split('=')[-1]
        variable_dict['date_time'] = str(variable_dict['dt']) + ' ' + str(variable_dict['hr']) + ':' + str(variable_dict['min']) + ':' + '00'
    elif 'dt=' in file_full_path and 'hr=' in file_full_path:
        path_values = file_full_path.split('\\')
        variable_dict['dt'] = path_values[-3].split('=')[-1]
        variable_dict['hr'] = path_values[-2].split('=')[-1]
        variable_dict['date_time'] = str(variable_dict['dt']) + ' ' + str(variable_dict['hr']) + ':' + '00' + ':' + '00'
    return variable_dict