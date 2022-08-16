import logging
from datetime import datetime



def log_file_name(log_file_path, log_file_prefix):
    log_file_path = log_file_path
    log_file_prefix = log_file_prefix
    cur_ts = datetime.today()
    cur_yr = str(cur_ts.year)
    cur_mnt = str(cur_ts.month).zfill(2)
    cur_dt = str(cur_ts.day).zfill(2)
    cur_hr = str(cur_ts.hour).zfill(2)
    cur_min = str(cur_ts.minute).zfill(2)
    cur_sec = str(cur_ts.second).zfill(2)
    log_file_ts = cur_yr + '_' + cur_mnt + '_' + cur_dt + '_' + cur_hr + '_' + cur_min + '_' + cur_sec
    logging.basicConfig(filename='{log_file_path}{log_file_prefix}_{log_file_ts}.log'.format(log_file_path = log_file_path, log_file_prefix = log_file_prefix , log_file_ts = log_file_ts),
                        encoding='utf-8', level=logging.DEBUG,
                        filemode='w'
                        # format= '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                        )
    return logging