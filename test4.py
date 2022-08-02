from os import walk

base_path = 'E:\DATA\EDM\incremental'

files_edm = []
for (dir_path, dir_names, file_names) in walk(base_path):
    if file_names != []:
        file_path = dir_path + '\\' + file_names[0]
        files_edm.append(file_path)


