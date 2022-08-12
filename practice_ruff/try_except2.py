import sys

lst = [1, 2, 3, 4,  5, 6]
try:
    for x in range(len(lst)):
        print(lst[x+1])
except Exception as e:
    print(e)
    tb = sys.exc_info()[2]
    print(e.with_traceback(tb))