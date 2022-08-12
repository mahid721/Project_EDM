import sys

lst = [1, 2, 3, 4,  5, 6]

try:
    for x in range(len(lst)):
        print(lst[x+1])
except NameError as e:
    print('Got Errror as ', e)
except IndexError as e:
    print('Got Errror as ', e)
    sys.exit()
finally:
    print('Helloo Hiii')

print('-------------11111111111111111--------------------')


#
# lst = [1, 2, 3, 4, 5, 6]
#
# for x in range(len(lst)):
#     print(lst[x+1])