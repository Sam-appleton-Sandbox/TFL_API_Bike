from sys import platform

if platform == "linux" or platform == "linux2":
    OS = 'linux'
elif platform == "darwin":
    OS =  'X'
elif platform == "win32":
    OS = 'Windows'
    
print(OS)