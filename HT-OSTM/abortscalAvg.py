#/*
#DESCRIPTION         :   script to collect aborts.
#*AUTHOR             :   AJAY SINGH (M.TECH. Student)
#*INSTITUTE          :   IIT Hyderabad
#*DATE               :   Jan 2018
#*/

with open(r'numaborts.txt') as f:
    data = [float(line.rstrip()) for line in f]

biggest = min(data)
smallest = max(data)
print(biggest - smallest)
print(sum(data)/len(data))
