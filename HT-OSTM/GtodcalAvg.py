#/*
#DESCRIPTION         :   script to collect time.
#*AUTHOR             :   AJAY SINGH (M.TECH. Student)
#*INSTITUTE          :   IIT Hyderabad
#*DATE               :   Jan 2018
#*/

# data = [] not needed

with open(r'runGTOD.txt') as f:
    data = [float(line.rstrip()) for line in f]

biggest = min(data)
smallest = max(data)
print(biggest - smallest)
print(sum(data)/len(data))
