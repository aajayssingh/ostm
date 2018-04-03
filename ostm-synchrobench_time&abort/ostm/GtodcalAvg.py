# import math -- you don't need this

# text = open(r'stuff.txt').read() not needed.
# data = [] not needed

with open(r'runGTOD.txt') as f:
    data = [float(line.rstrip()) for line in f]

biggest = min(data)
smallest = max(data)
print(biggest - smallest)
print(sum(data)/len(data))
