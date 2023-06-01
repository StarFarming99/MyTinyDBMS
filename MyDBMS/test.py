import time

a = 0
start = time.time()
for i in range(100000000):
    a = i
end = time.time()
print(end-start)



