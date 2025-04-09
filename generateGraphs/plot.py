import matplotlib.pyplot as plt
import sys
import math
import copy 

data = {}
runs = int(sys.argv[1])
file = (sys.argv[2])

def getNumber(s):
    output = ""
    for i in range(len(s)):
        if s[i].isdigit():
            output += s[i]
    return output

minnx = math.inf
maxx = -math.inf

miny = math.inf
maxy = -math.inf

# so we have a bunch of files that we need to parse
for session in range(0, 6):
    x = []
    y = []
    for i in range(1, runs+1):
        f = open(file + "/" + str(session) + "/" + str(i), "r")
        contents = f.read().splitlines()
        throughput = getNumber(contents[3])
        latency = getNumber(contents[4])
        x.append(int(throughput))
        y.append(int(latency))

        minnx = min(int(throughput), minnx)
        maxx = max(int(throughput), maxx)

        miny = min(int(latency), miny)
        maxy = max(int(latency), maxy)
    
    if session == 0:
        data["eventual"] = [x[:], y[:]]
    if session == 1:
        data["writes follow reads"] = [x[:], y[:]]
    if session == 2:
        data["monotonic writes"] = [x[:], y[:]]
    if session == 3:
        data["monotonic reads"] = [x[:], y[:]]
    if session == 4:
        data["read your writes"] = [x[:], y[:]]
    if session == 5:
        data["causal"] = [x[:], y[:]]
    
plt.xlabel('throughput (ops/sec)')
plt.ylabel('latency (ms)')
plt.axis([minnx,maxx, miny,maxy])

# print(data)
for session in data:
    x,y = data[session]
    print(x,y)
    if session == "eventual":
        plt.plot(x, y, marker='o', color='b', label = session)
    if session == "writes follow reads":
        plt.plot(x, y, marker='*', color='r', label = session)
    if session == "monotonic writes":
        plt.plot(x, y, marker='h', color='g', label = session)
    if session == "read your writes":
        plt.plot(x, y, marker='.', color='y', label = session)
    if session == "causal":
        plt.plot(x, y, marker='x', color='m', label = session)

# print(minnx,maxx, miny,maxy)
# print(0,maxx)
# plt.xticks(range(minnx,maxx))
# plt.yticks(range(miny,maxy))
# plt.axis((minnx, maxx, miny, maxy))
# plt.xlim([0,50])
plt.legend()

plt.savefig('tl.png')



