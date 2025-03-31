import matplotlib.pyplot as plt
import sys

files = int(sys.argv[1])
x_val = []
y_val = []

for i in range(1, files+1):
    f = open(str(i), "r")
    contents = f.read().splitlines()
    x_val.append(contents[0])
    y_val.append(contents[1])

plt.plot(x_val, y_val)
plt.xlabel('throughput (ops/sec)')
plt.ylabel('latency (ms)')
plt.savefig('tl.png')

# how to plot multiple lines?

