import matplotlib.pyplot as plt
import matplotlib
from matplotlib.font_manager import findfont, FontProperties
import sys
import math
import copy 

op = int((sys.argv[1]))

def getNumber(s):
    output = ""
    for i in range(len(s)):
        if s[i].isdigit():
            output += s[i]
    return output

def get_PB_Gossip(s):
    if "Gossip" in s or "PinnedRoundRobin" in s:
        return "Gossip"
    else:
        return "Primary-Backup"
    
def isPinned(s):
    if "RoundRobin" in s: 
        return True 
    else:
        False 

def get_write_percentage(s):
    i = 0
    j = 0
    w = "workload"
    for i in range(len(s)):
        if j == len(w): 
            return getNumber(s[i:])
        if s[i] == w[j]:
            j += 1
        else:
            j = 0

        i += 1
    return -1

def getDataFromFile(file):
    data = {}
    for session in range(0, 6):
        x = []
        y = []
        for i in range(1, 10):
            try:
              with open(file + "/" + str(session) + "/" + str(i), "r") as f:
                contents = f.read().splitlines()
                throughput = getNumber(contents[0])
                latency = getNumber(contents[1])
                # if int(latency) > 450:
                #     continue
                if throughput.isnumeric():
                    x.append(int(throughput)/1000)
                if latency.isnumeric():
                    y.append(int(latency))
        
                if session == 0:
                    data["eventual"] = [x[:], y[:]]
                if session == 1:
                    data["writes follow reads"] = [x[:], y[:]]
                if session == 2:
                    data["monotonic writes"] = [x[:], y[:]]
                if session == 3:
                    data["monotonic reads"] = [x[:], y[:]]
                if session == 4:
                    data["read my writes"] = [x[:], y[:]]
                if session == 5:
                    data["causal"] = [x[:], y[:]]
            except IOError as e:
                continue
    return data

def plotGraph(p, file):
    data = getDataFromFile(file)
    # p.xlabel('Throughput (kops/sec)')
    # p.ylabel('Latency (us)')

    # p.set_ylabel('Latency (us)', fontsize = 8.0)
    # p.set_xlabel('Throughput (kops/sec)', fontsize = 8.0)

    # p.set(xlabel='Throughput (kops/sec)', ylabel='Latency (us)')

    order = ["eventual","monotonic reads", "read my writes", "monotonic writes", "writes follow reads", "causal"]
    for o in order:
        for session in data:
            x,y = data[session]
            if session == o and session == "eventual":
                p.plot(x, y, marker='o', color='b', label = "EC")
            if session == o and session == "writes follow reads":
                p.plot(x, y, marker='*', color='r', label = "WFR")
            if session == o and session == "monotonic writes":
                p.plot(x, y, marker='h', color='g', label = "MW")
            if session == o and session == "read my writes":
                p.plot(x, y, marker='.', color='y', label = "RMW")
            if session == o and session == "monotonic reads":
                p.plot(x, y, marker='2', color='c', label = "MR")
            if session == o and session == "causal":
                p.plot(x, y, marker='x', color='m', label = "C")
    legend = p.legend(prop={'size': 3.5}, markerscale=0.50, handletextpad=0.5, loc='upper left', ncol=2)
                    #   ncol=2) 
    legend.get_frame().set_alpha(0)


    # p.tight_layout()

    # p.title(get_PB_Gossip(file1))
    p.set(xlim=(0, 278), ylim=(0, 518))
    p.set_title(get_PB_Gossip(file), fontsize = 8.0)

if op == 0:
    data = {}
    file = (sys.argv[2])

    for session in range(0, 6):
        x = []
        y = []
        for i in range(1, 100):
            try:
              with open(file + "/" + str(session) + "/" + str(i), "r") as f:
                contents = f.read().splitlines()
                throughput = getNumber(contents[0])
                latency = getNumber(contents[1])
                if int(latency) > 338:
                    break
                if throughput.isnumeric():
                    x.append(int(throughput)/1000)
                if latency.isnumeric():
                    y.append(int(latency))
        
                if session == 0:
                    data["eventual"] = [x[:], y[:]]
                if session == 1:
                    data["writes follow reads"] = [x[:], y[:]]
                if session == 2:
                    data["monotonic writes"] = [x[:], y[:]]
                if session == 3:
                    data["monotonic reads"] = [x[:], y[:]]
                if session == 4:
                    data["read my writes"] = [x[:], y[:]]
                if session == 5:
                    data["causal"] = [x[:], y[:]]
            except IOError as e:
                break

    plt.figure()
    # plt.figure(figsize=(3.5, 1.3), dpi=300)
    plt.xlabel('Throughput (kops/sec)')
    plt.ylabel('Latency (us)')

    order = ["eventual","monotonic reads", "read my writes", "monotonic writes", "writes follow reads", "causal"]
    for o in order:
        for session in data:
            x,y = data[session]
            if session == o and session == "eventual":
                plt.plot(x, y, marker='o', color='b', label = "EC")
            if session == o and session == "writes follow reads":
                plt.plot(x, y, marker='*', color='r', label = "WFR")
            if session == o and session == "monotonic writes":
                plt.plot(x, y, marker='h', color='g', label = "MW")
            if session == o and session == "read my writes":
                plt.plot(x, y, marker='.', color='y', label = "RMW")
            if session == o and session == "monotonic reads":
                plt.plot(x, y, marker='2', color='o', label = "MR")
            if session == o and session == "causal":
                plt.plot(x, y, marker='x', color='m', label = "C")

    plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    plt.savefig(file + 'tl.png', bbox_inches='tight')
else: 
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rc('font', size=3.8)
    plt.xticks(fontsize = 10)
    plt.yticks(fontsize = 10)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(3.3, 1.3),
                         tight_layout=True)
    # f = findfont(FontProperties(family=['Helvetica']))
    # font = {'family' : f,
    #     'weight' : 'bold',
    #     'size'   : 22}
    matplotlib.rc('font', family='sans-serif') 
    matplotlib.rc('font', serif='Helvetica Neue') 
    matplotlib.rc('text', usetex='false') 
    # matplotlib.rc('font', **font)

    file1 = (sys.argv[2])
    file2 = (sys.argv[3])
    directory = (sys.argv[4])
    fig.supxlabel('Throughput (kops/sec)', fontsize = 8.0)
    fig.supylabel('Latency (us)', fontsize = 8.0)

    # layout="constrained", figsize=(3.3, 1.3), dpi=500)

    # plt.figure(figsize=(9, 1.3), dpi=3000)

    # plt.tight_layout()
    plotGraph(ax1, file1)
    plotGraph(ax2, file2)
    # plt.tight_layout() # pad=0.4, w_pad=0.5, h_pad=1.0)

    file_name = get_write_percentage(file1)
    plt.savefig(directory + "/" + str(file_name) + get_PB_Gossip(file1) + "write" + '.pdf', format='pdf', dpi=1200)
