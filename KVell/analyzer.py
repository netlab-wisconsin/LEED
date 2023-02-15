import re

f = open("output256")
tmp = f.readlines()
res = {}
for i in range(0, len(tmp)):
    line = tmp[i].strip()
    if "run_workload" in line and " YCSB " in line:
        workload = re.search( r'YCSB [A|B|C|D|F|G]', line, re.M|re.I).group()
        thr = re.search( r'[0-9]+ req/s', line, re.M|re.I).group()[:-6]
        avg = re.search( r'[0-9]+ us',  tmp[i + 2].strip(), re.M|re.I).group()[:-3]
        tail = re.search( r'[0-9]+ us',  tmp[i + 3].strip(), re.M|re.I).group()[:-3]
        print(workload, thr, avg, tail)
        if workload not in res:
            res[workload] = []
        res[workload].append(','.join([thr, avg, tail]))
f.close()
f = open("result.csv", "w")
for i in res:
    for j in res[i]:
        f.write(i + "," + j + "\n")
f.close()


