# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian


workload=com.yahoo.ycsb.workloads.CoreWorkload
fieldcount=1
readallfields=true
writeallfields=true

readproportion=1
updateproportion=0
scanproportion=0
insertproportion=0

requestdistribution=zipfian

recordcount=20000000
operationcount=8000000
fieldlength=1024


