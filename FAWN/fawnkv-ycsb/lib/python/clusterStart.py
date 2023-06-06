import paramiko
import time

# if overwrite db
OVERWRITE = 0

# setup the server numberfrom BEGIN to END
BEGIN = 85
END = 94

# Monitor All dbsize in every pi 
def getSize():
    res = 0
    for i in range(BEGIN, END + 1):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname='10.134.11.' + str(i), port=22, username='pi', password='fawntest')
        print(str(i) + ":", end='')
        cmd = 'du -h /test'
        stdin, stdout, stderr = ssh.exec_command(cmd)
        
        result = stdout.read()
        if not result:
            result = stderr.read()
        ssh.close()
        
        tmp = result.decode().split()[0]
        print(tmp)
        if "K" in tmp:
             tmp = float(tmp[:-1])/1024/1024
        elif "M" in tmp:
            tmp = float(tmp[:-1])/1024
        else:
            tmp = float(tmp[:-1])
        
        res += tmp
    print("The total size is", res, "G")

# Kill All backend process
def kill():
    print("##########Kill all backend process!##########")
    for i in range(85, 95):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname='10.134.11.' + str(i), port=22, username='pi', password='fawntest')
        print(str(i) + ":", end='')
        cmd = 'sudo pkill -9 backend'
        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read()

        if not result:
            result = stderr.read()
        print(result.decode())
        ssh.close()
    print("##########Kill all backend process success!##########")


# Start All backends
def backendStart():
    print("##########Begin all backend process!##########")
    for i in range(BEGIN, END + 1):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname='10.134.11.' + str(i), port=22, username='pi', password='fawntest')
        chan = ssh.invoke_shell()
        if OVERWRITE == 1:
            cmd = "cd ~ && rm -f nohup.out && sudo nohup backend -m 10.134.11.96 -i " + "10.134.11." + str(i) + " -b \"/test/fawndb_\" -j -o&\n"  
        else:
            cmd = "cd ~ && rm -f nohup.out && sudo nohup backend -m 10.134.11.96 -i " + "10.134.11." + str(i) + " -b \"/test/fawndb_\" -j&\n"  
        chan.send(cmd)
        time.sleep(1)
        ssh.close()
    print("##########Begin all backend process success!##########")

if __name__ == "__main__": 
    kill()
    print("Please start the manager(\"nohup manager 10.134.11.96 -r 3 -j > manager.log 2>&1 &\"), y to continue")
    while True:
        a = input()
        if a == 'y':
            break
    backendStart()
    print("Please start the frontend(\"nohup frontend -m 10.134.11.96 -p 7001 -l 4001 -c 0 10.134.11.96 > froentend.log 2>&1 &\"), y to continue")
    while True:
        a = input()
        if a == 'y':
            break
    while True:
        print("Now db is:")
        getSize()
        time.sleep(20)