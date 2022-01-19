import paramiko
import time

# Get All dbsize in every pi 
def getSize():
    res = 0
    for i in range(85, 95):
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
        if "M" in tmp:
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

# Start manager
# Start All backends
# Start frontend
def backendStart():
    print("Kill manager")
    cmd = "ps -ef | grep \"manager 10.134.11.96\" | grep -v grep | awk \'{print $2}\' | xargs kill -9"
    ssh = paramiko.SSHClient()
    #把要连接的机器添加到known_hosts文件中
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #连接服务器
    ssh.connect(hostname='128.105.146.38', port=22, username='huazhang', password='huazhang0115')
    ssh.exec_command(cmd)
    print("Start Manager")
    chan = ssh.invoke_shell()
    cmd = "nohup manager 10.134.11.96 > manager.log 2>&1 &\n"
    chan.send(cmd)
    time.sleep(1)
    #getSize()
    print("##########Begin all backend process!##########")
    for i in range(85, 95):
        ssh = paramiko.SSHClient()
        #把要连接的机器添加到known_hosts文件中
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #连接服务器
        ssh.connect(hostname='10.134.11.' + str(i), port=22, username='pi', password='fawntest')
        print(str(i) + ":", end='')
        #cmd = "IP=$(ifconfig | grep -A1 \"eth0\" | grep \'inet\' |awk -F \' \' \'{print $2}\'|awk \'{print $1}\')  && rm -f nohup.out  && sudo nohup backend -m 10.134.11.95 -i $IP -b \"/test/fawndb_\" -o &"
        chan = ssh.invoke_shell()
        cmd = "cd ~ && rm -f nohup.out && sudo nohup backend -m 10.134.11.96 -i " + "10.134.11." + str(i) + " -b \"/test/fawndb_\" -o &\n"
        
        chan.send(cmd)
        time.sleep(1)
        ssh.close()
    print("##########Begin all backend process success!##########")
    getSize()
    print("Kill frontend")
    cmd = "ps -ef | grep \"frontend\" | grep -v grep | awk \'{print $2}\' | xargs kill -9"
    ssh = paramiko.SSHClient()
    #把要连接的机器添加到known_hosts文件中
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #连接服务器
    ssh.connect(hostname='128.105.146.38', port=22, username='huazhang', password='huazhang0115')
    ssh.exec_command(cmd)
    print("Start frontend")
    chan = ssh.invoke_shell()
    cmd = "nohup frontend -m 10.134.11.96 -p 7001 -l 4001 -c 0 10.134.11.96 > froentend.log 2>&1 &\n"
    chan.send(cmd)
    time.sleep(1)
    ssh.close()
    
kill()
backendStart()
while True:
    print("Now db is:")
    getSize()
    time.sleep(20)
