def getResult():
    from openpyxl import Workbook
    f = open(r"nohup.out")
    target = 'g'
    output = []
    for i in f.readlines():
        if f"fawn	workloads/workload{target}.spec" in i:
            if "us" in i.split()[-1]:
                output.append(i.split()[-1][:-2])
            else:
                output.append(i.split()[-1])

    wb = Workbook()
    ws = wb.active
    row = 1
    col = 1
    for i in range(0, len(output)):
        ws.cell(row, col).value = output[i]
        if (i + 1) % 3 == 0:
            row += 1
        if col == 3:
            col = 0
        col += 1
    wb.save("2.xlsx")

def testShellGenerator():
    for i in ['c', 'b', 'd', 'a', 'f', 'g']:
        for j in [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]:
            print(f"echo '--- TEST START thread: {j} workload {i}---'")
            print(
                f"./tester_remote_ycsbc -threads {j} -db fawn -P workloads/workload{i}.spec -feip 10.134.11.96 -cip 10.134.11.96 -feport 4001 -load 0")

            print(f"echo '--- TEST END thread: {j} workload {i}---'")


