import json
with open('scams.json') as json_file:
    data = json.load(json_file)
    key=[]
    for p in data["result"]:
        key.append(p)
    for k in key:
        # print(data["result"][k]["id"], end='')
        print(data["result"][k]["category"], end='')
        addr=data["result"][k]["addresses"]
        for a in addr:
            print (",", a, end='')
        print("")
