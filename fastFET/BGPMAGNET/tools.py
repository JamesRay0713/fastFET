
from datetime import datetime

import requests


def get_year_month(datetime):
    return datetime.year*100+datetime.month

def get_year_month_day(datetime):
    return datetime.year*10000+datetime.month*100+datetime.day

def check_error_list(fname):
    ErrorList=[]
    with open(fname,"r") as f:
        for line in f:
            data=line.strip().split("|")
            name=data[0]
            destination=data[1]
            timeout=int(data[2])
            url=data[3]
            try:
                r=requests.get(url,allow_redirects=True,verify=False,timeout=timeout)
                open(destination, 'wb').write(r.content)
                print("redone: "+name)
            
            except Exception as e:
                ErrorList.append(line)
    with open(fname,"w+") as f:
        for url in ErrorList:
            f.writelines("%s|%s|%s|%s\n"%(str(url[0]),str(url[1]),str(url[2]),str(url[3])))
    
