from multiprocessing import Process
import datetime,time
from fastFET.BGPMAGNET.base import urlGetter
import requests
from fastFET.BGPMAGNET.params import FTP, HTTPS, MAX_WAIT_TIME, RIB_FILE_TIEMOUT, UPDATES_FILE_TIMEOUT
import urllib3
import urllib3.exceptions
import time,random,os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MINUTE=60

class downloadProcess(Process):
    def __init__(self,s:list,destination,worker_num,save_by_collector=0):
        super(downloadProcess,self).__init__()
        self.s=s
        self.destination=destination
        try:
            os.stat(destination)
        except:
            os.mkdir(destination)
        else:
            pass
        self.flag=save_by_collector
        self.waittime=random.randint(1,worker_num)
        self.ErrorList=[]
    
    def set_file_name(self,url:str):
        data=url.replace("//","/").split('/')
        destination=""
        collector=data[2]
        name=str(collector)+str("_")+str(data[-1])  # TODO JamesRay 注释掉：name=str(data[-1])
        
        if self.flag==0:
            destination=self.destination+'/'+name
        else:
            destination=self.destination+'/'+collector+'/'+name
            try:
                os.stat(self.destination+'/'+collector)
            except:
                os.mkdir(self.destination+'/'+collector)
            else:
                pass
        return name,destination
    
    def run(self):
        for url in self.s:
            self.downloadByHttp(url)
        time.sleep(MINUTE*1)
        self.HandleErrorList()
        self.PrintErrorInfo()
        return
    
    def set_time_out(self,url:str):
        timeout=UPDATES_FILE_TIMEOUT
        if "bview" in url or "rib" in url:
            timeout=RIB_FILE_TIEMOUT
        return timeout
    
    def downloadByHttp(self,url):
        name,destination=self.set_file_name(url)
        timeout=self.set_time_out(url)
        try:
            r=requests.get(url,allow_redirects=True,verify=False,timeout=timeout)
            open(destination, 'wb').write(r.content)
            print("done: "+name)
        except Exception as e:
            self.ErrorList.append((name,destination,timeout,url))
    
    def HandleErrorList(self):
        i=0
        while i<len(self.ErrorList):
            data=self.ErrorList[i]
            name=data[0]
            destination=data[1]
            timeout=data[2]
            url=data[3]
            try:
                r=requests.get(url,allow_redirects=True,verify=False,timeout=timeout)
                open(destination, 'wb').write(r.content)
                print("redone: "+name)
                self.ErrorList.remove(data)
                i-=1
            except Exception as e:
                print(e)
            i+=1
    
    def PrintErrorInfo(self):
        f=open(self.destination+"/errorInfo.txt","w")
        for url in self.ErrorList:
            f.writelines("%s|%s|%s|%s\n"%(str(url[0]),str(url[1]),str(url[2]),str(url[3])))

    



