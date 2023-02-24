from fastFET.BGPMAGNET.ProcessHandler import downloadProcess
from multiprocessing import JoinableQueue
from fastFET.BGPMAGNET.base import  base_params, bgpGetter, urlGetter
from fastFET.BGPMAGNET.params import BGP_DATATYPE

class downloadByParams:
    def __init__(self,urlgetter:urlGetter,destination,save_by_collector=0) -> None:
        self.urlgetter=urlgetter
        self.destination=destination
        self.save_by_collector=save_by_collector
    
    def start_on(self, is_custom_urllist= False, urls= None):
        ''' - arg(is_custom_urllist): false, 默认，根据时间区间参数构建要下载的url列表; True, 从参数urls传入url列表'''
        if is_custom_urllist:
            urllist= urls
        else:
            urllist=self.urlgetter.getURL()
        print(len(urllist))
        divider=max(len(urllist)//20,1)
        downloadprocess=[]
        split=[urllist[i:i + divider] for i in range(0, len(urllist), divider)]
        for s in split:
            downloadprocess.append(downloadProcess(s,self.destination,len(split),self.save_by_collector))

        for i in range(len(split)):
            downloadprocess[i].start()
        for i in range(len(split)):
            downloadprocess[i].join()
        


  