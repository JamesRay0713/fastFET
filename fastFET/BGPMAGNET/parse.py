import sys
from fastFET.BGPMAGNET.bgpparser import parse
import os,time
from multiprocessing import Process

def parse_handle(path_to_file,path_to_write,num=32): 
    parse.parse_multiprocessing(path_to_file,path_to_write,num)

# rrc20_bview.20211028.0800.gz
def parseall(foldername):
    writefoldname=foldername+'_Parsed/'
    try:
        os.stat(writefoldname)
    except:
        os.mkdir(writefoldname)
    else:
        pass
    downloaded_data=os.listdir(foldername)
    pd=os.listdir(writefoldname)
    parsed_data=[]
    for p in pd:
        parsed_data.append(p.rsplit(".",1)[0])
    print(parsed_data)
    for f in downloaded_data:
        num=16
        if f in parsed_data:
            continue
        print(f'{f} start to be parsed...')
        name=foldername+'/'+f
        path_to_write=writefoldname+'/'+f+'.txt'
        if "bview" in f:
            num=96
            parse_handle(name,path_to_write,21)
        else:
            num=8
            parse_handle(name,path_to_write,num)
            # os.system("bgpreader -d singlefile -o rib-file=%s > %s"%(name,path_to_write))
            
       

if __name__=='__main__':
    st=time.time()
    parseall("./test/rrc00")
    print("-----------")
    print(time.time()-st)
    