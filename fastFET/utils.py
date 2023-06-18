import sys,os,psutil
import pandas as pd
import polars as pl
import datetime as dt
from datetime import datetime
import multiprocessing
import json, re, math
from collections import OrderedDict, defaultdict
import time
from functools import wraps
import jsonpath
import logging
import logging.config

from typing import Union

from fastFET.MultiProcess import ProcessingQueue

#########
# deal with files & file names#
#########

def makePath(path: str):
    '''given any path, if the intermediate nodes are not exist, make the dir.
    if basefile existed, clear the content.
    - return: arg: path'''
    folderPath = ""
    nodeList= path.split(os.sep)
    if nodeList[0]=="":     
        folderPath += os.sep
        nodeList.pop(0)
    for node in nodeList[:-1]:
        folderPath += node + os.sep
        if(not os.path.isdir(folderPath)):
            os.mkdir(folderPath)
    if nodeList[-1] != '':
        path= folderPath + nodeList[-1]
        
        open(path, 'w').close()
    return(path)

def intervalMin(type, prjtNm):
    '''- type: `'updates'`(rrc间隔5分钟, route-views间隔15分钟) or `'ribs'`(rrc间隔8小时；route-views间隔2小时)
    - prjtNm: `'rrc'` or `'rou'`. 
    - return: int(min) '''
    strmap= {'updates': {'rrc': 5, 'rou': 15},
             'ribs'   : {'rrc': 8*60, 'rou': 2*60}}
    return strmap[type][prjtNm[:3]]

def normSatEndTime(interval, satTime: datetime, endTime: datetime=None):
    '''normalize the time to fit file names.
    '''
    if endTime==None:
        endTime= satTime
    if interval== 5 or interval== 15:
        while 1:
            if satTime.minute % interval == 0:
                break
            satTime -= dt.timedelta(seconds= 60)
        while 1:
            if endTime.minute % interval == 0:
                break
            endTime += dt.timedelta(seconds= 60)
    if interval== 480 or interval== 120:        
        while 1:
            if satTime.hour % (interval/60) == 0:
                break
            satTime -= dt.timedelta(seconds= 3600)
        satTime -= dt.timedelta(seconds= satTime.minute*60)
        while 1:
            if endTime.hour % (interval/60) == 0:
                break
            endTime += dt.timedelta(seconds= 3600)
        endTime -= dt.timedelta(seconds= (60- endTime.minute)*60)
    return (satTime, endTime)

def cut_files_list(files, satTime, endTime):
    '''裁剪文件名列表至指定的起止时间范围内。'''    
    try:
        files= sorted(files)
        mmnts_in_files= [ re.search('\d{8}.\d{4}', file).group() for file in files ]
        satIdx= mmnts_in_files.index(satTime.strftime( '%Y%m%d.%H%M' ))
        endIdx= mmnts_in_files.index(endTime.strftime( '%Y%m%d.%H%M' ))
        cuted= files[ satIdx:endIdx+1 ]
    except:
        msg= f'can`t cut files like `{files[0]}`, you should check if existing incomplete files, or if having a different time format in file names.'
        logger.error(msg)
        raise RuntimeError(msg)
    return cuted

def allIn(interval, realFiles, satTime: datetime, endTime: datetime):
    '''- find time points which we `need`; 
    - compared with time points which we already `had`. 
    '''
    # get need-list
    need=[]
    while satTime.__le__( endTime ):
        need.append( satTime.strftime( '%Y%m%d.%H%M' ))
        satTime += dt.timedelta(seconds= interval* 60)
    # get had-list
    had= []
    try:
        had= [ re.search('\d{8}.\d{4}', file).group() for file in realFiles ]
    except:
        logger.info(f'has no parsed files between {satTime} - {endTime}')    
    # intersection
    a= set(need).difference( set(had))
    b= set(had ).difference( set(need))
    if len(set(need) & set(had)) != len(need):
        return False
    
    return True
    
class iohandler():
    def __init__(self, path):
        self.cpath= path

    def reader(self):
        ''' return dict of data files name like `{ (event_name, collector): feat_data_path, ...}` '''
        dir, _, datalist= os.walk(self.cpath).__next__()
        data_fnames= [dir+ '/'+ x for x in datalist]
        name_list= [re.search('__(.*)__(.*).csv', file).groups() for file in data_fnames]
        return dict(zip(name_list, data_fnames))

    def writer(self, df_res: pd.DataFrame, oldpath: str):
        wpath= self.cpath+ '/data_filtered'
        if not os.path.exists(wpath):
            os.mkdir(wpath)
        _, wfile= os.path.split(oldpath)
        df_res.to_csv( wpath+ '/'+ wfile, index= False)


#########
# other #
#########

def dict_list():
    return defaultdict(list)

def d_d_list():
    return defaultdict( dict_list )

def paralNum():
    ''''''
    sys_cores= multiprocessing.cpu_count()
    sys_memry= psutil.virtual_memory().total/1024**3
    processes= math.ceil(sys_memry/8)
    return processes

def computMem(var):
    ''''''
    if isinstance(var, pd.DataFrame):
        res= var.memory_usage( deep=True).sum()/1024**2
    elif isinstance(var, pl.DataFrame):
        res=  var.estimated_size()/1024**2
    else:
        res=  sys.getsizeof(var)/1024**2
    res_str= '%.3f' % res
    return res_str
    

##############
# parse feats#
##############
    # 14个字段
raw_fields= ['protocol','timestamp','msg_type','peer_IP','peer_AS','dest_pref','path','origin','next_hop','local_pref','MED','community','atomicAGG','aggregator']


def runJobs( file_dict, func, nbProcess= 4 ):
    '''- main function'''
    isParallel= False
    
    logger.info(' ')
    s= '# FEATURE EXTRACT #'
    logger.info('#'* len(s))
    logger.info(s)
    logger.info('#'* len(s))

    upd_evt_list= list(file_dict['updates'].items())
    rib_evt_list= list(file_dict['ribs'].items())
    if not len(rib_evt_list):
        rib_evt_list= [None]* len(upd_evt_list)
    if not len(upd_evt_list):
        upd_evt_list= [None]* len(rib_evt_list)
    jobs= zip( upd_evt_list, rib_evt_list )

    '''pool= multiprocessing.Pool(processes= nb_process)
    pool.map_async(func, zip( upd_evt_list, rib_evt_list))
    pool.close()
    pool.join()''' 
    
    if isParallel:
        processingQueue = ProcessingQueue(nbProcess=nbProcess)  
        for j in jobs:
            processingQueue.addProcess( func, args= j )         
        processingQueue.run(logger)                             
    
    else:
        for args in jobs:
            func( *args )


def csv2df(paths: Union[list, str], headers: list= raw_fields, not_priming= True, space=6 ):   # space8()
    '''合并paths为大文件'''
    if isinstance(paths, str):
        paths= [paths] 
    merged= ''
    str_map= {True: 'upds', False: 'ribs' }
    isUpds= bool(len(paths)-1)
    
    if len(paths) != 1:     
        paths= [ p for p in paths if p!= None]
        
        s= '|'.join( headers )+ '|'
        out= os.path.dirname(paths[0])+ '/head.txt'
        os.system('echo \''+ s+ '\' > '+ out)
        paths_str= out+ ' '+ ' '.join(paths)
        
        merged= os.path.dirname(paths[0])+ '/merged.txt'
        t1= time.time()
        os.system('cat '+ paths_str+ ' > '+ merged)
        logger.info(' '*8+ str_map[not_priming]+ '---> merge upd files cost: %3.3fs; size: %.3fMb' % (time.time()-t1, os.path.getsize(merged)/1024**2 ) )
        
    t2= time.time()
    file_map= {True: merged, False: paths[0] }
    
    with open(paths[0] ) as f:
        line= f.readline()
    if ',' in line:
        df= pl.read_csv(file_map[ isUpds ], has_header=True)
    else:
        df= pl.read_csv(file_map[ isUpds ], sep='|', has_header= isUpds , ignore_errors= True)
        
        if len(paths)==1:
            df.columns= headers   
        logger.info(' '*8+ str_map[ (isUpds & not_priming)] +'---> read  csv files cost: %3.3fs; mem: %5.2fMb; shape: %s' % (time.time()-t2, df.estimated_size()/1024**2, str(df.shape) ) )

        if len(paths) != 1:
            os.system( 'rm -f '+ merged+ ' '+ out )
    
    return df

def labelMaker(save_path: str, sat_end_list: list, all_to_normal= False):
    ''' - sat_end_list: list['start, end', '', ...]
        - all_to_normal: 为True时, 把所有样本的label初始化为无异常
    '''
    df= pl.read_csv( save_path )
    if all_to_normal or 'label' not in df.columns:
        series_= pl.Series('label', ['normal']* df.shape[0])
        df['label']= series_
        
    event_name= save_path.split('__')[1]
    event_type= ''
    for t in ['hijack', 'leak', 'outage']:
        if t in event_name:
            event_type= t
            break
    if event_type=='':
        raise RuntimeError("There must be a type of anomoly('hijack', 'leak', 'outage') in event name.")

    date= df['date']
    df['date']= [ dt.datetime.strptime(s, "%Y/%m/%d %H:%M") for s in date]

    for sat_end in sat_end_list:
        sat_end= sat_end.split(',')
        start= dt.datetime.strptime(sat_end[0].strip()[:-3], "%Y/%m/%d %H:%M")
        end = dt.datetime.strptime(sat_end[1].strip()[:-3], "%Y/%m/%d %H:%M")

        sat_idx= df.filter(pl.col('date')== start)['time_bin'].to_list()[0]
        end_idx= df.filter(pl.col('date')== end)['time_bin'].to_list()[0]
        
        for i in range( sat_idx, end_idx+1):
            df[i, 'label']= event_type

    date= df['date']
    df['date']= [ s.strftime("%Y/%m/%d %H:%M") for s in date]
    
    df.sort('time_bin').to_csv( save_path )

def splitChunk(paths:list, need_rib):
    '''切分文件集合以读取'''
    if len(paths)==1:
        return [paths]
    sys_cores= multiprocessing.cpu_count()
    sys_memry= psutil.virtual_memory().total/1024**3
    sizeG, chunksize= 0, 0
    res= []
    
    for f in paths:
        sizeG+= os.path.getsize(f)
    sizeG= sizeG/1024**3

    #if not need_rib:
    chunksize= 2 if sys_memry>=8 else sys_memry/4
    chunk= math.ceil( sizeG/chunksize )
    chunk_files= math.ceil( len(paths)/chunk )
    for i in range(chunk-1):
        res.append( paths[i*chunk_files: (i+1)*chunk_files])
    res.append( paths[(chunk-1)* chunk_files:] )
    
    logger.info(f'    split updates files: system info: cpus({sys_cores}); memory({sys_memry:.3f} Gb)')
    logger.info(f'                         files total size: {sizeG:.3f} Gb; max limit per chunk {chunksize:.3f} Gb') 

    return res

def exprDict( featNm_pfx:str):
    
    dic= {
        featNm_pfx+ "_cnt": pl.col(featNm_pfx).count().suffix("_cnt"),
        featNm_pfx+ "_avg": pl.col(featNm_pfx).mean().suffix("_avg"),  
        featNm_pfx+ "_max": pl.col(featNm_pfx).max().suffix("_max")
    }
    return dic

def featsGrouping(dictTree:dict, feats:list):
    '''对目标特征集合中的特征分组
    - arg: dictTree:完整的字典树
    - arg: feats:目标特征集合
    - return: dict[key: 路径元组; val: 一个路径下的目标特征子集list ] '''
    feats_paths= {} 
    for feat in feats:
        curpath= jsonpath.normalize(jsonpath.jsonpath(dictTree, '$..'+feat, result_type='PATH')[0])
        curNodes=tuple( curpath.split(';')[1:])
        feats_paths[ curNodes[-1]]= curNodes[:-1]
    paths_feats= {} 
    
    for feat, path in feats_paths.items():
        if path not in paths_feats.keys():
            paths_feats[ path ]= [feat]
        else:
            paths_feats[ path ].append( feat )
    return paths_feats

def feat2expr( featTree, feats ):
    ''''''
    feats_exprs= {} 
    for feat in feats:
        curexpr= jsonpath.jsonpath(featTree, '$..'+feat, result_type='VALUE')[0]
        feats_exprs[feat]= curexpr
    return feats_exprs

##############
# graph feats#
##############

def df2edge( df: pl.DataFrame ):
    '''从原始df的path列获取边集合
    - return: list(tuple(ASNum, ASNum)) '''
    res= ( df.lazy()
        .filter(
            (pl.col("msg_type") != 'STATE') &   
            (~pl.col('path').str.contains('\{'))
            )
        #.groupby(['peer_AS', 'dest_pref']).tail(1)   
        .select( [
            pl.col('path').str.split(" ").alias('path_list')
            ])
        .with_row_count('index')       
        .explode('path_list')
        .groupby('index').agg([
            pl.col( 'path_list'),
            pl.col( 'path_list').shift(-1).alias('path_list_shift')     
        ])
        .filter( ( pl.col( 'path_list_shift' ) != None) &
                 ( pl.col( 'path_list')== pl.col( 'path_list_shift') ) ) 
        .select( pl.exclude( 'index' ))
    ).collect().rows()
    return res 


##############
# Decorator  #
##############

def timer(func):
    '''- in wrap, args[-1] is space, args[-2]  '''
    @wraps(func)    
    def wrap(*args, **kwargs):
        begin_time = time.perf_counter()
        begin_memo = curMem()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        end_memo = curMem()

        try:
            funcName= func.__name__ if func.__name__ != 'run_cmpxFeat_inMulproc' else args[-2]
            logger.info(' '* args[-1] + f'func= `{funcName}`; cost={(end_time - begin_time):3.2f} sec; begin&end_memo= {begin_memo}->{end_memo}; ppid={os.getppid()} ') 
        except Exception as e:
            #raise e
            logger.info(' '*6+ f'..func= `{funcName}`; cost={(end_time - begin_time):3.2f} sec; begin&end_memo= {begin_memo}->{end_memo}; ppid={os.getppid()} ') 
        return result 
    return wrap

#########
#  log  #
#########

def setup_logging(configPath= os.path.dirname(__file__)+ '/logConfig.json',default_level=logging.DEBUG):
    ''' - `configPath`  config logging
        '''
    makePath(os.getcwd()+ '/log/')
    if os.path.exists(configPath):
        with open(configPath,"r") as f:
            config = json.load(f)
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)

logger= logging.getLogger()

def logMemUsage( space:int, func_name:str ):
    ''''''
    s= " "*space+ "<mem usage> func: `%20s`, cur_pid: %d (%s), ppid: %d (%s)" % (
            func_name,
            os.getpid(),
            curMem(),
            os.getppid(),
            curMem()
    )
    return s

def curMem(is_ppid= False, makeTP= False):
    ''''''
    if is_ppid:
        curmem= psutil.Process(os.getppid()).memory_info().rss/1024**2
    else:
        curmem= psutil.Process(os.getpid() ).memory_info().rss/1024**2
    if makeTP:
        curTP= int(time.time())
        res= f"{curmem:.1f}Mb, timestamp: {curTP}"
    else:
        res= f"{curmem:.1f}Mb"
    return res


    
if __name__=='__main__':
    setup_logging()
    