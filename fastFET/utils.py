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
    if nodeList[0]=="":     # 如果是从根开始
        folderPath += os.sep
        nodeList.pop(0)
    for node in nodeList[:-1]:
        folderPath += node + os.sep
        if(not os.path.isdir(folderPath)):
            os.mkdir(folderPath)
    if nodeList[-1] != '':
        path= folderPath + nodeList[-1]
        # 此时path代表一个文件，如果原文件存在，则要清空里面的内容
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
    if interval== 480 or interval== 120:        # debug：对rib表时间的标准化时，记得分钟处只能是00
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
    '''裁剪文件名列表至指定的起止时间范围内。
    - 参数：files一定不空
    - 方法：提取文件名列表中所有的日期字符子串。等于未下载/未下载完全
    - Note: files仅含upd / rib 中一种，且不为空'''
    
    try:
        files= sorted(files)
        mmnts_in_files= [ re.search('\d{8}.\d{4}', file).group() for file in files ]
        satIdx= mmnts_in_files.index(satTime.strftime( '%Y%m%d.%H%M' ))
        endIdx= mmnts_in_files.index(endTime.strftime( '%Y%m%d.%H%M' ))
        cuted= files[ satIdx:endIdx+1 ]
    except:
        # 在文件名列表中找不到起止索引的原因：文件真的没有下载/解析完全；文件名中的时间子串的时间间隔格式不是5的倍数
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
    # 求交集，needlist需全属于hadlist
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
    '''用于在图特征的逐分钟计算中，选择并行的核数
    - 粗略方案：每个进程分配4G，总共只用0.5-0.75的系统内存。'''
    sys_cores= multiprocessing.cpu_count()
    sys_memry= psutil.virtual_memory().total/1024**3
    processes= math.ceil(sys_memry/8)
    return processes

def computMem(var):
    '''- 计算变量的内存（Mb）'''
    
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

    # debug: 放弃使用Pool，因为它不能在子进程中创建子进程（由于daemon的存在），因此自定义了一个多进程类。
    '''pool= multiprocessing.Pool(processes= nb_process)
    pool.map_async(func, zip( upd_evt_list, rib_evt_list))    # TODO:尝试map_异步等函数
    pool.close()
    pool.join()''' 
    # debug2: 放弃自定义进程池，由于内存压力，决定逐事件采集。
    if isParallel:
        processingQueue = ProcessingQueue(nbProcess=nbProcess)  # 开一个进程池
        for j in jobs:
            processingQueue.addProcess( func, args= j )         # 任务入队
        processingQueue.run(logger)                             # 执行子进程们的主函数。这里run内步骤的最后包含了close和join。'''
    
    else:
        for args in jobs:
            func( *args )


def csv2df(paths: Union[list, str], headers: list= raw_fields, not_priming= True, space=6 ):   # space8()
    '''合并paths为大文件；读取数据到pl.dataframe。
    - a step with merging to a big file (need to <5G)
        - 耗时：rib表(9G, 5800万行)- 15~50s;
    - `args(headers)`: 字段名列表
    - `args(paths)`: 
    - return: pl.DF'''
    if isinstance(paths, str):
        paths= [paths] 
    merged= ''
    str_map= {True: 'upds', False: 'ribs' }
    isUpds= bool(len(paths)-1)
    
    if len(paths) != 1:     # 非rib
        paths= [ p for p in paths if p!= None]
        # 先增加一个文件，内容为列名。防止pl读到首行是撤销
        s= '|'.join( headers )+ '|'
        out= os.path.dirname(paths[0])+ '/head.txt'
        os.system('echo \''+ s+ '\' > '+ out)
        paths_str= out+ ' '+ ' '.join(paths)
        # 后cat合并所有文件
        merged= os.path.dirname(paths[0])+ '/merged.txt'
        t1= time.time()
        os.system('cat '+ paths_str+ ' > '+ merged)
        logger.info(' '*8+ str_map[not_priming]+ '---> merge upd files cost: %3.3fs; size: %.3fMb' % (time.time()-t1, os.path.getsize(merged)/1024**2 ) )
        
    # 后读取
    t2= time.time()
    file_map= {True: merged, False: paths[0] }
    
    df= pl.read_csv(file_map[ isUpds ], sep='|', has_header= isUpds , ignore_errors= True)
    if len(paths)==1:
        df.columns= headers   # 若果读取的是rib表，则要手动添加表头字段。
    logger.info(' '*8+ str_map[ (isUpds & not_priming)] +'---> read  csv files cost: %3.3fs; mem: %5.2fMb; shape: %s' % (time.time()-t2, df.estimated_size()/1024**2, str(df.shape) ) )

    # 最后删除merge.txt、head.txt
    if len(paths) != 1:
        os.system( 'rm -f '+ merged+ ' '+ out )
    
    return df

def labelMaker(save_path: str, sat_end_list: list, all_to_normal= False):
    ''' - path: 一事件中已采集好的特征DF的路径
        - sat_end_list: list['start, end', '', ...], 保证了有多处label时候也不出错
        - all_to_normal: 当为True, 即把所有样本的label初始化为无异常
    '''
        # 从文件读取df
    df= pl.read_csv( save_path )
    if all_to_normal or 'label' not in df.columns:
        series_= pl.Series('label', ['normal']* df.shape[0])
        df['label']= series_

        # 获取当前df的event的类型event_type
    event_name= save_path.split('__')[1]
    for t in ['hijack', 'leak', 'outage']:
        if t in event_name:
            event_type= t
            break
        # 改df的date列为datetime格式
    date= df['date']
    df['date']= [ dt.datetime.strptime(s, "%Y/%m/%d %H:%M") for s in date]

    #逐一对多处片段生成新label
    for sat_end in sat_end_list:
            # 改起止时刻为datetime格式
        sat_end= sat_end.split(',')
        start= dt.datetime.strptime(sat_end[0].strip()[:-3], "%Y/%m/%d %H:%M")
        end = dt.datetime.strptime(sat_end[1].strip()[:-3], "%Y/%m/%d %H:%M")
            # 得到标签起止行号
        sat_idx= df.filter(pl.col('date')== start)['time_bin'].to_list()[0]
        end_idx= df.filter(pl.col('date')== end)['time_bin'].to_list()[0]
        
            # 遍历df，生成当前的小段label
        for i in range( sat_idx, end_idx+1):
            df[i, 'label']= event_type

        # date列格式改回str
    date= df['date']
    df['date']= [ s.strftime("%Y/%m/%d %H:%M") for s in date]
    
    df.sort('time_bin').to_csv( save_path )

def splitChunk(paths:list, need_rib):
    '''为了加快polars读取过程，当文件集合需要按照整体size大小分块
    - return: list[list]'''
    sys_cores= multiprocessing.cpu_count()
    sys_memry= psutil.virtual_memory().total/1024**3
    sizeG, chunksize= 0, 0
    res= []
    
    # 拿到paths的总sizeG
    for f in paths:
        sizeG+= os.path.getsize(f)
    sizeG= sizeG/1024**3

    # 当无图特征时，chunk的大小指定为 2G。。最终决定，无论需要图特征与否，都用2G
    #if not need_rib:
        # 经测试，在8核18G内存的笔记本并行获取38个volume特征条件下，chunk<= 2G时处理速度 >47万条/s; =2.3G时速度为 36万条/s。
        #         在128核128G内存的服务器并行获取38个volume特征条件下，chunk在0~5G时速度都在35~40万条/s的区间，而VIRT越来越高
        # 因此，不管系统内存多少，统一把chunk设为2G
    chunksize= 2 if sys_memry>=8 else sys_memry/4   # set chunksize here，单位为Gb
    chunk= math.ceil( sizeG/chunksize )
    chunk_files= math.ceil( len(paths)/chunk )
    for i in range(chunk-1):
        res.append( paths[i*chunk_files: (i+1)*chunk_files])
    res.append( paths[(chunk-1)* chunk_files:] )
    
    logger.info(f'    split updates files: system info: cpus({sys_cores}); memory({sys_memry:.3f} Gb)')
    logger.info(f'                         files total size: {sizeG:.3f} Gb; max limit per chunk {chunksize:.3f} Gb') 

    return res

def exprDict( featNm_pfx:str):
    '''对目标列featNm输出三个polars表达式：计数、出现的均值、出现的最大值。不用输出`求和`，因为‘和’就是条目总数的统计。'''
    dic= {
        featNm_pfx+ "_cnt": pl.col(featNm_pfx).count().suffix("_cnt"),
        featNm_pfx+ "_avg": pl.col(featNm_pfx).mean().suffix("_avg"),  # debug: 无法使用.cast(pl.Float32)   在有的情况不能正常地f64转f32，原因未知。
        featNm_pfx+ "_max": pl.col(featNm_pfx).max().suffix("_max")
    }
    return dic

def featsGrouping(dictTree:dict, feats:list):
    '''实现对目标特征集合中的特征分组，并拿到每组的节点路径。
    - arg: dictTree:一个完整的字典树（叶为Expr，父为所有的featName，父以上为特征类别）, 
    - arg: feats:目标特征集合。
    - return: dict[key: 路径元组；val: 一个路径下的目标特征子集list ] '''
    # 先找到每个feat在特征树中的父节点路径。
    feats_paths= {} # key特征名；val路径元组
    for feat in feats:
        curpath= jsonpath.normalize(jsonpath.jsonpath(dictTree, '$..'+feat, result_type='PATH')[0])
        curNodes=tuple( curpath.split(';')[1:])
        feats_paths[ curNodes[-1]]= curNodes[:-1]
    paths_feats= {} 
    # 后交换key-val对，同时合并相同key(即路径元组)的val到一个list中（即特征分组了）
    for feat, path in feats_paths.items():
        if path not in paths_feats.keys():
            paths_feats[ path ]= [feat]
        else:
            paths_feats[ path ].append( feat )
    return paths_feats

def feat2expr( featTree, feats ):
    '''只是简化特征树，从featTree得到{特征名: expr}的单层字典'''
    feats_exprs= {} # key:特征名；val:expr
    for feat in feats:
        curexpr= jsonpath.jsonpath(featTree, '$..'+feat, result_type='VALUE')[0]
        feats_exprs[feat]= curexpr
    return feats_exprs

##############
# graph feats#
##############

def df2edge( df: pl.DataFrame ):
    '''从一个原始df中分析path字段，得到所有的边，以此构建全局AS拓扑图
    - 拓展：需不需在建图时给每个节点、边增加一些属性（如BML框架有属性‘IP数量’）。暂时不需要我认为。只需拿到边的集合即可
    - 处理path字段的方法：split成list，对于有 prePending的，不能用unique()，这会打乱AS顺序。
    - arg: df: 是原df，不是FET.chunkHandler中被preProcess()预处理后的df
    - return: list(tuple(ASNum, ASNum)) '''
    res= ( df.lazy()
        .filter(
            (pl.col("msg_type") != 'STATE') &   # TODO: 具体看看过滤啥。评判下是否peer-pfx具有唯一性。
            (~pl.col('path').str.contains('\{'))
            )
        #.groupby(['peer_AS', 'dest_pref']).tail(1)   # 注：df是从rib表来的，peer-pfx具有唯一性，因此不需分组取末行。
        .select( [
            pl.col('path').str.split(" ").alias('path_list')
            ])
        .with_row_count('index')       # 用于给rib_df的每个条目编号
        .explode('path_list')
        .groupby('index').agg([
            pl.col( 'path_list'),
            pl.col( 'path_list').shift(-1).alias('path_list_shift')     # 如此一来，同一行的这两列的值相结合的tuple，就得到一条边了，避免了for循环。
        ])
        .filter( ( pl.col( 'path_list_shift' ) != None) &
                 ( pl.col( 'path_list')== pl.col( 'path_list_shift') ) ) # 过滤掉prePending造成的`同点边`，和最右一个AS形成的`单点边`
        .select( pl.exclude( 'index' ))
    ).collect().rows()
    return res 


##############
# Decorator  #
##############

def timer(func):
    '''- in wrap, args[-1] is space, args[-2]  '''
    @wraps(func)    # 有效防止在多进程时候， 装饰器不能序列化
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
    '''- 获取锚点所在函数的名字、函数位于的进程(内存使用)、父进程(内存使用)'''
    s= " "*space+ "<mem usage> func: `%20s`, cur_pid: %d (%s), ppid: %d (%s)" % (
            func_name,
            os.getpid(),
            curMem(),
            os.getppid(),
            curMem()
    )
    return s

def curMem(is_ppid= False, makeTP= False):
    '''- 调用了该函数的进程在当前时刻的内存使用情况(Mb)'''
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
    