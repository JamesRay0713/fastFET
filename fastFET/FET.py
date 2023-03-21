import ctypes
from operator import itemgetter
import os
import re, glob
import sys
import time
import inspect
import datetime as dt

import jsonpath, csv, json
import pandas as pd
import polars as pl
from tqdm import tqdm
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from fastFET import featTradition
from fastFET import featTree
from fastFET import utils
from fastFET.MultiProcess import ProcessingQueue
from fastFET.featTradition import *
from fastFET.featGraph import GraphBase
from fastFET.collectData import GetRawData
from fastFET.bgpToolKit import DownloadParseFiles, simple_plot

utils.setup_logging()
logger= utils.logger

class FET():
    
    __slot__= [ 'slot', 'raw_dir', 'increment', 'duration', 'need_rib', 'need_tri', 'cut_peer', 'peer'
                'raw_fields', 'featNms', 'feats_dict', 'midnode_res', 'pd_shared_topo', 
                'first_ts', 'df_AS', 'df_peer_pfx', 'path_df_MOAS',
                'preDF', 'pd_shared_preDF', 'df_res_graph']
    
    def __init__(self, 
        slot= 60, 
        raw_dir= 'Dataset/',
        increment= 4,
        duration= 2,
        cut_peer= True,
    ) -> None:
        '''args 
            - slot: 统计特征数量的时间间隔(s)
            - raw_dir: 特征采集的原始离线数据、输出数据存放的默认路径
            - increment: 定义事件起止时间的增量(h)
            - duration: 当事件缺省结束时间时，将其指定为 start_time+ duration (h)
            - cut_peer: 在有图特征的情况，引入的rib表(>200万时)要裁剪为只有一个peer，以缓解内存压力以及提高计算效率。
        '''
        
        self.slot= slot
        self.raw_dir= raw_dir
        self.increment= increment
        self.duration= duration
        self.cut_peer= cut_peer

        self.raw_fields= ['protocol','timestamp','msg_type','peer_IP','peer_AS','dest_pref','path','origin','next_hop','local_pref','MED','community','atomicAGG','aggregator']
        self.featNms= []        # 存放自定义的目标特征名
        self.feats_dict= {}     # 用于存放从特征树中得到目标特征分组、函数路径。
        self.midnode_res= {}    # 用于存放函数路径中，已经计算过的节点的结果，防止self.chunkHandler的第二步中的重复计算。key: 函数名；val: df
        self.pd_shared_topo = multiprocessing.Value(ctypes.py_object)    # 多进程共享变量。存储rib表在事件起始时刻的df(用pandas.DF)
        self.pd_shared_preDF= multiprocessing.Value(ctypes.py_object) # 多进程共享变量。存储预处理后的完整upds条目（13列）
        # 另：在每轮monitor中，都会新定义5个类变量：（详见self.initHandler函数）
        # self.first_ts
        # self.df_AS        大表1：
        # self.df_peer_pfx
        # self.ldf_MOAS
        # self.pd_shared_topo (见上)
        #utils.makePath(os.path.dirname(__file__)+'/event_list.csv')     # 

    # ##
    # 以下4个函数为FET类的辅助性函数
    # ##  
    def progBar(self, chunkDF):
        '''TODO: 进度条'''
        '''pbar = tqdm(total=len()) 
        pbar.set_description('Sleep')
        update = lambda *args: pbar.update()'''
        pass

    def getAllFeats(self):
        return featTree.getAllFeats()

    def setCustomFeats(self, FEAT ):
        '''3种方法自定义特征：
        - 全特征选取：FEAT= 'ALL'
        - 按类别选取：FEAT= [ 'volume', 'path', 'dynamic',  'editdistance', 'ratio', 'nodegraph', 'ASgraph' ]
        - 单特征选取：FEAT= [......] ,特征列表详见`self.getAllFeats()`
        '''
        all_catas= [ 'volume', 'path', 'dynamic',  'editdistance', 'ratio', 'nodegraph', 'ASgraph' ]
        all_feats= featTree.getAllFeats()
        if FEAT== "ALL":
            self.featNms= all_feats
        elif (set(FEAT) & set(all_catas)):
            self.featNms= featTree.getCateFeats( FEAT )            
        else:
            self.featNms= featTree.getDepend(FEAT)

    def recurFunc(self, node_list):
        '''chunkForTradi子函数。递归函数，用于获取特征树节点所代表函数的链式调用'''
        if len(node_list):
            if node_list[-1] in self.midnode_res.keys():
                return self.midnode_res[ node_list[-1] ].lazy()
            return globals()[ node_list[-1] ]( self.recurFunc(node_list[:-1]), self )
        else:
            return self.preDF.lazy() 
     
    @utils.timer
    def chunkForTradi(self, space):   #space8
        '''chunkHandler子函数：采集传统特征。传统特征计算用pl.collect，造多条expr链来隐式并行计算。
        - return: 所有传统特征在每个slot上的统计值。'''
        # 1----1、并行计算所有传统特征
        ldf_list= []    # 元素为lazyGroupbyDataFrame
        for path, subFeats in self.feats_dict.items():
            if 'graph' not in path and 'ratio' not in path:     # 排除掉非传统特征
                cur_expr_chain=self.recurFunc(path )    # 都是`groupby('time_bin')`结尾
                
                # 拿到当前小块subFeats对应的expr。itemgetter能从字典中同时访问多个key
                path_end_little_dict= jsonpath.jsonpath(featTree.featTree, '$.'+ '.'.join( list(path) ))[0]
                exprs= itemgetter(*subFeats)( path_end_little_dict )  
                    # debug: 要保证传入agg()的exprs是list
                if isinstance(exprs, tuple):    # 当从字典拿到多个key的值时返回元组，要变成list才行
                    exprs= list(exprs)          # 当从字典拿到一个值时返回expr, 可以直接传入agg()
                else: exprs= [ exprs ]                

                ldf_list.append( cur_expr_chain.agg( exprs ) )
        if not len( ldf_list ):
            logger.info(' '*space+'No tradition feats!!!')
            return None       # 如果不收集传统特征，则该函数返回None。
        else:
            df_list= pl.collect_all( ldf_list )     #########################################并行计算所有传统特征
            
        # 1----2, 串行计算所有传统特征 ，用于获取每个特征的耗时
        '''df_list= []    # 存放计算每一组特征的结果
        for path, subFeats in self.feats_dict.items():
            for feat in subFeats:
                if 'graph' not in path and 'ratio' not in path:     # 排除掉非传统特征
                    t1= time.time()
                    cur_expr_chain=self.recurFunc(path )    # 都是`groupby('time_bin')`结尾
                    #t2= time.time()
                    #logger.info(f' '*(space+2)+ f'func= `{path}_prepare`; cost={(t2-t1):.3f} sec')
                    
                    # 拿到当前小块subFeats对应的expr。itemgetter能从字典中同时访问多个key
                    path_end_little_dict= jsonpath.jsonpath(featTree.featTree, '$.'+ '.'.join( list(path) ))[0]
                    #exprs= itemgetter(*subFeats)( path_end_little_dict )  
                        # debug: 要保证传入agg()的exprs是list
                    #if isinstance(exprs, tuple):    # 当从字典拿到多个key的值时返回元组，要变成list才行
                    #    exprs= list(exprs)          # 当从字典拿到一个值时返回expr, 可以直接传入agg()
                    #else: exprs= [ exprs ]      

                    cur_res= cur_expr_chain.agg( path_end_little_dict[feat] ).collect()
                    logger.info(f' '*(space+2)+ f'func= `{feat}`; cost={(time.time()-t1):.3f} sec')
                    df_list.append( cur_res )'''
                
        if not len( df_list ):
            logger.info(' '*space+'No tradition feats!!!')
            return None       # 如果不收集传统特征，则该函数返回None。
        else:            

            # 最后合并所有df并按时间排序。
            df_res_tradi= df_list[0]
            for df_ in df_list[1:]:     # 注意：每个子df的行数可能不一样（差1）。因为有的upd文件的会多一个slot(仅多几个-几十个条目，且可能没有宣告条目造成的)
                df_res_tradi= df_res_tradi.join(df_, on='time_bin')     # 合并后,join自动把不同的行缩减为全相同的行
            df_res_tradi.sort('time_bin', in_place= True)
            
        # 2、根据传统特征计算ratio特征
            if ('ratio',) in self.feats_dict.keys():
                ratio_feats= self.feats_dict[('ratio',)]
                df_res_tradi= ratio(ratio_feats, featTree.featTree, df_res_tradi )
            logger.info(' '*(space+2)+ 'result DF in tradition: %s' % str(df_res_tradi.shape))
            
        # 3、删除临时大变量
            self.midnode_res.clear()
        # 4、临时备份传统特征
            '''p= self.raw_dir+'temp/'
            utils.makePath(p)
            with open(f'{p}tradi_feats.txt', 'a') as f:
                cont= df_res_tradi.to_csv()
                f.write('<< '*20+ f'first_timestamp= {self.first_ts}, a df in a collector in a event\n')
                f.write(cont)
                f.write('\n\n')'''
            
            return  df_res_tradi 

        

    @utils.timer
    def chunkForGraph(self, space ):   #space8()
        '''chunkHandler子函数/分支进程：采集图特征。args均来自self
        - 目的：得到每个slot的`最新完整AS边集合`，计算该graph下的若干特征。
        - 方法：(考虑生产-消费模型；考虑协程。。暂不考虑这个了)。
        - args:(进程共享list) self.df_res_graph : 该函数是独立子进程，其返回值放入类变量，从而能够传输到子进程外。
        - args:(pd.DF) self.pd_shared_preDF : 当前chunk预处理过的df再只保留宣告和4列(mem: 约1~2M/1万条) 
        - args:(dict) self.feats_dict 
        - args:(pd.DF) self.pd_shared_topo: [ peer_AS, dest_pref, path_raw ]  
        
        - DEBUG: 从父进程传来的DF均使用无效，因此从这里开始的所有函数中，涉及到pl.DF的均改为pandas.DF'''
        #logger.info(f'164行--->开始图特征提取chunkForGraph {utils.curMem()}')

        nbJobs= self.pd_shared_preDF.value['time_bin'].unique()   # 拿到当前chunk的所有time_bin块号，即需要并行的任务数。
        nbJobs.sort()
        
        # 2、并行（第一重并行）拿到每个slot的图（注：在GraphBase.perSlotComput有计算并行的特征（第二重并行））
        #       如何设计并行：被并行的函数参数是超大df对象，进程数多时会有df对象的重复拷贝，很占内存。因此考虑把参数放入manager()
        #       实验表明，当引入全量的rib表来获取每分钟的topo边集合(pd.DF下)内存压力相当大，多进程并行slots更不可能。
        #                 因此考虑只选择rib中行数最多的peer的所有行作为一个实际的rib。
        cores= utils.paralNum()
        logger.info(f' '*(space+2)+ f'`chunkForGraph`: processes: {cores}; cpus: {multiprocessing.cpu_count()}')

        pq1= ProcessingQueue( nbProcess= cores )
        manager1= multiprocessing.Manager()
            # 一、进程间通信：各slot子进程的return值并入manager。
        shared_res= manager1.list()
        lock= multiprocessing.Lock()    # 用于每个分钟进程最后把数据写入同一个文件。
                # 在图特征计算的每一个slot进程中，两个共享变量self.pd_shared_topo，self.pd_shared_preDF都只读不修改
                # 所有slot进程完毕后，要用self.preDF更新 self.pd_shared_topo 

        # 当rib表超大时，需要瘦身为只有一个peer后，才能用多进程
        if self.cut_peer:   
            logger.info(' '*(space+2)+ 'sharing vars below processes: chunk_upds:%sMb,%s; pd_shared_topo:%sMb,%s' 
                        % (utils.computMem(self.pd_shared_preDF.value), str(self.pd_shared_preDF.value.shape),
                        utils.computMem(self.pd_shared_topo.value), str(self.pd_shared_topo.value.shape) ))
            
            #for j in nbJobs:   # 一个任务即一个slot的数据 #TODO: range( nbJobs )     [nbJobs[0], nbJobs[-1]]
            for j in nbJobs: 
                pq1.addProcess( target= GraphBase.perSlotComput, args=(self.pd_shared_topo,self.pd_shared_preDF, shared_res, self.feats_dict, self.raw_dir, j, lock, space+4))
            pq1.run()
        
        # 当rib表为全量大表时，迫于可能的内存压力（在20211004-rrc00中用pd.DF获取一个slot的拓扑边时可高达40G+），因此只能串行计算，但其效率极低
        else:      
            logger.info(' '*(space+2)+ 'without parallel in build graph of each slot.')
            for j in nbJobs:
                GraphBase.perSlotComput(self.pd_shared_topo,self.pd_shared_preDF, shared_res, self.feats_dict, self.raw_dir, j, None, space+4)

        # 3，处理结果，把子进程的共享变量shared_res拆开一并传给父进程的共享变量self.df_res_graph。
        #       注：共享列表shared_res是list: 其元素为dict(key: 特征名, val: 统计值)。
        for item in shared_res:
            self.df_res_graph.append(item)

        del shared_res
        

    
    def filterInPreprocess(self):
        '''用于预处理前得到3个tag'''
            # 先把目标特征集合分组。。这里分了组的变量存入self，是为了在类外方便获取
        self.feats_dict= utils.featsGrouping(featTree.featTree, self.featNms)
        #dic_feat2expr= utils.feat2expr(featTree, feats) 暂时用不到
            # 后拿到一些tag，方便在中间特征预处理时，判断是否需要执行一些expr
        tag_path_unq, tag_oriAS= False, False
        tag_path_len= bool( set(["path_len_max", "path_len_avg", "is_longer_path", "is_shorter_path"]) & set(self.featNms) )       
        for k, v in self.feats_dict.items():
            if 'path_AStotal' in k:
                tag_path_unq= True
            if 'vol_oriAS' in k:
                tag_oriAS= True
        tag_path_unq= tag_path_unq | bool( set(["path_unq_len_max", "path_unq_len_avg", "is_longer_unq_path","is_shorter_unq_path"]) & set(self.featNms) )
        tag_oriAS= tag_oriAS | bool(set(["type_0", "type_1", "type_2", "type_3",] ) & set(self.featNms))

        return tag_path_len, tag_path_unq, tag_oriAS

    @utils.timer
    def postInChunkHandler(self, space=6):   # space8
        '''- ChunkHandler的收尾函数：更新 pd_shared_topo。
        - 拿到这个chunk所有的peer-pfx组的最后一行，加到 pd_shared_topo 再取peer-pfx的unique
        - debug: 由于pl.DF传到子进程无效，因此要pandas化，但这里是主进程，计算时又可暂时转为pl.DF（可能计算更快）'''
        ldf_all_upd= ( pl.DataFrame(self.pd_shared_preDF.value).lazy()
            .groupby([ 'peer_AS', 'dest_pref' ])
            .tail(1)
            .select([
                pl.col('peer_AS').cast(pl.Int64),
                'dest_pref',
                'path_raw'
            ] )   # 4 ->3列
        )
        ldf_topo =  pl.DataFrame(self.pd_shared_topo.value).lazy()
            
        self.pd_shared_topo.value= ( pl.concat( [ ldf_topo , ldf_all_upd ])   
            .groupby( [ 'peer_AS', 'dest_pref' ] )
            .tail(1)
        ).collect().to_pandas()

    @utils.timer
    def chunkHandler(self, chunk ,modify_df_topo, space ): # space6(timer用4，函数体内用6)
        '''monitorHandler 子函数：
        - arg:  chunk: upds文件名列表，存储了当前chunk的完整原始数据。
        - arg:  self.featNms: 目标特征集合（<= featTree的叶子数）
        - return: 当前chunk最终提取的特征矩阵
        - 作用：从一个DF提取特征完整流程, 定义一个框架：执行树，需求哪些特征就点亮相关节点。不管多少特征，都是要整个框架跑一遍
        - 注意：传统特征子函数放在主进程、图特征子函数放在分支进程。'''
        # 1、开始预处理。。。此后要考虑特征大类的选择了。
            # 先明确要生成哪些列
        tag_path_len, tag_path_unq, tag_oriAS= self.filterInPreprocess()
            # 后实现预处理。下面预处理后的df放进了self，是为了给recurFunc调用。
        self.preDF= preProcess(self.raw_fields, chunk, self.slot, self.first_ts, tag_path_len, tag_path_unq, tag_oriAS, 6)   # 最终决定：预处理过的df_pre不要lazy化
        #self.preDF= self.preDF[:10000000]

        # 2、正式采集特征：
        '''
            #   - (主进程)传统的：用到一个共享大表
            #           根据`分组字典 feats_dict` ，用pl.collect，造多条expr链来隐式并行计算。
            #           这里存在问题：非叶节点所在层数越高，其对应函数重复计算的次数就越多。
            #           考虑维护一个字典self.midnode_res，用以存储非叶函数的返回值。若后续节点路径中会用到，直接获取即可。
            #   - (进程分支)图的：用到两个共享大表
            #           pl.DF在进程间传递后无法使用。因此图特征计算全部依赖于pandas.DF
            # 
            # 思考：pl.df无法应用于子进程，但pd.df在大表下更耗时，怎么办？
            #       不知道在主进程中用pl.df串行算出每分钟的边集合，再下发给每个进程的耗时怎么样
            #       
            #       1，在传统特征、图特征并持的情况下没办法；
            #       2，当只有图特征时，'''
        self.df_res_graph= None # 用于拿出 图特征子进程的返回值
        df_res_graph= None
        df_res_tradi= None

        # 注意：不要企图把‘获取slot的全局edge集合’的DF计算的步骤放到主进程串行化。
        
        if self.need_rib:   # 当有图特征需求时
            # 2.1、先准备参数：从self.preDF获得共享大表self.pd_shared_preDF
            #       - 前者留作主进程的传统特征；
            #       - 后者对行列瘦身：列留4个；行根据cut_peer来取舍是否只留一个peer的条目。
            #       - 最后pandas化，才能把DF用于子进程。DEBUG: 这里存疑：为啥pl.DF无法在子进程中使用？？
            if self.peer:
                flt_expr= pl.col('peer_AS')== self.peer
            else:
                flt_expr= pl.col('peer_AS')!= -1
                # 用裁剪peer的方式瘦身共享大表应考虑的问题：某个slot没出现过该peer。因此nbJobs要从原始的preDF获取才能得到完整的time_bin块号
            self.pd_shared_preDF.value= ( self.preDF.lazy()
                #.filter( pl.col('msg_type')== 1 )  #debug: 不能删掉W条目，path_raw为空代表了那个前缀从路由表下线了。
                .filter( flt_expr )
                .select([ 
                    pl.col('time_bin'),     # 注：用裁剪peer的方式瘦身时，这里的块号可能不连续
                    'peer_AS', 
                    'dest_pref', 
                    'path_raw'])
            ).collect().to_pandas()

            # 2.2、然后准备一个共享变量，用于存放图特征分支进程的返回值
            manager0= multiprocessing.Manager()
            self.df_res_graph= manager0.list()

            #logger.info(f'270行--->从preDF中得到用作图特征的preDF后的进程总内存消耗 {utils.curMem()}')
            # 2.3、然后开一个进程分支处理图特征函数
            graph_process= multiprocessing.Process( target= self.chunkForGraph, args=( 6,) )
            graph_process.start()
        else:
            logger.info(' '*(space+2)+ 'No graph features!!!')
        
        df_res_tradi= self.chunkForTradi(6)  # 可能为None。列数= feats_tradi
        
        if self.need_rib:
            _ = self.pd_shared_topo.value.shape
            graph_process.join()

        # 3、合并传统特征、图特征两部分结果。
        if self.df_res_graph != None:   # 是list: 其元素为dict(key: 特征名/时间块号, val: 统计值)
            df_res_graph= []
            for item in self.df_res_graph:
                df_res_graph.append( item.copy() )
            df_res_graph= pl.DataFrame(df_res_graph).with_column(pl.col('time_bin').cast(pl.Int16))    # 图特征的结果转换为pl.df
            # 注意，此时df_res_graph 可能存在time_bin块号空缺的情况

        df_res= self.preDF.groupby('time_bin').agg(
            pl.col('timestamp').first().apply( lambda x: dt.datetime.fromtimestamp(x, tz= dt.timezone.utc).strftime('%Y/%m/%d %H:%M')).alias('date')
        )       # 合并两部分前，先初始化两列：time_bin+ date
        for df_ in [df_res_tradi, df_res_graph]:
            if df_:
                df_res= df_res.join(df_, on= 'time_bin', how= 'outer')
        df_res= df_res.fill_null('forward')     # 解决了 df_res_graph 可能存在time_bin块号空缺的情况

        # 4 一个chunk的后处理：（当有图特征时）更新 pd_shared_topo
        if modify_df_topo:
            try:
                self.postInChunkHandler(space+2)
            except Exception as e:
                #raise e
                logger.info(' '*(space+2)+ 'NO graph feats, NO need to update pd_shared_topo')
        
        return df_res

    @utils.timer
    def initHandler(self, paths_upd, real_sat_time, path_rib, space=4):      # space6(首尾空4，内部空6)
        '''monitorHandler 第一个子函数：给该事件下采集器下的 3或4个全局大表初始化
        - 3或4个大表变量：df_AS，ldf_peer_pfx，ldf_MOAS 属于传统特征， pd_shared_topo 属于图特征
        - args: paths_upd: 包含两部分upds文件：用于更新rib表的；真正事件始末的。
        - args: 无图特征时，real_sat_time, path_rib 均为 None        
        - return: paths: 是对paths_upd文件列表的裁剪，只留下真正事件始末的upd文件。'''
        
        #  1、先切分paths_upd，并造pd_shared_topo大表
        if real_sat_time:   # 即如需图特征
            paths_time_point= [ re.search('\d{8}.\d{4}', p).group() for p in paths_upd ]
            idx_watershed= paths_time_point.index( real_sat_time )
            paths= paths_upd[idx_watershed: ]
            
                # 大表4：特征采集前的最新的rib表：行上只有一种peer，列有3个字段[peer_AS, dest_pref, path]。
                # 注：self.peer用于在需要裁剪rib表的peer的情况下，存放保留的那个Peer
            self.pd_shared_topo.value, self.peer= GraphBase.latestPrimingTopo( self.raw_fields, path_rib, paths_upd[ :idx_watershed ], self.cut_peer ,6)
            logger.info(f' '*(space+2)+ f'need rib table (only including peer `{self.peer}`): {self.pd_shared_topo.value.shape[0]} lines')

        else:
            paths= paths_upd
        
        # 2、后造剩下3个大表
        cols_AS_table = [('AS_number', pl.UInt32),('counts', pl.UInt32)]
        cols_pp_table = [('index',pl.UInt32), ('timestamp', pl.Int32), ('time_bin',pl.Int16), ('msg_type',pl.Int8), ('peer_AS',pl.Int32), 
                        ('dest_pref',pl.Utf8), ('path_raw',pl.Utf8), ('hash_attr',pl.UInt64), ('path_len',pl.Int64), ('path_unq_len',pl.Int64),
                        ('origin_AS',pl.UInt32), ('tag_hist_cur', pl.Boolean)]
        self.first_ts= pl.scan_csv(paths[0], has_header=False, sep='|').fetch(1)[0,1]
        # 大表1：用来存所有AS的出现数量。
        self.df_AS= pl.DataFrame( columns= cols_AS_table )      
        
        # 大表2：9~12个字段。
        self.df_peer_pfx= pl.DataFrame( columns=cols_pp_table )
        
        # 大表3：9~12个字段，即时存储。
            # TODO:还要把受害者首次出现的条目写进来
            # df_MOAS用于收集劫持条目，这里不考虑收集type-1~3、type-U的情况，数量太多了。只考虑MOAS了。
        self.path_df_MOAS= ''
        
        return paths   

    def postHandler(self, save_path:str, space ):    #space6
        '''- monitorHandler 子函数：所有chunk都收集完了之后, 修缮特征矩阵、打标签'''
        #logger.info(' '*6+ 'post handle in cur monitor: ')
        # 1、把MOAS数据导出（这里不用了，而是在featTradition模块的 peerPfx_relateHijack 方法中顺便实现
        '''df_MOAS= (self.ldf_MOAS.collect()
            # debug: self.ldf_MOAS+expr+collect。会说expr链条中有不同列的DF竖直合并出错。但我溯源完全没找到病灶啊。很离谱。
            #      但self.ldf_MOAS+collect+expr，仅仅换顺序，就完全没问题啊。找原因失败，就直接换序吧。
            .select(pl.exclude(['time_bin', 'path_len', 'path_unq_len', 'tag_hist_cur']))
            .with_column(pl.col('timestamp')
                .apply(lambda x: dt.datetime.fromtimestamp(x, tz= dt.timezone.utc).strftime('%Y-%m-%d %H:%M'))
                .alias('time'))
        )
        df_MOAS.write_csv(MOAS_path, sep=',')'''

        # 2、修缮特征矩阵。可能出现一个slot有两行的情况，删掉
        df= pl.read_csv( save_path )
        df= ( df.groupby('date').sum().sort('time_bin').drop_nulls() )

        # 3、打标签
        event_name= save_path.split('__')[1]
        sat_end= ''
        with open( os.path.dirname(__file__)+'/event_list.csv' ) as f:
            while True:
                line= f.readline()
                if not line:    
                    break
                if event_name in line:
                    sat_end= ','.join( line.split(',')[1:3] )
                    break   
        sat_end= [ sat_end ]
        utils.labelMaker(save_path, sat_end)
        
        # 4、转储当前事件当前monitor下的图特征、传统特征的备份文件
        '''for type in ['graph', 'tradi']:
            p= f'Dataset/temp/{type}_feats_per_slot.txt'
            base= save_path.split("/")[-1]
            p2= f'Dataset/temp/{type}_{base}'
            if os.path.exists(p):
                os.system(f'mv {p} {p2}')'''


    def monitorHandler(self, paths_upd: list, real_sat_time, path_rib, evtNm= '_', monitor= '_' ):    # sapce4(代表函数体内)
        '''
        - description: eventHandler子函数。只想对自己指定的txtfiles提取特征时，也可使用该函数
        - args-> self {*}: 
        - args-> paths_upd {list | None}: updates文件名列表(List)。在有图特征情况下，包含了用于更新初始拓扑的那部分文件。
        - args-> real_sat_time {str | None}: 用于切分paths_upd
        - args-> path_rib {list}: rib文件名
        - args-> evtNm {*}: 
        - args-> monitor {*}: 
        - return {*}
        '''
        if paths_upd != None and paths_upd != []:
            # 1、先：做一些全局的初始化，并准备存储路径。 3或4个全局大表的初始化一定要在monitor循环里面。
            paths_actual= self.initHandler(paths_upd, real_sat_time, path_rib, 4)
            try:
                date= re.search('.(\d{8}).', paths_actual[0]).group(1)
            except:
                date= '__'
            '''原始：save_path= self.raw_dir+ 'features/%s__%s__%s.csv' % (date ,evtNm, monitor)
            self.path_df_MOAS= self.raw_dir+ 'MOAS/%s__%s__%s.csv' % (date ,evtNm, monitor)'''
            #修改： 
            d= 'Dataset/'
            save_path= d+ 'features/%s__%s__%s.csv' % (date ,evtNm, monitor)
            self.path_df_MOAS= d+ 'MOAS/%s__%s__%s.csv' % (date ,evtNm, monitor)
            
            utils.makePath(save_path)
            utils.makePath(self.path_df_MOAS)

            # 2、对一堆update文件分块后，按序对chunk解析特征。注意： peer-pfx-dynamic 类特征有时间关系，无法并行多个chunk
            llist= utils.splitChunk(paths_actual, self.need_rib)
            chunk_num= len(llist)
            for serialNum, chunk in enumerate(llist) :
                try:
                # chunkHandler开始一个完整的提取过程：upds文件名列表→ df→ 预处理→各层提取。
                    # 要实现的功能：输入任一特征名，得到一个列。耗时计算也要对应
                    modify_df_topo= True if serialNum+1 < chunk_num else False  # 用于指示最后一个chunk不需要更新rib表了
                    logger.info(f' '*4+ f'chunk-{serialNum+1}/{len(llist)} ---------- `chunkHandler` started:')
                    res= self.chunkHandler(chunk, modify_df_topo, 4 )
                    # 每得到一个chunk的特征，就直接写入文件
                        # 注意：一个chunk的首尾两个time_bin可能没有收集到完整的数据。下一个chunk内还可能有部分这个时刻的数据（后处理函数中解决）。
                    with open(save_path, 'a') as f:
                        has_head= True if serialNum==0 else False
                        #res.sort('time_bin', in_place=True)
                        res_= res.to_csv(has_header= has_head)
                        f.write(res_)
                except Exception as e:  
                    raise(e)
                
            # 3、 当前monitor采集的收尾工作
            self.postHandler(save_path, 6)
            
        # 只从多个rib表采集特征的场景
        else:
            res= {}
            feats_dict= utils.featsGrouping(featTree.featTree, self.featNms)
            # 遍历每个rib表的路径
            for path in path_rib:  
                t1= time.time()
                df_one_rib_a_peer, peer_cur= GraphBase.latestPrimingTopo(self.raw_fields, path, '')

                G= GraphBase.perSlotTopo(df_one_rib_a_peer)
                #G_narrwed, nodes= g_tool.getKcoreNodes(G)  # 缩减原始图规模
                feats_nx, feats_nk= GraphBase.funkList( feats_dict, G, G.nodes )
                result= GraphBase.parallFeatFunc( feats_nx, feats_nk ) # 12
                date= re.search('(\d{8}).\d{4}', path).group(1)
                res[date]= result
                # 临时存储已获得的采集特征
                with open('z_temp.csv', 'w') as f:
                    json.dump(res, f)

                logger.info(f'{path=} cost: {(time.time()- t1): .3f} sec.')

            # 写出最终结果
            out_dir= self.raw_dir+ 'features/'
            utils.makePath(out_dir)
            _df= pd.DataFrame(res)
            _df.to_csv( out_dir+ 'only_ribs__%s__%s.csv' % (evtNm, monitor))
            os.system('rm z_temp.csv')

    def eventHandler(self, upd_dic, rib_dic):    # sapce0(代表函数体内空0个，函数首尾空-2个)
        '''
        - description: 单事件特征采集接口。由于内存压力，考虑事件间串行。
        - args-> self {*}: 
        - args-> upd_dic {*}: format:`( evtNm, { monitor: ( [ paths ]|None , real_sat_time|None ) } ) | None`
            - 注: real_sat_time是指在有图特征情况下，updates文件们要被该参数按时序分为两部分。前部分用作rib表初始化，后部分用作传统特征提取、以及在每个slot画完整AS拓扑
            - 注: 值为`None`的场景：只从rib采集图特征。
        - args-> rib_dic {*}: format:`( evtNm, { monitor: [paths] } ) | None`
        - return {*}
        '''
        evtNm= upd_dic[0] if upd_dic else rib_dic[0]
        logger.info('* '*40)
        logger.info('START event: %s' % evtNm)
        t1= time.time()

        if upd_dic:
            # 注： paths_upd 可能为空[]
            for monitor, (paths_upd, real_sat_time ) in upd_dic[1].items():  # sapce2  # 可以并行每个monitor，但没有做，一般只需采集1-2个monitor
                logger.info('- '*20)
                logger.info(' '*2+ '%s: ' % monitor)
                
                if paths_upd==[] or paths_upd== None:
                    continue
                else:
                    t2= time.time()
                    if rib_dic:
                        path_rib= rib_dic[1][monitor]
                    else:
                        path_rib= None
                    # 拿到了一个rrc下的txt文件集合和一个rib.txt了...
                    self.monitorHandler( paths_upd, real_sat_time, path_rib, evtNm, monitor )

                    logger.info(' '*2+ '%s -- %s finished, time cost: %.3f sec ' % ( monitor, evtNm, time.time()- t2 ))
        else:
            for monitor, paths in rib_dic[1].items():
                logger.info(f' '*2+ f'{monitor}: ')
                if paths == [] or paths == None:
                    continue
                t2= time.time()
                self.monitorHandler( None, None, paths, evtNm, monitor)
                logger.info(f' '*2+ f'{monitor} -- {evtNm} finished, time cost: {(time.time()- t2): .3f} sec.')
        logger.info( 'END event: %s, time cost: %.3f sec. ' % ( evtNm, time.time()- t1))

    def run(self, only_rib= False):
        '''
        - description: 特征提取工具的主要api, 对特征列表进行完整的特征提取。
        - args-> only_rib {*}: 适用场景：`只从大量rib表采集图特征进行分析`
        - return {*} 特征存放的目录。
        '''
        ### 太初之时，设置多进程运行方式
        #multiprocessing.set_start_method('forkserver')
        ###
        if not len( self.featNms ):
            raise Exception('You have not select features. Please using `FET.FET.setCustomFeats()`')

        # 从给定的特征名中判断是否需要rib表(即图特征)，是否需要传统特征
        complete_graph_feats= featTree.getAllFeats()[104:]
        complete_tradi_feats= featTree.getAllFeats()[:104]
        self.need_rib = True if len( set(complete_graph_feats) & set(self.featNms ) ) else False 
        # debug: 只要是按slot时间块采集特征，那么任何特征都需要updates消息。因此`不需要updates消息`只适用于一种情况：只从rib表采集图特征的时候
        #self.need_upd = True if len( set(complete_tradi_feats) & set(self.featNms ) ) else False 
        self.need_upd = False if only_rib else True
        
        t_prepare_data= time.time()
        event_path= os.path.dirname(__file__)+'/event_list.csv'
        grd= GetRawData(event_path, self.raw_dir ,self.increment, self.duration, self.need_upd, self.need_rib)
        fileDict= grd.run()
        logger.info(f'time cost at download & parse data: {(time.time()-t_prepare_data):.3f}sec')
        
        utils.runJobs(fileDict, self.eventHandler)
        
        p=self.raw_dir+ 'features/'
        logger.info(f'FEATURE output path: {p}')
        return p

#简便模式：针对快速分析一个事件
def FET_vSimple(t_start, t_end, collector, stored_dir= './temp_events/', make_plot= False):
    '''- FET工具的简单版本，用于快速得到某一时段的简单特征曲线
        - return {pl.df}'''
    # 拿到要提取特征的paths列表
    paths= sorted(DownloadParseFiles('a', t_start, t_end, collector, stored_dir).run())
    #paths= sorted(glob.glob('/home/huanglei/work/z_test/1_some_events_details_analysis/temp_events/parsed/*'))

    # df的读取，预处理
    t0= time.time()
    bigdf= utils.csv2df(paths)
    print(f'read to bigdf cost: {(time.time()-t0):.2f} s')
    
    t1= time.time()
    first_ts= bigdf[0,'timestamp']
    df= (bigdf.lazy()
        .filter((pl.col("msg_type") != 'STATE'))
        .filter( ((pl.col('path').is_not_null()) | (pl.col("msg_type") == 'W') )) 
        .with_column( pl.col('path').str.replace(' \{.*\}', ''))
        .select([ 
            ((pl.col('timestamp')- first_ts)// 60).cast(pl.Int16).alias('time_bin'),
            pl.col('timestamp'), 
            pl.when( pl.col('msg_type')== 'A').then( pl.lit(1)).otherwise(pl.lit(0)).cast(pl.Int8).alias('msg_type'),
            'peer_AS', 
            pl.col('dest_pref').cast(pl.Utf8),
            pl.col('path').cast(pl.Utf8)        
        ])
        .with_columns( [
            pl.col('path').str.extract(' (\d+)$', 1).cast(pl.UInt32).alias('origin_AS')
        ] )
        .with_row_count('index')
    ).collect()
    print(f"feats pre-process: {(time.time()-t1):.2f} s, num_of_miniutes= {df['time_bin'].unique().shape[0]}")

    # 特征提取
    t2= time.time()
    ldf_list= [df.lazy().groupby('time_bin').agg( list(jsonpath.jsonpath(featTree.featTree, '$..vol_sim')[0].values())[:3])]

    func_names_pfx= list(featTree.featTree['volume']['vol_pfx'].keys())[:-1]+ \
                    list(featTree.featTree['volume']['vol_pfx']['vol_pfx_peer'].keys())
    for func_name in func_names_pfx:
        ldf_list.append( globals()[ func_name ]( df.lazy(), None ).agg(
            list(jsonpath.jsonpath(featTree.featTree, '$..'+func_name)[0].values())[0] ) )

    func_names_ori= list(featTree.featTree['volume']['vol_oriAS'].keys())
    for func_name in func_names_ori:
        ldf_list.append( globals()[ func_name ]( df.lazy().filter(pl.col('msg_type')== 1), None ).agg(
            list(jsonpath.jsonpath(featTree.featTree, '$..'+func_name)[0].values())[0]
        ) )

    df_list= pl.collect_all( ldf_list )
    print(f"got feats({len(df_list)}): {(time.time()-t2):.2f} s")

    # 特征汇总
    df_res= df.groupby('time_bin').agg(
        pl.col('timestamp').first().apply( lambda x: dt.datetime.fromtimestamp(x, tz= dt.timezone.utc).strftime('%Y/%m/%d %H:%M')).alias('date')
    ) 
    for df_ in df_list:   
        df_res= df_res.join(df_, on='time_bin')   
    df_res.sort('time_bin', in_place= True)

    # 导出
    p= f"{stored_dir}/simple_feats_{t_start}_{collector}.csv"
    df_res.to_csv(p)
    print(f"stored_path: `{p}`")

    # 作图
    if make_plot:
        simple_plot(p, subplots=False, has_label=False)
    return df_res

@utils.timer
def preProcess(fields, chunk, slot, first_ts, tag_plen= True, tag_punq= True, tag_oriAS= True, space= None):   # space8
    '''chunkHandler子函数: 读取文件名列表chunk，经过预处理成为updates消息的preDF
    - fields: DF的列名
    - chunk: 文件名列表
    - slot: 时间片大小
    - first_ts: 当前event→monitor下的 起始时间
    - tag_plen, tag_punq, tag_oriAS: 分别标记需不需要在预处理中执行3种expr
    - return: <=13列（一定有的8+1(index)列 + 可能有的4列 ）.
    - TODO: 还可再优化，越简单的特征，其实预处理步骤应该更少。'''
    df= utils.csv2df(chunk, fields ).with_columns([
        pl.col('path').cast(pl.Utf8),
        pl.col('dest_pref').cast(pl.Utf8),
        pl.col('local_pref').cast(pl.Int64),
        pl.col('MED').cast(pl.Int64),
    ])   # debug: 要做这一步格式转换，防止一个文件中全是STATE类的条目。
    sel_list= [ # 
            pl.col('timestamp').cast(pl.Int32),
            ((pl.col('timestamp')- first_ts)// slot).cast(pl.Int16).alias('time_bin'),
            pl.when( pl.col('msg_type')== 'A').then( pl.lit(1)).otherwise(pl.lit(0)).cast(pl.Int8).alias('msg_type'),
            pl.col('peer_AS').cast(pl.Int32),
            'dest_pref',
            pl.col('path').suffix('_raw'),
            pl.col('origin').map(lambda x: 0 if x=='IGP' else ( 1 if x== 'EGP' else 2)).cast(pl.UInt8),
            'hash_attr'
            ]
    add_col_list= []
    if tag_plen:
        # debug: arr.lengths()得到的时pl.UInt32，不能转换为Int8，只能转为更大的Int64
        sel_list.append( pl.col('path').str.split(" ").arr.lengths().cast(pl.Int64).alias('path_len') )    # .cast(pl.Int8)
    if tag_punq:
        sel_list.append( pl.col('path').str.split(" ").arr.unique().alias('path_unq') )
        # debug: arr.lengths()得到的时pl.UInt32，不能转换为Int8
        add_col_list.append( pl.col('path_unq').arr.lengths().cast(pl.Int64).alias('path_unq_len') )       # .cast(pl.Int8)
    if tag_oriAS:   # 提取path字段最后一个AS作为起源AS。  is `None` at 'msg_type== W'
        # debug: 下句多次测试没有任何问题。但使用了一个5G大小的df_upd就报错‘无法把utf8转为int32’，因此格式改为UInt32
        add_col_list.append( pl.col('path_raw').str.extract(' (\d+)$', 1).cast(pl.UInt32).alias('origin_AS') )
    
   # 先把整行内容哈希成一个值
    df['hash_attr']= df[:, 2:].hash_rows(k0=42)
    
    df= (df.lazy()
        #.with_column( df[:, 2:].hash_rows(seed=42).alias('hash_attr') )    # 0.14.10版本
        # 删除不是AW的行； 
        .filter((pl.col("msg_type") != 'STATE'))
        .filter( ((pl.col('path').is_not_null()) | (pl.col("msg_type") == 'W') ))     # 宣告条目中，path为空的过滤掉
        # 后删除行中带{}的AS。注意：不是删除含有{} 的整行，而是删除被{}包裹的AS
        .with_column( pl.col('path').str.replace(' \{.*\}', ''))
        .select( sel_list )
        .with_columns( add_col_list )
        .with_row_count('index')
        
    ).collect()


    if space != None:
       logger.info(' '*(space+2)+ 'after preprocess: df_mem: %sMb; pre_DF shape: %s' % (utils.computMem(df), str(df.shape)))
    return df  # 最终决定，预处理的df以非lazy形式给出。一方面分发给传统特征的诸多并行链条上，另一方面分发给图特征函数用。

class EventEditor():
    '''the increase, delete and other operations for the events list '''
    def __init__(self) -> None:
        self._clearEvents()
    
    def addEvents(self, evts:list):
        '''add any event you want.
        - arg  format: `['event_name, start_time, end_time(可为空), collector(可多个)']`
        - arg example: `["facebook_outage, 2021/10/4 15:40:00, 2021/10/4 21:40:00, rrc00, rrc06", "GoogleLeak, 2017/08/25 01:00:00, 2017/08/25 06:00:00, rrc06"]`
        '''
        with open(os.path.dirname(__file__)+'/event_list.csv', 'a') as f:
            for s in evts:
                f.write(s+'\n')

    def delEvents(self, evts:list):
        '''delete one or more events you have added.'''
        with open(os.path.dirname(__file__)+'/event_list.csv', 'r') as f:
            existed= f.readlines()
            tobedel= []
            if len(existed):
                for exist in existed:
                    if exist.strip() in evts:
                        tobedel.append(exist)
                res= set(existed)- set(tobedel)
        with open(os.path.dirname(__file__)+'/event_list.csv', 'w') as f:
            f.write(''.join(res))

    def getEventsList(self):
        '''return all events'''
        with open(os.path.dirname(__file__)+'/event_list.csv', 'r') as f:
            res= f.readlines()
            return res

    def _clearEvents(self):
        '''clear up all events'''
        utils.makePath(os.path.dirname(__file__)+'/event_list.csv')
        
class EventTool():
    ''''''

    @staticmethod
    def bestCollector( ASN ):
        '''给定一个AS号，得到与之在拓扑上最接近的采集器名称'''
        pass

if __name__=='__main__':
    mobj= FET()
    mobj.run()

