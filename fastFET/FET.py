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
from fastFET.bgpToolKit import DownloadParseFiles
from fastFET.drawing import simple_plot

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
        self.featNms= []        
        self.feats_dict= {}     
        self.midnode_res= {}    
        self.pd_shared_topo = multiprocessing.Value(ctypes.py_object)   
        self.pd_shared_preDF= multiprocessing.Value(ctypes.py_object)

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
        ''''''
        if len(node_list):
            if node_list[-1] in self.midnode_res.keys():
                return self.midnode_res[ node_list[-1] ].lazy()
            return globals()[ node_list[-1] ]( self.recurFunc(node_list[:-1]), self )
        else:
            return self.preDF.lazy() 
     
    @utils.timer
    def chunkForTradi(self, space):   #space8
        '''采集传统特征'''
         
        ldf_list= []     
        for path, subFeats in self.feats_dict.items():
            if 'graph' not in path and 'ratio' not in path:      
                cur_expr_chain=self.recurFunc(path )                      
                path_end_little_dict= jsonpath.jsonpath(featTree.featTree, '$.'+ '.'.join( list(path) ))[0]
                exprs= itemgetter(*subFeats)( path_end_little_dict )  
                     
                if isinstance(exprs, tuple):     
                    exprs= list(exprs)           
                else: exprs= [ exprs ]                

                ldf_list.append( cur_expr_chain.agg( exprs ) )
        if not len( ldf_list ):
            logger.info(' '*space+'No tradition feats!!!')
            return None        
        else:
            df_list= pl.collect_all( ldf_list )      
            
        #  串行
        '''df_list= []     
        for path, subFeats in self.feats_dict.items():
            for feat in subFeats:
                if 'graph' not in path and 'ratio' not in path:      
                    t1= time.time()
                    cur_expr_chain=self.recurFunc(path )     
                    #t2= time.time()
                    #logger.info(f' '*(space+2)+ f'func= `{path}_prepare`; cost={(t2-t1):.3f} sec')
                                         
                    path_end_little_dict= jsonpath.jsonpath(featTree.featTree, '$.'+ '.'.join( list(path) ))[0]
                    #exprs= itemgetter(*subFeats)( path_end_little_dict )  
                         
                    #if isinstance(exprs, tuple):    
                    #    exprs= list(exprs)          
                    #else: exprs= [ exprs ]      

                    cur_res= cur_expr_chain.agg( path_end_little_dict[feat] ).collect()
                    logger.info(f' '*(space+2)+ f'func= `{feat}`; cost={(time.time()-t1):.3f} sec')
                    df_list.append( cur_res )
        '''
                
        if not len( df_list ):
            logger.info(' '*space+'No tradition feats!!!')
            return None        
        else:            
            df_res_tradi= df_list[0]
            for df_ in df_list[1:]:      
                df_res_tradi= df_res_tradi.join(df_, on='time_bin')      
            df_res_tradi.sort('time_bin', in_place= True)
            
            if ('ratio',) in self.feats_dict.keys():
                ratio_feats= self.feats_dict[('ratio',)]
                df_res_tradi= ratio(ratio_feats, featTree.featTree, df_res_tradi )
            logger.info(' '*(space+2)+ 'result DF in tradition: %s' % str(df_res_tradi.shape))
            
            self.midnode_res.clear()
            return  df_res_tradi        

    @utils.timer
    def chunkForGraph(self, space ):   #space8()
        '''图特征'''
        nbJobs= self.pd_shared_preDF.value['time_bin'].unique()    
        nbJobs.sort()

        cores= utils.paralNum()
        logger.info(f' '*(space+2)+ f'`chunkForGraph`: processes: {cores}; cpus: {multiprocessing.cpu_count()}')

        pq1= ProcessingQueue( nbProcess= cores )
        manager1= multiprocessing.Manager()
        shared_res= manager1.list()
        lock= multiprocessing.Lock()     

        if self.cut_peer:   
            logger.info(' '*(space+2)+ 'sharing vars below processes: chunk_upds:%sMb,%s; pd_shared_topo:%sMb,%s' 
                        % (utils.computMem(self.pd_shared_preDF.value), str(self.pd_shared_preDF.value.shape),
                        utils.computMem(self.pd_shared_topo.value), str(self.pd_shared_topo.value.shape) ))
            
            for j in nbJobs: 
                pq1.addProcess( target= GraphBase.perSlotComput, args=(self.pd_shared_topo,self.pd_shared_preDF, shared_res, self.feats_dict, self.raw_dir, j, lock, space+4))
            pq1.run()
        
        else:      
            logger.info(' '*(space+2)+ 'without parallel in build graph of each slot.')
            for j in nbJobs:
                GraphBase.perSlotComput(self.pd_shared_topo,self.pd_shared_preDF, shared_res, self.feats_dict, self.raw_dir, j, None, space+4)

        for item in shared_res:
            self.df_res_graph.append(item)

        del shared_res
    
    def filterInPreprocess(self):
        ''''''
        self.feats_dict= utils.featsGrouping(featTree.featTree, self.featNms)
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
        ''''''
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
    def chunkHandler(self, chunk ,modify_df_topo, space ): # space6
        '''
        - arg:  chunk: upds文件名列表
        - arg:  modify_df_topo: 是否需要更新pd_shared_topo'''
        tag_path_len, tag_path_unq, tag_oriAS= self.filterInPreprocess()
        self.preDF= preProcess(self.raw_fields, chunk, self.slot, self.first_ts, tag_path_len, tag_path_unq, tag_oriAS, 6)   
        
        self.df_res_graph= None  
        df_res_graph= None
        df_res_tradi= None

        if self.need_rib:   
            if self.peer:
                flt_expr= pl.col('peer_AS')== self.peer
            else:
                flt_expr= pl.col('peer_AS')!= -1
                
            self.pd_shared_preDF.value= ( self.preDF.lazy()
                .filter( flt_expr )
                .select([ 
                    pl.col('time_bin'),     
                    'peer_AS', 
                    'dest_pref', 
                    'path_raw'])
            ).collect().to_pandas()
            
            manager0= multiprocessing.Manager()
            self.df_res_graph= manager0.list()
            
            graph_process= multiprocessing.Process( target= self.chunkForGraph, args=( 6,) )
            graph_process.start()
        else:
            logger.info(' '*(space+2)+ 'No graph features!!!')
        
        df_res_tradi= self.chunkForTradi(6)          
        if self.need_rib:
            _ = self.pd_shared_topo.value.shape
            graph_process.join()
            
        if self.df_res_graph != None:   
            df_res_graph= []
            for item in self.df_res_graph:
                df_res_graph.append( item.copy() )
            df_res_graph= pl.DataFrame(df_res_graph).with_column(pl.col('time_bin').cast(pl.Int16))                  

        df_res= self.preDF.groupby('time_bin').agg(
            pl.col('timestamp').first().apply( lambda x: dt.datetime.fromtimestamp(x, tz= dt.timezone.utc).strftime('%Y/%m/%d %H:%M')).alias('date')
        )        
        for df_ in [df_res_tradi, df_res_graph]:
            if df_:
                df_res= df_res.join(df_, on= 'time_bin', how= 'outer')
        df_res= df_res.fill_null('forward')      
        
        if modify_df_topo:
            try:
                self.postInChunkHandler(space+2)
            except Exception as e:
                #raise e
                logger.info(' '*(space+2)+ 'NO graph feats, NO need to update pd_shared_topo')
        
        return df_res

    @utils.timer
    def initHandler(self, paths_upd, real_sat_time, path_rib, space=4):      # space6
        '''
        - args: paths_upd: 包含用于更新rib表的updates文件。
        - args: 无图特征时，real_sat_time, path_rib 均为 None
        '''
        if real_sat_time:    
            paths_time_point= [ re.search('\d{8}.\d{4}', p).group() for p in paths_upd ]
            idx_watershed= paths_time_point.index( real_sat_time )
            paths= paths_upd[idx_watershed: ]
            self.pd_shared_topo.value, self.peer= GraphBase.latestPrimingTopo( self.raw_fields, path_rib, paths_upd[ :idx_watershed ], self.cut_peer ,6)
            logger.info(f' '*(space+2)+ f'need rib table (only including peer `{self.peer}`): {self.pd_shared_topo.value.shape[0]} lines')

        else:
            paths= paths_upd
            
        cols_AS_table = [('AS_number', pl.UInt32),('counts', pl.UInt32)]
        cols_pp_table = [('index',pl.UInt32), ('timestamp', pl.Int32), ('time_bin',pl.Int16), ('msg_type',pl.Int8), ('peer_AS',pl.Int32), 
                        ('dest_pref',pl.Utf8), ('path_raw',pl.Utf8), ('hash_attr',pl.UInt64), ('path_len',pl.Int64), ('path_unq_len',pl.Int64),
                        ('origin_AS',pl.UInt32), ('tag_hist_cur', pl.Boolean)]
        try:
            self.first_ts= pl.scan_csv(paths[0], has_header=False, sep='|').fetch(1)[0,1]
        except:
            self.first_ts= pl.scan_csv(paths[0]).fetch(1)[0,1]
            
        self.df_AS= pl.DataFrame( columns= cols_AS_table )   
        
        self.df_peer_pfx= pl.DataFrame( columns=cols_pp_table )
        
        self.path_df_MOAS= ''
        
        return paths   

    def postHandler(self, save_path:str,  space= 6 , dont_label= None):    #space6
        '''标签'''
        df= pl.read_csv( save_path )
        df= ( df.groupby('date').sum().sort('time_bin').drop_nulls() )
        
        if not dont_label:
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

    def monitorHandler(self, paths_upd: list, real_sat_time, path_rib, evtNm= '_', monitor= '_', dont_label= False ):    # sapce4
        '''
        - args-> paths_upd {list | None}: updates文件名列表(List)。在有图特征情况下，包含了用于更新初始拓扑的那部分文件。
        - args-> real_sat_time {str | None}: 用于切分paths_upd
        - args-> path_rib {list}: rib文件名
        - args-> evtNm {*}: 
        - args-> monitor {*}: 
        - args-> dont_label {*}: 无需打标签操作。默认为False, 即需要打标签
        - return {*}: 默认将提取的特征存入`./Dataset/features/{data}__{evtNm}__{monitor}.csv`
        '''
        if paths_upd != None and paths_upd != []:
            paths_actual= self.initHandler(paths_upd, real_sat_time, path_rib, 4)
            try:
                date= re.search('.(\d{8}).', paths_actual[0]).group(1)
            except:
                date= '__'
            save_path= self.raw_dir+ 'features/%s__%s__%s.csv' % (date ,evtNm, monitor)
            self.path_df_MOAS= self.raw_dir+ 'MOAS/%s__%s__%s.csv' % (date ,evtNm, monitor)
            
            utils.makePath(save_path)
            utils.makePath(self.path_df_MOAS)
            
            llist= utils.splitChunk(paths_actual, self.need_rib)
            chunk_num= len(llist)
            for serialNum, chunk in enumerate(llist) :
                try:
                    modify_df_topo= True if serialNum+1 < chunk_num else False  
                    logger.info(f' '*4+ f'chunk-{serialNum+1}/{len(llist)} ---------- `chunkHandler` started:')
                    res= self.chunkHandler(chunk, modify_df_topo, 4 )
                    with open(save_path, 'a') as f:
                        has_head= True if serialNum==0 else False
                        #res.sort('time_bin', in_place=True)
                        res_= res.to_csv(has_header= has_head)
                        f.write(res_)
                except Exception as e:  
                    raise(e)
                    
            self.postHandler(save_path, 6, dont_label= dont_label)
            
        else:
            res= {}
            feats_dict= utils.featsGrouping(featTree.featTree, self.featNms)
            for path in path_rib:  
                t1= time.time()
                df_one_rib_a_peer, peer_cur= GraphBase.latestPrimingTopo(self.raw_fields, path, '')

                G= GraphBase.perSlotTopo(df_one_rib_a_peer)
                feats_nx, feats_nk= GraphBase.funkList( feats_dict, G, G.nodes )
                result= GraphBase.parallFeatFunc( feats_nx, feats_nk ) # 12
                date= re.search('(\d{8}).\d{4}', path).group(1)
                res[date]= result
                with open('z_temp.csv', 'w') as f:
                    json.dump(res, f)

                logger.info(f'{path=} cost: {(time.time()- t1): .3f} sec.')
                
            out_dir= self.raw_dir+ 'features/'
            utils.makePath(out_dir)
            _df= pd.DataFrame(res)
            _df.to_csv( out_dir+ 'only_ribs__%s__%s.csv' % (evtNm, monitor))
            os.system('rm z_temp.csv')

    def eventHandler(self, upd_dic, rib_dic):    # sapce0
        '''
        - description: 单事件特征提取
        - args-> upd_dic {*}: format:`( evtNm, { monitor: ( [ paths ]|None , real_sat_time|None ) } ) | None`
            - 注: real_sat_time是指在有图特征情况下，updates文件们要被该参数按时序分为两部分。前部分用作rib表初始化，后部分用作传统特征提取、以及在每个slot画完整AS拓扑
            - 注: 值为`None`的场景：只从rib采集图特征。
        - args-> rib_dic {*}: format:`( evtNm, { monitor: [paths] } ) | None`
        '''
        evtNm= upd_dic[0] if upd_dic else rib_dic[0]
        logger.info('* '*40)
        logger.info('START event: %s' % evtNm)
        t1= time.time()

        if upd_dic:
            for monitor, (paths_upd, real_sat_time ) in upd_dic[1].items():
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
        '''main func
        - args-> only_rib {*}: 为True时, 只从大量rib表采集图特征进行分析
        '''
        if not len( self.featNms ):
            raise Exception('You have not select features. Please using `FET.FET.setCustomFeats()`')
            
        complete_graph_feats= featTree.getAllFeats()[104:]
        complete_tradi_feats= featTree.getAllFeats()[:104]
        self.need_rib = True if len( set(complete_graph_feats) & set(self.featNms ) ) else False 
        
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


def FET_vSimple(t_start= None, t_end= None, collector= None, df= None, stored_dir= './raw_data/', make_plot= False):
    '''快速得到某一时段的简单特征, 并作曲线'''
    utils.makePath(stored_dir)
    if not df:
        paths= sorted(DownloadParseFiles('a', t_start, t_end, collector, stored_dir).run())
        t0= time.time()
        bigdf= utils.csv2df(paths)
        print(f'read to bigdf cost: {(time.time()-t0):.2f} s')
    else:
        bigdf= df
    
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
    
    df_res= df.groupby('time_bin').agg(
        pl.col('timestamp').first().apply( lambda x: dt.datetime.fromtimestamp(x, tz= dt.timezone.utc).strftime('%Y/%m/%d %H:%M')).alias('date')
    ) 
    for df_ in df_list:   
        df_res= df_res.join(df_, on='time_bin')   
    df_res.sort('time_bin', in_place= True)

    p= f"{stored_dir}/simple_feats_{t_start}_{collector}.csv"
    df_res.to_csv(p)
    print(f"stored_path: `{p}`")
    
    if make_plot:
        simple_plot(p, subplots=False, has_label=False)
    return df_res

@utils.timer
def preProcess(fields, chunk, slot, first_ts, tag_plen= True, tag_punq= True, tag_oriAS= True, space= None):   # space8
    '''对chunk预处理
    - fields: DF的列名
    - chunk: 文件名列表
    - slot: 时间片大小
    - first_ts:起始时间
    - tag_plen, tag_punq, tag_oriAS: 分别标记是否需要在预处理中执行3种expr
    '''
    df= utils.csv2df(chunk, fields ).with_columns([
        pl.col('path').cast(pl.Utf8),
        pl.col('dest_pref').cast(pl.Utf8),
        pl.col('local_pref').cast(pl.Int64),
        pl.col('MED').cast(pl.Int64),
    ])   
    sel_list= [ 
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
        
        sel_list.append( pl.col('path').str.split(" ").arr.lengths().cast(pl.Int64).alias('path_len') )    # .cast(pl.Int8)
    if tag_punq:
        sel_list.append( pl.col('path').str.split(" ").arr.unique().alias('path_unq') )
        add_col_list.append( pl.col('path_unq').arr.lengths().cast(pl.Int64).alias('path_unq_len') )       # .cast(pl.Int8)
    if tag_oriAS:  
        add_col_list.append( pl.col('path_raw').str.extract(' (\d+)$', 1).cast(pl.UInt32).alias('origin_AS') )
    
    df['hash_attr']= df[:, 2:].hash_rows(k0=42)
    
    df= (df.lazy()
        .filter((pl.col("msg_type") != 'STATE'))
        .filter( ((pl.col('path').is_not_null()) | (pl.col("msg_type") == 'W') ))     
        .with_column( pl.col('path').str.replace(' \{.*\}', ''))
        .select( sel_list )
        .with_columns( add_col_list )
        .with_row_count('index')
        
    ).collect()

    if space != None:
       logger.info(' '*(space+2)+ 'after preprocess: df_mem: %sMb; pre_DF shape: %s' % (utils.computMem(df), str(df.shape)))
    return df  
    
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


if __name__=='__main__':
    mobj= FET()
    mobj.run()

