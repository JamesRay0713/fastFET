from copy import deepcopy
import sys,inspect
import threading
from time import time
import numpy as np
import psutil, os
from fastFET.MultiProcess import ProcessingQueue
from fastFET import utils
from fastFET.utils import logger

import multiprocessing
import networkit as nk
import networkx  as nx
import pandas    as pd
import polars    as pl
from networkx import find_cliques



class GraphBase( object ):
    '''- 目标：为了有效计算图特征，需拥有在相应事件起始时间前1个slot的AS级拓扑'''
    __slot__= []
    def __init__(self) -> None:
        pass
    @staticmethod
    def expr_block_cut(df:pl.DataFrame, peer ):
        '''assist in latestPrimingTopo'''
        res= ( df.lazy()
                .filter( pl.col('peer_AS')== peer )
                #.drop_nulls()  不需要该句：rib表中path不可能为空；而updates又需保留path的空（代表撤销该条目）
                .groupby( 'dest_pref' )
                .tail(1)        # 注：df_priming 是从updates表来的，只需对每个唯一peer-pfx对取最新一个条目。
                #.filter( (~pl.col('path').str.contains('\{')) )    # 该方法可能误伤正常的行，改为str.replace
                .with_column( pl.col('path').str.replace(' \{.*\}', ''))        # 修改不正确的path格式
            )
        return res

    @staticmethod
    def expr_block_notcut(df:pl.DataFrame):
        '''assist in latestPrimingTopo'''
        res= ( df.lazy()
                .groupby( ['peer_AS','dest_pref'] )
                .tail(1)        # 注：df_priming 是从updates表来的，只需对每个唯一peer-pfx对取最新一个条目。
                #.filter( (~pl.col('path').str.contains('\{')) )    # 该方法可能误伤正常的行，改为str.replace
                .with_column( pl.col('path').str.replace(' \{.*\}', ''))        # 修改不正确的path格式
            )
        return res
    
    @staticmethod
    def latestPrimingTopo( raw_fields, path_rib, paths_priming,cut_peer= True, space=6):    # space8
        '''
        - description: 读取单个rib表为`DF(n*3)`, 如果rib表需要演进到指定时刻, 则需把`paths_priming`的updates的数据加入rib. 另外由于全量rib过大,默认裁剪数据为仅含一个peer.
        - args-> raw_fields {list}: 字段名
        - args-> path_rib {str}: rib路径
        - args-> paths_priming {list}: 需合并到rib的updates数据
            - 解释: 可通过RIPE-NCC等里的rib表拿到固定时刻的完整拓扑，但离事件起始时刻还有一定间隔，因此还需利用updates消息逐条更新到事件起始时刻
        - args-> cut_peer {bool}: 默认需要裁剪至只剩单个peer
        - args-> space {*}: 用于logger
        - return {`(pd_df_rib ['peer_AS', 'dest_pref', 'path_raw' ], peer)`}: (事件起始时刻全局路由拓扑, 被保留下来的peer-AS)
        '''
        '''
        - polars方法：获取rib-dump时刻到事件起始时刻的updates得到DF，直接对peer-pfx分组，得到每组path列的tail(1)，直接更新到rib表的拓扑
                    不裁剪peer的耗时：20211004-rrc00-5800万rib-5200万primingUpd-5270万最新rib---- 170s
                    裁剪至一个peer耗时：得到约100万pl_df_rib---- 42s
        '''
        peer= None
        # 1、先获取rib表并裁剪（只留3列，cut_peer为true时只留出现最多的peer的行）。
        df_rib= utils.csv2df( path_rib, raw_fields).select( [
                    'peer_AS', 'dest_pref', 'path' ])
        #df_rib.to_csv('/home/huanglei/work/z_test/1_some_events_details_analysis/testforGraph/a_complete_rib.csv')
        rib_lines= df_rib.shape[0]
        #cut_peer= rib_lines>300000 and cut_peer    # 更新‘裁剪peer’的tag  （更新：一旦需要计算图特征，则必须只保留一个peer的行）
        if cut_peer:
            # 拿到将被保留的peer
            peer_size= df_rib.groupby('peer_AS').agg(pl.col('path').count()).sort('path',reverse=True)
            peer= peer_size[0,0]

            logger.info(' '*(space+2)+ f'peers situation in `{os.path.basename(path_rib[0])}`:')
            logger.info(' '*(space+4)+ f"peers_num={peer_size.shape[0]}; peer_rank1=AS{peer}({peer_size[0,1]}/{peer_size['path'].sum()}); peer_rank2=AS{peer_size[1,0]}({peer_size[1,1]}/{peer_size['path'].sum()})")
            
            ldf_rib= GraphBase.expr_block_cut( df_rib, peer ) # debug：rib表的peer-pfx不一定具有唯一性，因此需分组取末行。但为了减少时间成本，应先裁剪了peer, 再对pfx去重
        else:
            ldf_rib= GraphBase.expr_block_notcut(df_rib)
        # 2、后获取priming_upds并裁剪（只留3列，cut_peer为true时只留刚刚找到的peer的行）
        if len(paths_priming):
            df_priming= utils.csv2df(paths_priming, raw_fields, not_priming= False).select( [
                    'peer_AS', 'dest_pref', 'path' ])   # 此时W类行的path为空。
            if cut_peer:
                ldf_priming= GraphBase.expr_block_cut( df_priming, peer )
            else:
                ldf_priming= GraphBase.expr_block_notcut(df_priming)
            # 3、开始对rib 和 前部分 updates 合并更新得最新df
            #    注意：原始的ldf_rib中，在相同的peer-pfx下会存在重复，即rib表的每个条目起码在peer-pfx分组上并不是独一无二的。
            lis= ['dest_pref'] if cut_peer else ['peer_AS', 'dest_pref']
            ldf_rib= ( pl.concat([ ldf_rib, ldf_priming ])  # tips：一定要rib消息在前，upds消息在后
                .groupby( lis )
                .tail(1)
                .drop_nulls()   # 这里包含的操作：若每组的最后一行为w消息，则删掉了该条目。
                #.with_row_count('index')   # 其实index列没啥用
            )
        
        res= ldf_rib.rename( {'path': 'path_raw'}).collect().to_pandas()
        return res, peer

    @staticmethod 
    #@utils.timer
    def perSlotTopo(pd_shared_topo, pd_shared_preDF= None, j= None, space=12):   # space14()
        '''self.perSlotComput子函数：拿到当前slot的拓扑的边关系(rib+到当前slot的upds)
        - pd_shared_preDF(pd.DF): chunk中只含A类型的updates的df['time_bin', 'peer_AS', 'dest_pref', 'path_raw'], 默认peer_AS只有一种
        - pd_shared_topo(pd.DF): 来自self.latestPrimingTopo。更新到事件起始时刻的rib表的df['peer_AS', 'dest_pref', 'path_raw'], 默认peer_AS只有一种
        - j: time_bin块号
        - return: 当前slot的rib表中提取的所有边构成的graph
        - 重要：由于不知名的原因，pl.DF无法在子进程中使用表达式，因此改为pandas.DF
        - 对比时间复杂度:（获取每slot的全局拓扑图所需边的方法）
            - 法1：用pandas.DF, upds和合并后的rib先后groupby取tail：去重前后630w+ →10w条边，约13sec。
            - 法3：用polars.DF：无法使用。若能使用，去重前后630w+ →10w条边，约3sec。可惜了
        '''
        # 事前：以下适用于调用该函数，仅对rib表构图的场景
        if pd_shared_preDF== None:
            pd_cur_upds= pd.DataFrame()
            pd_topo= pd_shared_topo
        else:
            # 法1：pd.DF。
                # 先拿到前j个slot中每组peer-pfx对的最新一条更新消息 pd_cur_upds
            pd_cur_upds= ( pd_shared_preDF.value
                .loc[ pd_shared_preDF.value['time_bin']<= j]
                .groupby([ 'peer_AS', 'dest_pref' ])
                .tail(1)
                .iloc[:, 1:])
                # 然后把 pd_cur_upds 合并到topo总表，并再次按组择其最新；后把path列展开成一列
                #   注：rib表约有150万+的路径，完全展开后将有650万+的AS。
            pd_topo= pd_shared_topo.value

        all_path= ( pd.concat([pd_topo, pd_cur_upds], ignore_index=True)
            .groupby([ 'peer_AS', 'dest_pref' ])
            .tail(1)
            .reset_index(drop=True)
            .loc[:,'path_raw']
            .str
            .split(" ")
            .explode()          # debug: explode比split(expand=True).stack()更快。
            .rename('AS_raw')
            )
            # 后把展开列按path分组算shift
        #logger.info(f'合并后: {utils.curMem()}')  # debug: 两次log发现，all_path带来巨大内存消耗，但该变量使用完毕后，为啥内存没恢复？？？
        all_path_shift= ( all_path
            .groupby(all_path.index)
            .shift(-1)
            #.fillna(method='ffill')    # debug: shift后最好不要填充，不然会影响端点相同边、重复边的统计值
            .rename('AS_shift'))
            # 两列合并起来便是边的两端点
        res= ( pd.concat([ all_path, all_path_shift], axis=1)
                .rename(columns= {0: "AS_raw", 1: "AS_shift"})
            )
            # 注：此时的res.shape=(650万+, 2)，占用800Mb+（里面有大量的重复）

            # 最后过滤掉：端点相同的边；重复出现的边；有端点为Null的边。
            # 注：需要认定 `(左,右) (右,左)`也属于重复边吗：不需要。一是代码设计不便；二是AS间的关系很难出现这种情况
            # 注：去重后res.shape=(10万+,2),占用13Mb+
        res= (res
            .loc[res['AS_raw']!= res['AS_shift'], :]
            .drop_duplicates()
            .dropna()).values
        
        # 法3：pl.DF方法
        '''ldf_cur_upds= ( pd_shared_preDF.value#.lazy()   
            .filter( (pl.col('time_bin')<= j) )   # tips：此时条目一定是按time_bin顺序的。
            .groupby([ 'peer_AS', 'dest_pref' ])
            .tail(1)
            .select( ['peer_AS','dest_pref','path_raw'] )   # 13 ->3列
        )#.collect()
        res = ( pl.concat( [ pd_shared_topo.value.lazy(), ldf_cur_upds.lazy() ])   
            .groupby( [ 'peer_AS', 'dest_pref' ] )
            .tail(1)
            #.with_row_count('index')            # 3->4列，用于在explode后区分每一行（即每一个peer-pfx对）。。其实不必，直接如下对list作shift即可
            .drop_nulls()   # debug: 一定要排查掉path_raw列值为Null的情况，不然explode会胡乱掉，难受。
            .select([                            # 3->2列
                pl.col('path_raw').str.split(" ").alias('path_list_raw'),
                pl.col('path_raw').str.split(" ").arr.shift( -1 ).alias('path_list_sft')
            ])
            .explode( ['path_list_raw', 'path_list_sft'] )  # 2列展开
            .filter( (pl.col('path_list_sft') != None)&
                    (pl.col('path_list_raw') != pl.col('path_list_sft')) 
                    )      # 自此，每行的2列就是拓扑图中一个边的连接关系。 
            .unique()
        ).collect().to_numpy()'''

        # 最后绘制拓扑图
        G= nx.Graph()
        G.add_edges_from( res )
        if j==0:    # 只在第一分钟处有日志
            logger.info(f' '*(space+2)+ f'draw latest AS-topo: edges:{res.shape[0]}; mem_G:{sys.getsizeof(G):4.3f}Mb' )
        return G

    @staticmethod
    #@utils.timer
    def getKcoreNodes(G, j= None, space=12):    # space14()
        '''用k-core缩减图的精度: 只要那些度>=k的节点
        - return: k-core子图，子图中的节点集合'''
        num_nodes_bfo= len(G.nodes)
        need_k_core= True
        if need_k_core:
            core_number = nx.core_number(G)
            k = np.percentile(list(core_number.values()),98)    # TODO: 该超参数可调
            G = nx.k_core(G, k, core_number)

        nodes = G.nodes
        if j==0:    # 只在第一分钟处有日志
            logger.info(f' '*(space+2)+ f'nerrow the topo nodes: {num_nodes_bfo} -> {len(nodes)}' )
        return(G, nodes)

    @staticmethod
    def get_subgraph_without_low_degree(G, min_degree= 5):
        '''- 删除出度小于min_degree(含)的节点
           - 该方法比上面那个k-core子图的方法更快'''
        Gnew= nx.create_empty_copy(G)
        Gnew.add_nodes_from(G.nodes())
        Gnew.add_edges_from(G.edges())
        nodes_to_remove = [node for node, degree in dict(Gnew.out_degree()).items() if degree <= min_degree]
        Gnew.remove_nodes_from(nodes_to_remove)
        return Gnew, Gnew.nodes
    
    @staticmethod
    def funkList( feats_dict, G, nodes, space=12 ):   # space14()
        ''' 获取所有图特征的函数接口 
        - arg: feats_dict: (key: path, val: feats)'''
        feats_nx= {}
        feats_nk= {}
        for path, feats in feats_dict.items():
            if 'graph' in path:
                # debug: 下列3个for中，`class.__dict__[feat]`形式得到的是不可调用的静态函数。
                #       所以要加小尾巴。详见：https://stackoverflow.com/questions/41921255/staticmethod-object-is-not-callable
                if path[1]== 'graphNode_nx':
                    for feat in feats:
                        feats_nx[ feat ]= ( graphInterAS.avgNode, graphNode.__dict__[ feat ].__func__, G, nodes )
                if path[1]== 'graphNode_nk':
                    Gnk= graphNode_nk.nx2nk(G)
                    for feat in feats:
                        feats_nk[ feat ]= ( graphInterAS.avgNode, graphNode_nk.__dict__[ feat+'_nk' ].__func__, G, Gnk, nodes )
                if path[1]== 'graphInterAS':
                    # 先准备一些参数。计算拉普拉斯相关的邻接矩阵等，相当耗时：1700个节点→ 150sec
                    param= graphInterAS.prepareParam( feats, G )
                    for feat in feats:
                        feats_nx[ feat ]= ( graphInterAS.__dict__[ feat ].__func__, G, nodes, param )
        return ( feats_nx, feats_nk )

    @staticmethod
    def threadFunc( res_all:dict, feats_nk, space=14 ):   # space16()
        '''- parallFeatFunc子函数1。
        注：这12个特征计算很快，可以用for串行'''
        res_nk= {}
        for featNm, val in feats_nk.items():
            featFunc, *args= val
            try:
                t_= time()
                res_nk[ featNm ]= featFunc( *args )
                logger.info(f' '*(space)+ f'thread_func= `{featNm}`; cost={(time() - t_):3.2f} sec; cur_memo= {utils.curMem()}')

            except Exception as e :
                logger.info(f' '*(space)+ f'Error with feature in thread: {featNm}' )
                raise e
        res_all.update( res_nk )    # 把nk部分的结果合并到总变量。

    @staticmethod
    def run_simpFeats_inMulproc(feats_nx_simp, res_nx, space):
        t= time()
        for featNm, (func, *args) in feats_nx_simp.items():
            res_nx[ featNm ]= func( *args )
        logger.info(f' '*space+ f'funcs= `{len(feats_nx_simp)} simple feats`; cost= {(time()- t):3.2f} sec; cur_memo= {utils.curMem()}')
        
    @staticmethod
    @utils.timer
    def run_cmpxFeat_inMulproc( val, res_nx, featNm, space=14):      # space16()
        '''- parallFeatFunc子函数：实现每个nx类特征的单一进程计算
        - args: val: (特征名, (计算特征的函数地址'''
        try:
            func, *args = val
            res_nx[ featNm ]= func( *args )
    
        except Exception as e:
            logger.info( ' '*space+ '! ! ! Error with feature: '+ featNm  )
            raise e

    @staticmethod
    #@utils.timer
    def parallFeatFunc( feats_nx, feats_nk, space=12 ):    # space14()
        '''perSlotComput子函数：得到一个slot的graph后，在此并行计算nx类（进程）和nk类（线程）特征
        - return: res_all, 当前slot下，每个图特征的统计值。'''
        res_all= {}

        # 1、nk相关13个特征的计算：单独开一个子线程(底层是C)
        #   tips，13个特征中， nd_local_efficiency 特征耗时严重，放弃采集。
        #   经测试，把thread.join()放在 step2的多进程后，能够实现nk与nx两部分的并行计算。
        if len(feats_nk):
            thread= threading.Thread( target= GraphBase.threadFunc, args=( res_all, feats_nk, space+2 ))
            thread.start()      # 这里只开了一个子线程来计算 13个 nk相关特征。
        

        # 2、nx相关19个特征用多进程（底层是python）
        #   其中有15个特征是秒得，仅4个需要30~200s的时间。因此考虑两种手段
        #   手段1: 19个特征分给19个进程
        '''tt= time()
            logger.info(f' '*(space+2)+ f'>>>> processes for nk starting:')
            manager= multiprocessing.Manager()
            res_nx= manager.dict()

            pq= ProcessingQueue( nbProcess= utils.paralNum() )
            for featNm, val in feats_nx.items():    # 19个任务用进程
                pq.addProcess( target= GraphBase.run_aFeat_inMulproc, args= (val, res_nx, featNm, 12) )
            pq.run()

            res_nx_cp= {}
            for k in list( feats_nx.keys()):
                res_nx_cp[k]= res_nx[k].copy()

            thread.join()
            logger.info(f' '*(space+2)+ f'>>>> processes for nk ending, cost={(time()-tt):.3f} sec.')'''
        
        #   手段2: 16个特征(其一为高复杂度)合到1个进程，剩余3个(复杂度高)单独各一个进程
        '''feats_nx_simp= {}
        feats_nx_cmpx= {}
        res_nx_cp= {}
        if len(feats_nx):
            manager= multiprocessing.Manager()
            res_nx= manager.dict()
            # 先把feats_nx切分成15+1*4
            for featNm, val in feats_nx.items():
                if featNm in ['gp_edge_connectivity', 'gp_node_connectivity', 'nd_square_clustering', 'nd_load_centrality']:
                    feats_nx_cmpx[featNm]= val
                else:
                    feats_nx_simp[featNm]= val

            # 后纷纷加入进程队列。
            # 鉴于进程开销大，如果 feats_nx_cmpx <2，则不开辟子进程算单个slot下的单个特征
            if len(feats_nx_cmpx)>1:    # 实现把一个复杂特征放入简单特征集合
                for feat, val in feats_nx_cmpx.items():
                    feats_nx_simp[feat]= val
                    break
                feats_nx_cmpx.pop(feat)
            # --- 并列的分支进程，操作 feats_nx_cmpx
            if len(feats_nx_cmpx):
                pq= ProcessingQueue( nbProcess= min(len(feats_nx_cmpx), utils.paralNum()) )
                # pq.addProcess( target= GraphBase.run_simpFeats_inMulproc, args= (feats_nx_simp, res_nx, space+2))
                for featNm, val in feats_nx_cmpx.items():    # <=5个复杂度高的任务用进程
                    pq.addProcess( target= GraphBase.run_cmpxFeat_inMulproc, args= (val, res_nx, featNm, space+2) )
                pq.runOnce()
            # --- 主进程，操作feats_nx_simp，直接全for, 串行计算所有特征
            t= time()
            for featNm, (func, *args) in feats_nx_simp.items():
                res_nx[ featNm ]= func( *args )
            logger.info(f' '*(space+2)+ f'funcs= `{len(feats_nx_simp)} simple feats`; cost= {(time()- t):3.2f} sec; cur_memo= {utils.curMem()}')

            # 并列的分支进程回收：
            if len(feats_nx_cmpx):
                pq.join()

            for k in list( feats_nx.keys()):
                res_nx_cp[k]= res_nx[k].copy()
            del res_nx'''

        #   手段3：放弃采集高耗时的12个特征，只采集7个。因此直接简化为在主进程中串行采集
        res_nx_cp= {}
        if len(feats_nx):
            manager= multiprocessing.Manager()
            res_nx= manager.dict()
            for featNm, (func, *args) in feats_nx.items():
                t_= time()
                res_nx[ featNm ]= func( *args )
                logger.info(f' '*(space)+ f'nx_func= `{featNm}`; cost={(time() - t_):3.2f} sec; cur_memo= {utils.curMem()}')

            
            for k in list( feats_nx.keys()):
                res_nx_cp[k]= res_nx[k].copy()
            del res_nx

        if len(feats_nk):
            thread.join()

        # 3、最后把多进程算得的特征合并到结果里
        res_all.update( res_nx_cp )
        return res_all

    @staticmethod
    @utils.timer
    def perSlotComput(pd_shared_topo,pd_shared_preDF,shared_res, feats_dict, raw_dir, j, lock= None, space=10):   #space12()
        '''功能：对一个slot的完整计算：更新rib表→ 从path获取所有边来制作graph→ 并行计算图中各种特征
        - 强调：该函数是并行的，而该函数中对特征的计算又是一重并行。另外要注意对大型变量及时del
        - args: (return)shared_res: 第一层多进程的共享变量，存储return值
        - args: shared_args（以下参数来自父进程，从mp.manager传进来）
                    feats_dict(dict) : 目标特征的路径与分组（path, feats）
                    chunk_upds(pd.DF): chunk中只含A类型的updates的df['time_bin', 'peer_AS', 'dest_pref', 'path_raw']
                                        注：4列DF，其mem约1~2Mb/万条。其实还是13列
                    pd_df_topo(pd.DF): 来自self.latestPrimingTopo。更新到事件起始时刻的rib表的df['peer_AS', 'dest_pref', 'path_raw']
        - args: j: 当前需截取 time_bin ≤ j 的 pd_shared_preDF 部分
        '''
        #logger.info(f'282行--->图模块：开始对一个slot的完整计算 {utils.curMem()}')
        # 0, 正式执行该函数前，考虑一个tips：在只观察一个peer的上下文中，瘦身得到的pd_shared_preDF可能在某个time_bin上没有条目
        #       造成该time_bin特征的重复计算。为此直接复制time_bin-1处算得得特征即可
        logger.info(f' '*(space)+ f'start `perSlotComput`-----------{j:4d}: ( pid:{os.getpid()}[{utils.curMem()}], ppid:{os.getppid()}[{utils.curMem(True)}] )')
        
        # 1, 画最新的全局拓扑。step1+step2.1耗时：在一个peer的98万个条目(即7万节点, 11万边→ 缩减1660节点, 1万边)下，耗时20s
        G = GraphBase.perSlotTopo(pd_shared_topo,pd_shared_preDF, j, space+2)
            # 保存缩减前的节点数、边数
        num_ori_nodes= len(G.nodes)

        
        # 2, 计算当前图中的特征
        # 2.1, G的简化：图太大，需要用k-core缩减图的精度        
        G, nodes= GraphBase.getKcoreNodes(G, j, space+2)        

        # 2.2, 特征函数的准备（实参也放进去）
        feats_nx, feats_nk= GraphBase.funkList( feats_dict, G, nodes, space+2 )
        #logger.info( utils.logMemUsage(10, inspect.stack()[0][3] ))     # 调试锚点：随删

        # 2.3, 并行计算图特征。return: dict(key: 特征名, val: 统计值)
        result= GraphBase.parallFeatFunc( feats_nx, feats_nk, space+2 ) # 12
        result.update({'time_bin': j})     # 给这一分钟的特征加一个行号。

        #3, return result加入进程共享变量
        shared_res.append(result )
        
        ### 保险起见，把这分钟的图特征结果存起来
        p= raw_dir+ 'temp/graph_feats_per_slot.txt'
        if j==0:
            utils.makePath(p)   # 具有清空功能
        if lock: lock.acquire()
        with open(f'{p}', 'a') as f:
            if j==0:
                key= ','.join( [ str(i) for i in list(result.keys())])
                f.write(key+'\n')
            val= ','.join( [ str(i) for i in list(result.values())])
            f.write(val+'\n')
        if lock: lock.release()

        del G
        del nodes

class graphNode( object ):
    '''4个nx的特征；13个nx和nk都可的特征（最终选择用nk）；2个备用的特征。'''
    __slot__= []
    def __init__(self) -> None:
        pass

    # assist func

    def my_node_clique_number(G, nodes=None, cliques=None):
        """处理每一个节点n：用ego_graph拿到n的1阶子图→ 在子图中获取所有极大团→ 对比这些团的节点数→ 拿到最大值（即clique_number）。
        - Returns the size of the largest maximal clique containing each given node.
        - Returns a single or list depending on input nodes.
        - Optional list of cliques can be input if already computed.
        """
        if cliques is None:
            if nodes is not None:
                # Use ego_graph to decrease size of graph
                if isinstance(nodes, list):
                    d = {}
                    for n in nodes:
                        H = nx.ego_graph(G, n)  # H即G的子图（以节点n为中心，1为半径）
                        d[n] = max(len(c) for c in find_cliques(H))
                else:
                    H = nx.ego_graph(G, nodes)
                    d = max(len(c) for c in find_cliques(H))
                return d
            # nodes is None--find all cliques
            cliques = list(find_cliques(G))
            
        all_nodes = False
        if nodes is None:
            all_nodes = True
            nodes = list(G.nodes())  # none, get entire graph

        if not isinstance(nodes, list):  # check for a list
            v = nodes
            # assume it is a single value
            d = max([len(c) for c in cliques if v in c])
        else:
            d = {}            
            for v in nodes:
                d[v] = 0
            for c in cliques:
                l = len(c)
                for v in c:
                    if(all_nodes or v in nodes):
                        d[v] = max(d[v],l)
        return d

    def my_number_of_cliques(G, nodes=None, cliques=None):
        """算一个节点的极大团的数量
        - Returns the number of maximal cliques for each node.
        - Returns a single or list depending on input nodes.
        - Optional list of cliques can be input if already computed.
        """
        if cliques is None:
            cliques = list(find_cliques(G))
        
        all_nodes = False
        if nodes is None:
            all_nodes = True
            nodes = list(G.nodes())  # none, get entire graph

        if not isinstance(nodes, list):  # check for a list
            v = nodes
            # assume it is a single value
            numcliq = len([1 for c in cliques if v in c])
        else:
            numcliq = {}
                
            for v in nodes:
                numcliq[v] = 0

            for c in cliques:
                for v in c:
                    if(all_nodes or v in nodes):
                        numcliq[v]+=1
            
        return numcliq

    def global_efficiency_nk(Gnk):
        n = Gnk.numberOfNodes()
        denom = n * (n - 1)
        if denom != 0:
            g_eff = 0
            lengths = nk.distance.APSP(Gnk).run().getDistances()
            for l in lengths:
                for distance in l:
                    if distance > 0:
                        g_eff += 1 / distance
            g_eff /= denom
        else:
            g_eff = 0
        return g_eff

    '''def nx2cu(G):   # 用不到
        import cugraph, cudf
        edges = [(int(a),int(b)) for a,b in [*G.edges]]
        edgelistDF = cudf.DataFrame(edges, columns=['src','dst'])
        Gcu = cugraph.from_cudf_edgelist(edgelistDF, source='src', destination='dst', renumber=True)
        return(Gcu)'''


    # main feat func: return的都是 dict（所有node，node对应特征的值）
    # 
    @staticmethod
    def nd_load_centrality(G, nodes):
        d = nx.load_centrality(G)
        return(dictKeys(d, nodes))
    @staticmethod
    def nd_degree(G, nodes):
        d = G.degree
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_degree_centrality(G, nodes):
        d = nx.degree_centrality(G)
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_square_clustering(G, nodes):
        d = nx.square_clustering(G, nodes=nodes)
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_average_neighbor_degree(G, nodes):
        d = nx.average_neighbor_degree(G, nodes=nodes)
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_node_clique_number(G, nodes):
        '''return 每个节点的`集团数`(即节点的所有极大团的节点数的最大值)'''
        if(nodes==G.nodes):
            d = graphNode.my_node_clique_number(G)
        else:
            d = graphNode.my_node_clique_number(G, nodes=list(nodes))
        return(dictKeys(d, nodes))
    @staticmethod
    def nd_number_of_cliques(G, nodes):
        '''返回 每个节点的'极大团'的数量'''
        if(nodes==G.nodes):
            d = graphNode.my_number_of_cliques(G)
        else:
            d = graphNode.my_number_of_cliques(G, nodes=list(nodes))
        return(dictKeys(d, nodes))
    @staticmethod
    def nd_closeness_centrality(G, nodes):
        v = [nx.closeness_centrality(G, u=n) for n in nodes]
        return(valuesDict(v, nodes))
    @staticmethod
    def nd_betweenness_centrality(G, nodes):
        d = nx.betweenness_centrality(G)
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_local_efficiency(G, nodes):
        v = [nx.global_efficiency(G.subgraph(G[n])) for n in nodes]
        return(valuesDict(v, nodes))
    @staticmethod
    def nd_harmonic_centrality(G, nodes):
        d = nx.harmonic_centrality(G, nbunch=nodes)
        return(dictKeys(d) )
    @staticmethod
    def nd_eigenvector_centrality(G, nodes):
        d = nx.eigenvector_centrality(G)
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_pagerank(G, nodes):
        d = nx.pagerank(G)
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_clustering(G, nodes):
        d = nx.clustering(G, nodes=nodes)
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_triangles(G, nodes):
        d = nx.triangles(G, nodes=nodes)
        return(dictKeys(d,nodes))
    @staticmethod
    def nd_eccentricity(G, nodes):
        v = [nx.eccentricity(G, v=n) for n in nodes]
        return(valuesDict(v, nodes))
    @staticmethod
    def nd_average_shortest_pth_length(G, nodes):
        def average_shortest_pth_length_node(G, n):
            return(np.mean(list(nx.single_source_shortest_path_length(G,n).values())))
        v = [average_shortest_pth_length_node(G, n) for n in nodes]
        return(valuesDict(v, nodes))


    # main feat func(BML暂未使用)
    def connectivity(G, nodes): # too slow, see approx version
        v = []
        for n in nodes:
            v.append(np.mean([nx.connectivity.local_node_connectivity(G,n,t) for t in nodes]))
        return(valuesDict(v, nodes))

    def approx_closeness_nk(G, Gnk, nodes):
        d = nk.centrality.ApproxCloseness(Gnk, len(nodes)).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

class graphNode_nk( object ):
    ''' 13个特征'''
    __slot__= []
    def __init__(self) -> None:
        pass

    # assitance func
    def nx2nk(G):
        Gnk = nk.nxadapter.nx2nk(G)
        Gnk.indexEdges()
        return(Gnk)

    # main func
    @staticmethod
    def nd_degree_centrality_nk(G, Gnk, nodes):
        d = nk.centrality.DegreeCentrality(Gnk, normalized=True).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

    @staticmethod
    def nd_node_clique_number_nk(G, Gnk, nodes):
        cliques = nk.clique.MaximalCliques(Gnk).run().getCliques()
        v = {}
        for node in Gnk.iterNodes():
            v[node] = 0

        for clique in cliques:
            l = len(clique)
            for node in clique:
                v[node] = max(v[node], l)
        return(dictKeys(valuesDict(v.values(), G.nodes), nodes))

    @staticmethod
    def nd_number_of_cliques_nk(G, Gnk, nodes):
        cliques = nk.clique.MaximalCliques(Gnk).run().getCliques()
        d = {}
        for n,v in zip(G.nodes, Gnk.iterNodes()):
            if(n in nodes):
                d[n] = len([1 for c in cliques if v in c])
        return(dictKeys(d, nodes))

    @staticmethod
    def nd_closeness_centrality_nk(G, Gnk, nodes):
        d = nk.centrality.Closeness(Gnk, False, False).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

    @staticmethod
    def nd_betweenness_centrality_nk(G, Gnk, nodes):
        d = nk.centrality.Betweenness(Gnk,normalized=True).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

    @staticmethod
    def nd_harmonic_centrality_nk(G, Gnk, nodes):
        d = nk.centrality.HarmonicCloseness(Gnk, normalized=False).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

    @staticmethod
    def nd_local_efficiency_nk(G, Gnk, nodes):
        v = [ graphNode.global_efficiency_nk( graphNode_nk.nx2nk(G.subgraph(G[n]))) for n in nodes]
        return(valuesDict(v, nodes))

    @staticmethod
    def nd_eigenvector_centrality_nk(G, Gnk, nodes):
        d = nk.centrality.EigenvectorCentrality(Gnk).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

    @staticmethod
    def nd_pagerank_nk(G, Gnk, nodes):
        d = nk.centrality.PageRank(Gnk).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

    @staticmethod
    def nd_clustering_nk(G, Gnk, nodes):
        d = nk.centrality.LocalClusteringCoefficient(Gnk).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

    @staticmethod
    def nd_triangles_nk(G, Gnk, nodes):
        d = nk.sparsification.TriangleEdgeScore(Gnk).run().scores()
        return(dictKeys(valuesDict(d, G.nodes), nodes))

    @staticmethod
    def nd_eccentricity_nk(G, Gnk, nodes):
        d = {}
        for n,v in zip(G.nodes, Gnk.iterNodes()):
            if(n in nodes):
                _,d[n] = nk.distance.Eccentricity.getValue(Gnk,v)
        return(d)

    @staticmethod
    def nd_average_shortest_pth_length_nk(G, Gnk, nodes):
        def average_shortest_pth_length_node(Gnk, n):
            return(np.mean(nk.distance.Dijkstra(Gnk, n).run().getDistances()))
        d = {}
        for n,v in zip(G.nodes, Gnk.iterNodes()):
            if(n in nodes):
                d[n] = average_shortest_pth_length_node(Gnk, v)
        return(d)


class graphInterAS( object ):
    '''15个图特征：7个普通；8个（可能）需要对`特征值`的追加计算'''
    __slot__= []
    def __init__(self) -> None:
        pass

    # assist func
    @staticmethod
    def avgNode(func, *args):
        '''- 因为class graphNode中特征函数返回的是所有节点的特征的值dict{node: val}
        - 所以需计算均值以得到该slot下的一个值'''
        dic= func( *args )
        if not isinstance(dic, dict):
            dic= dict(dic)
        return np.mean([*dic.values()])

    @staticmethod
    def prepareParam( feats, G ):   # space14()
        '''15个图特征的函数中，其中8个需要一些中间计算
        - arg: feat: 目标特征集合中的图特征名
        - return: 3种特征值'''
        eigenvalues= {}
        if len( set(["gp_effective_graph_resistance",  "gp_nb_spanning_trees", "gp_algebraic_connectivity"]) & set( feats )) :
            #logger.info(" "*14+ "Computing laplacian_eigenvalues")
            s = time()
            eigenvalues["laplacian"] = np.real(nx.laplacian_spectrum(G))
            logger.info(" "*14+ "Computing laplacian_eigenvalues (%.3f)sec" % (time()-s))
                
        if len(set(["gp_largest_eigenvalue", "gp_symmetry_ratio", "gp_natural_connectivity"] ) & set( feats )):
            #logger.info(" "*14+ "Computing adjacency_eigenvalues")
            s = time()
            eigenvalues["adjacency"] = np.real(nx.adjacency_spectrum(G))
            logger.info(" "*14+ "Computing adjacency_eigenvalues (%.3f)sec" % (time()-s))
                
        if len( set(["gp_weighted_spectrum_3", "gp_weighted_spectrum_4"]) & set( feats )):
            #logger.info(" "*14+ "Computing normalized_laplacian_eigenvalues")
            s = time()
            eigenvalues["normalized_laplacian"] = np.real(nx.normalized_laplacian_spectrum(G))
            logger.info(" "*14+ "Computing normalized_laplacian_eigenvalues (%.3f)sec" % (time()-s))

        return eigenvalues
        
    # main func     共有14个特征函数，15个特征
    @staticmethod
    def gp_nb_of_nodes(G, nodes, _ ):
        return(np.float64(len(G.nodes)))
    @staticmethod
    def gp_nb_of_edges(G, nodes, _ ):
        return(np.float64(len(G.edges)))
    @staticmethod
    def gp_diameter(G, nodes, _ ):
        return(np.float64(nx.diameter(G, usebounds=True)))
    @staticmethod
    def gp_assortativity(G, nodes, _ ):
        return(np.float64(nx.degree_assortativity_coefficient(G)))
    @staticmethod
    def gp_largest_eigenvalue(G, nodes, eigenvalues=None):
        adjacency_eigenvalues = None
        if(not eigenvalues is None):
            adjacency_eigenvalues = eigenvalues["adjacency"]
        if(adjacency_eigenvalues is None):
            adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
        return(np.float64(max(adjacency_eigenvalues)))
    @staticmethod
    def gp_algebraic_connectivity(G, nodes, eigenvalues=None):
        laplacian_eigenvalues = None
        if(not eigenvalues is None):
            laplacian_eigenvalues = eigenvalues["laplacian"]
        if(laplacian_eigenvalues is None):
            laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
            
        laplacian_eigenvalues = np.delete(laplacian_eigenvalues, laplacian_eigenvalues.argmin())
        v = np.min(laplacian_eigenvalues)
        return(np.float64(v))
    @staticmethod
    def gp_effective_graph_resistance(G, nodes, eigenvalues=None):
        laplacian_eigenvalues = None
        if(not eigenvalues is None):
            laplacian_eigenvalues = eigenvalues["laplacian"]
        if(laplacian_eigenvalues is None):
            laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
        laplacian_eigenvalues = np.delete(laplacian_eigenvalues, laplacian_eigenvalues.argmin())
        nonzero_eigenvalues = laplacian_eigenvalues[np.nonzero(laplacian_eigenvalues)]
        nst = len(G)*np.sum(1/nonzero_eigenvalues)
        return(np.float64(nst))
    @staticmethod
    def gp_symmetry_ratio(G, nodes, eigenvalues=None):
        adjacency_eigenvalues = None
        if(not eigenvalues is None):
            adjacency_eigenvalues = eigenvalues["adjacency"]
        if(adjacency_eigenvalues is None):
            adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
        r = len(np.unique(adjacency_eigenvalues))/(np.float64(nx.diameter(G, usebounds=True)) +1 )
        return(np.float64(r))
    @staticmethod
    def gp_natural_connectivity(G, nodes, eigenvalues=None):
        adjacency_eigenvalues = None
        if(not eigenvalues is None):
            adjacency_eigenvalues = eigenvalues["adjacency"]
        if(adjacency_eigenvalues is None):
            adjacency_eigenvalues = np.real(nx.adjacency_spectrum(G))
        nc = np.log(np.mean(np.exp(adjacency_eigenvalues)))
        return(np.float64(nc))
    @staticmethod
    def gp_node_connectivity(G, nodes, _ ):        # if ,too slow see approx
        return(np.float64(nx.node_connectivity(G)))
    @staticmethod
    def gp_edge_connectivity(G, nodes, _ ):
        return(np.float64(nx.edge_connectivity(G)))
    @staticmethod
    def gp_weighted_spectrum_3(G, nodes, eigenvalues=None):
        n=3
        normalized_laplacian_eigenvalues = None
        if(not eigenvalues is None):
            normalized_laplacian_eigenvalues = eigenvalues["normalized_laplacian"]
        if(normalized_laplacian_eigenvalues is None):
            normalized_laplacian_eigenvalues = np.real(nx.normalized_laplacian_spectrum(G))
        ws = np.sum((1-normalized_laplacian_eigenvalues)**n)
        return(np.float64(ws))
    @staticmethod
    def gp_weighted_spectrum_4(G, nodes, eigenvalues=None):     # 与前者重复的函数
        n=4
        normalized_laplacian_eigenvalues = None
        if(not eigenvalues is None):
            normalized_laplacian_eigenvalues = eigenvalues["normalized_laplacian"]
        if(normalized_laplacian_eigenvalues is None):
            normalized_laplacian_eigenvalues = np.real(nx.normalized_laplacian_spectrum(G))
        ws = np.sum((1-normalized_laplacian_eigenvalues)**n)
        return(np.float64(ws))
    @staticmethod
    def gp_percolation_limit(G, nodes, _ ):
        degrees = np.array(list(graphNode.nd_degree(G, nodes).values()))
        k0 = np.sum(degrees/len(G))
        k02 = np.sum((degrees**2)/len(G))
        pl = 1 - 1/(k02/k0 -1)
        return(np.float64(pl))
    @staticmethod
    def gp_nb_spanning_trees(G, nodes, eigenvalues=None):
        laplacian_eigenvalues = None
        if(not eigenvalues is None):
            laplacian_eigenvalues = eigenvalues["laplacian"]
        if(laplacian_eigenvalues is None):
            laplacian_eigenvalues = np.real(nx.laplacian_spectrum(G))
        laplacian_eigenvalues = np.delete(laplacian_eigenvalues, laplacian_eigenvalues.argmin())
        nonzero_eigenvalues = laplacian_eigenvalues[np.nonzero(laplacian_eigenvalues)]
        nst = np.prod(nonzero_eigenvalues/len(G))
        return(np.float64(nst))


### 辅助函数
def dictKeys(d, keys):
    '''根据keys获取d的子字典。
    - arg: d: 完整的字典
    - arg: keys: d的部分keys'''
    subD = {}
    keys2 = dict(d).keys()
    for k in keys:
        if(k in keys2):
            subD[k] = d[k]
    return(subD)

def valuesDict(values, keys):
    '''- 把val和key两个list组合成dict'''
    return(dict(zip(keys, values)))
