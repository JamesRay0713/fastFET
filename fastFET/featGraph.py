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
    
    __slot__= []
    def __init__(self) -> None:
        pass
    @staticmethod
    def expr_block_cut(df:pl.DataFrame, peer ):
        '''assist in latestPrimingTopo'''
        res= ( df.lazy()
                .filter( pl.col('peer_AS')== peer )
                #.drop_nulls()
                .groupby( 'dest_pref' )
                .tail(1)
                #.filter( (~pl.col('path').str.contains('\{')) )
                .with_column( pl.col('path').str.replace(' \{.*\}', ''))
            )
        return res

    @staticmethod
    def expr_block_notcut(df:pl.DataFrame):
        '''assist in latestPrimingTopo'''
        res= ( df.lazy()
                .groupby( ['peer_AS','dest_pref'] )
                .tail(1)
                #.filter( (~pl.col('path').str.contains('\{')) )
                .with_column( pl.col('path').str.replace(' \{.*\}', ''))
            )
        return res
    
    @staticmethod
    def latestPrimingTopo( raw_fields, path_rib, paths_priming,cut_peer= True, space=6):    # space8
        '''
        - description: 得到指定时刻的rib表
        - args-> raw_fields {list}: 字段名
        - args-> path_rib {str}: rib路径
        - args-> paths_priming {list}: 需合并到rib的updates数据
        - args-> cut_peer {bool}: 默认需要裁剪至只剩单个peer
        - args-> space {*}: 用于logger
        - return {`(pd_df_rib ['peer_AS', 'dest_pref', 'path_raw' ], peer)`}
        '''
        peer= None
        df_rib= utils.csv2df( path_rib, raw_fields).select( [
                    'peer_AS', 'dest_pref', 'path' ])
        rib_lines= df_rib.shape[0]
        if cut_peer:
            peer_size= df_rib.groupby('peer_AS').agg(pl.col('path').count()).sort('path',reverse=True)
            peer= peer_size[0,0]

            logger.info(' '*(space+2)+ f'peers situation in `{os.path.basename(path_rib[0])}`:')
            logger.info(' '*(space+4)+ f"peers_num={peer_size.shape[0]}; peer_rank1=AS{peer}({peer_size[0,1]}/{peer_size['path'].sum()}); peer_rank2=AS{peer_size[1,0]}({peer_size[1,1]}/{peer_size['path'].sum()})")
            
            ldf_rib= GraphBase.expr_block_cut( df_rib, peer )  
        else:
            ldf_rib= GraphBase.expr_block_notcut(df_rib)
         
        if len(paths_priming):
            df_priming= utils.csv2df(paths_priming, raw_fields, not_priming= False).select( [
                    'peer_AS', 'dest_pref', 'path' ])    
            if cut_peer:
                ldf_priming= GraphBase.expr_block_cut( df_priming, peer )
            else:
                ldf_priming= GraphBase.expr_block_notcut(df_priming)
                
            lis= ['dest_pref'] if cut_peer else ['peer_AS', 'dest_pref']
            ldf_rib= ( pl.concat([ ldf_rib, ldf_priming ])   
                .groupby( lis )
                .tail(1)
                .drop_nulls()    
                #.with_row_count('index')    
            )
        
        res= ldf_rib.rename( {'path': 'path_raw'}).collect().to_pandas()
        return res, peer

    @staticmethod 
    #@utils.timer
    def perSlotTopo(pd_shared_topo, pd_shared_preDF= None, j= None, space=12):   # space14()
        '''获取当前slot的拓扑的边关系
        - pd_shared_preDF(pd.DF)
        - pd_shared_topo(pd.DF
        '''
        if pd_shared_preDF== None:
            pd_cur_upds= pd.DataFrame()
            pd_topo= pd_shared_topo
        else:
            pd_cur_upds= ( pd_shared_preDF.value
                .loc[ pd_shared_preDF.value['time_bin']<= j]
                .groupby([ 'peer_AS', 'dest_pref' ])
                .tail(1)
                .iloc[:, 1:])
            pd_topo= pd_shared_topo.value

        all_path= ( pd.concat([pd_topo, pd_cur_upds], ignore_index=True)
            .groupby([ 'peer_AS', 'dest_pref' ])
            .tail(1)
            .reset_index(drop=True)
            .loc[:,'path_raw']
            .str
            .split(" ")
            .explode()           
            .rename('AS_raw')
            )
             
        #logger.info(f'合并后: {utils.curMem()}')   
        all_path_shift= ( all_path
            .groupby(all_path.index)
            .shift(-1)
            #.fillna(method='ffill')     
            .rename('AS_shift'))
             
        res= ( pd.concat([ all_path, all_path_shift], axis=1)
                .rename(columns= {0: "AS_raw", 1: "AS_shift"})
            )
        res= (res
            .loc[res['AS_raw']!= res['AS_shift'], :]
            .drop_duplicates()
            .dropna()).values
        
        '''ldf_cur_upds= ( pd_shared_preDF.value#.lazy()   
            .filter( (pl.col('time_bin')<= j) )    
            .groupby([ 'peer_AS', 'dest_pref' ])
            .tail(1)
            .select( ['peer_AS','dest_pref','path_raw'] )    
        )#.collect()
        res = ( pl.concat( [ pd_shared_topo.value.lazy(), ldf_cur_upds.lazy() ])   
            .groupby( [ 'peer_AS', 'dest_pref' ] )
            .tail(1)
            #.with_row_count('index')             
            .drop_nulls()    
            .select([                             
                pl.col('path_raw').str.split(" ").alias('path_list_raw'),
                pl.col('path_raw').str.split(" ").arr.shift( -1 ).alias('path_list_sft')
            ])
            .explode( ['path_list_raw', 'path_list_sft'] )   
            .filter( (pl.col('path_list_sft') != None)&
                    (pl.col('path_list_raw') != pl.col('path_list_sft')) 
                    )       
            .unique()
        ).collect().to_numpy()'''

        G= nx.Graph()
        G.add_edges_from( res )
        if j==0:     
            logger.info(f' '*(space+2)+ f'draw latest AS-topo: edges:{res.shape[0]}; mem_G:{sys.getsizeof(G):4.3f}Mb' )
        return G

    @staticmethod
    #@utils.timer
    def getKcoreNodes(G, j= None, space=12):    # space14()
        '''获取k-core子图中的节点集合'''
        num_nodes_bfo= len(G.nodes)
        need_k_core= True
        if need_k_core:
            core_number = nx.core_number(G)
            k = np.percentile(list(core_number.values()),98)     
            G = nx.k_core(G, k, core_number)

        nodes = G.nodes
        if j==0:     
            logger.info(f' '*(space+2)+ f'nerrow the topo nodes: {num_nodes_bfo} -> {len(nodes)}' )
        return(G, nodes)

    @staticmethod
    def get_subgraph_without_low_degree(G, min_degree= 5):
        '''- 删除出度小于min_degree(含)的节点, 该方法快于k-core子图'''
        Gnew= nx.create_empty_copy(G)
        Gnew.add_nodes_from(G.nodes())
        Gnew.add_edges_from(G.edges())
        nodes_to_remove = [node for node, degree in dict(Gnew.out_degree()).items() if degree <= min_degree]
        Gnew.remove_nodes_from(nodes_to_remove)
        return Gnew, Gnew.nodes
    
    @staticmethod
    def funkList( feats_dict, G, nodes, space=12 ):   # space14()
        
        feats_nx= {}
        feats_nk= {}
        for path, feats in feats_dict.items():
            if 'graph' in path:
                if path[1]== 'graphNode_nx':
                    for feat in feats:
                        feats_nx[ feat ]= ( graphInterAS.avgNode, graphNode.__dict__[ feat ].__func__, G, nodes )
                if path[1]== 'graphNode_nk':
                    Gnk= graphNode_nk.nx2nk(G)
                    for feat in feats:
                        feats_nk[ feat ]= ( graphInterAS.avgNode, graphNode_nk.__dict__[ feat+'_nk' ].__func__, G, Gnk, nodes )
                if path[1]== 'graphInterAS':
                    
                    param= graphInterAS.prepareParam( feats, G )
                    for feat in feats:
                        feats_nx[ feat ]= ( graphInterAS.__dict__[ feat ].__func__, G, nodes, param )
        return ( feats_nx, feats_nk )

    @staticmethod
    def threadFunc( res_all:dict, feats_nk, space=14 ):   # space16()
        ''''''
        res_nk= {}
        for featNm, val in feats_nk.items():
            featFunc, *args= val
            try:
                t_= time()
                res_nk[ featNm ]= featFunc( *args )
                #logger.info(f' '*(space)+ f'thread_func= `{featNm}`; cost={(time() - t_):3.2f} sec; cur_memo= {utils.curMem()}')

            except Exception as e :
                logger.info(f' '*(space)+ f'Error with feature in thread: {featNm}' )
                raise e
        res_all.update( res_nk )     

    @staticmethod
    def run_simpFeats_inMulproc(feats_nx_simp, res_nx, space):
        t= time()
        for featNm, (func, *args) in feats_nx_simp.items():
            res_nx[ featNm ]= func( *args )
        logger.info(f' '*space+ f'funcs= `{len(feats_nx_simp)} simple feats`; cost= {(time()- t):3.2f} sec; cur_memo= {utils.curMem()}')
        
    @staticmethod
    @utils.timer
    def run_cmpxFeat_inMulproc( val, res_nx, featNm, space=14):       
        '''- 每个nx类特征的单一进程计算'''
        try:
            func, *args = val
            res_nx[ featNm ]= func( *args )
    
        except Exception as e:
            logger.info( ' '*space+ '! ! ! Error with feature: '+ featNm  )
            raise e

    @staticmethod
    #@utils.timer
    def parallFeatFunc( feats_nx, feats_nk, space=12 ):     
        ''''''
        res_all= {}
        if len(feats_nk):
            thread= threading.Thread( target= GraphBase.threadFunc, args=( res_all, feats_nk, space+2 ))
            thread.start()       
        
        # method 2
        '''feats_nx_simp= {}
        feats_nx_cmpx= {}
        res_nx_cp= {}
        if len(feats_nx):
            manager= multiprocessing.Manager()
            res_nx= manager.dict()
            for featNm, val in feats_nx.items():
                if featNm in ['gp_edge_connectivity', 'gp_node_connectivity', 'nd_square_clustering', 'nd_load_centrality']:
                    feats_nx_cmpx[featNm]= val
                else:
                    feats_nx_simp[featNm]= val

            if len(feats_nx_cmpx)>1:
                for feat, val in feats_nx_cmpx.items():
                    feats_nx_simp[feat]= val
                    break
                feats_nx_cmpx.pop(feat)
            if len(feats_nx_cmpx):
                pq= ProcessingQueue( nbProcess= min(len(feats_nx_cmpx), utils.paralNum()) )
                # pq.addProcess( target= GraphBase.run_simpFeats_inMulproc, args= (feats_nx_simp, res_nx, space+2))
                for featNm, val in feats_nx_cmpx.items():     
                    pq.addProcess( target= GraphBase.run_cmpxFeat_inMulproc, args= (val, res_nx, featNm, space+2) )
                pq.runOnce()
             
            t= time()
            for featNm, (func, *args) in feats_nx_simp.items():
                res_nx[ featNm ]= func( *args )
            logger.info(f' '*(space+2)+ f'funcs= `{len(feats_nx_simp)} simple feats`; cost= {(time()- t):3.2f} sec; cur_memo= {utils.curMem()}')

            if len(feats_nx_cmpx):
                pq.join()

            for k in list( feats_nx.keys()):
                res_nx_cp[k]= res_nx[k].copy()
            del res_nx'''

        # method 3
        res_nx_cp= {}
        if len(feats_nx):
            manager= multiprocessing.Manager()
            res_nx= manager.dict()
            for featNm, (func, *args) in feats_nx.items():
                t_= time()
                res_nx[ featNm ]= func( *args )
                #logger.info(f' '*(space)+ f'nx_func= `{featNm}`; cost={(time() - t_):3.2f} sec; cur_memo= {utils.curMem()}')

            
            for k in list( feats_nx.keys()):
                res_nx_cp[k]= res_nx[k].copy()
            del res_nx

        if len(feats_nk):
            thread.join()

        res_all.update( res_nx_cp )
        return res_all

    @staticmethod
    @utils.timer
    def perSlotComput(pd_shared_topo,pd_shared_preDF,shared_res, feats_dict, raw_dir, j, lock= None, space=10):   #space12()
        ''''''
        logger.info(f' '*(space)+ f'start `perSlotComput`-----------{j:4d}: ( pid:{os.getpid()}[{utils.curMem()}], ppid:{os.getppid()}[{utils.curMem(True)}] )')
        
        G = GraphBase.perSlotTopo(pd_shared_topo,pd_shared_preDF, j, space+2)
        num_ori_nodes= len(G.nodes)
        G, nodes= GraphBase.getKcoreNodes(G, j, space+2)    

        # case of not connected-subgraph
        if not nx.is_connected(G):
            connected_subgraphs = list(nx.connected_components(G))
            largest_subgraph = max(connected_subgraphs, key=len)
            largest_connected_subgraph = G.subgraph(largest_subgraph)
            logger.info(f"`G`(node: {len(G.nodes())}) is not connected-graph. You will get largest_subgraph(node: {len(largest_connected_subgraph.nodes())}")
            G= largest_connected_subgraph    
            nodes= G.nodes
            
        feats_nx, feats_nk= GraphBase.funkList( feats_dict, G, nodes, space+2 )

        result= GraphBase.parallFeatFunc( feats_nx, feats_nk, space+2 )
        result.update({'time_bin': j})

        shared_res.append(result )
        p= raw_dir+ 'temp/graph_feats_per_slot.txt'
        utils.makePath(raw_dir+ 'temp/')
        
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
    ''''''
    __slot__= []
    def __init__(self) -> None:
        pass

    def my_node_clique_number(G, nodes=None, cliques=None):
        """
        - Returns the size of the largest maximal clique containing each given node.
        - Returns a single or list depending on input nodes.
        - Optional list of cliques can be input if already computed.
        """
        if cliques is None:
            if nodes is not None:
                 
                if isinstance(nodes, list):
                    d = {}
                    for n in nodes:
                        H = nx.ego_graph(G, n)
                        d[n] = max(len(c) for c in find_cliques(H))
                else:
                    H = nx.ego_graph(G, nodes)
                    d = max(len(c) for c in find_cliques(H))
                return d
             
            cliques = list(find_cliques(G))
            
        all_nodes = False
        if nodes is None:
            all_nodes = True
            nodes = list(G.nodes())   

        if not isinstance(nodes, list):   
            v = nodes
             
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
            nodes = list(G.nodes())   
        if not isinstance(nodes, list):   
            v = nodes             
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

    '''def nx2cu(G):
        import cugraph, cudf
        edges = [(int(a),int(b)) for a,b in [*G.edges]]
        edgelistDF = cudf.DataFrame(edges, columns=['src','dst'])
        Gcu = cugraph.from_cudf_edgelist(edgelistDF, source='src', destination='dst', renumber=True)
        return(Gcu)'''

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
    def nx2nk(G):
        Gnk = nk.nxadapter.nx2nk(G)
        Gnk.indexEdges()
        return(Gnk)

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
    '''15个图特征'''
    __slot__= []
    def __init__(self) -> None:
        pass

    @staticmethod
    def avgNode(func, *args):
        ''''''
        dic= func( *args )
        if not isinstance(dic, dict):
            dic= dict(dic)
        return np.mean([*dic.values()])

    @staticmethod
    def prepareParam( feats, G ):   # space14()
        ''''''
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
    def gp_node_connectivity(G, nodes, _ ):         
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
    def gp_weighted_spectrum_4(G, nodes, eigenvalues=None):
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

def dictKeys(d, keys):
    ''''''
    subD = {}
    keys2 = dict(d).keys()
    for k in keys:
        if(k in keys2):
            subD[k] = d[k]
    return(subD)

def valuesDict(values, keys):
    ''''''
    return(dict(zip(keys, values)))
