from copy import deepcopy
import polars as pl

from fastFET import utils
 
# 造一个特征树，包含BGP特征提取中所有想得到的特征。
# 叶子为pl.expr，叶父为feat，叶爷及以上为feat category，
featTree= { # df.lazy()
    "volume":{  # 大类1: 无需计算辅助型的中间特征，一般直接、或简单通过groupby([time_bin,其他]).agg(count()).groupby(time_bin)得到统计量。
        "vol_sim": {
            "v_total":  pl.col("msg_type").count().alias("v_total"),
            "v_A":      (pl.col("msg_type")== 1).sum().alias("v_A"),
            "v_W":      (pl.col("msg_type")== 0).sum().alias("v_W"),
            "v_IGP":    (pl.col("origin")== 0).sum().alias("v_IGP"),
            "v_EGP":    (pl.col("origin")== 1).sum().alias("v_EGP"),
            "v_ICMP":   (pl.col("origin")== 2).sum().alias("v_ICMP"),
            "v_peer":   pl.col("peer_AS").unique().count().alias("v_peer")
        },
        "vol_pfx": {    # 该节点是占位func
            "vol_pfx_total": utils.exprDict("v_pfx_t"),
            "vol_pfx_A": utils.exprDict("v_pfx_A"), 
            "vol_pfx_W": utils.exprDict("v_pfx_W"),
            "vol_pfx_peer": {   # 是占位func
                "vol_pfx_peer_total": utils.exprDict("v_pp_t"),
                "vol_pfx_peer_A": utils.exprDict("v_pp_A"), 
                "vol_pfx_peer_W": utils.exprDict("v_pp_W")
            }
        },
        "vol_oriAS": {          # 在有oriAS的情况下，也就等于是筛掉了W条目的情况。
            "vol_oriAS_total":  utils.exprDict( "v_oriAS_t"),
            "vol_oriAS_peer":   utils.exprDict( "v_oriAS_peer"),
            "vol_oriAS_pfx":    utils.exprDict( "v_oriAS_pfx"),
            "vol_oriAS_peer_pfx":utils.exprDict( "v_oriAS_pp")
        }
    },
    "path":{    # 大类2：与path字段相关的，包括path切分后的总AS数统计。
        "path_sim":{
            "path_len_max": pl.col('path_len').max().suffix('_max'),
            "path_len_avg": pl.col('path_len').mean().suffix('_avg'),
            "path_unq_len_max":  pl.col('path_unq_len').max().suffix('_max'),
            "path_unq_len_avg":  pl.col('path_unq_len').mean().suffix('_avg')   # debug: 这里不能转换格式`.cast(pl.Float32)`，会出现莫名其妙的报错：您要合并的series长度不等。至今仍搞不懂
        }, 
        "path_AStotal": {
            "path_AStotal_count": utils.exprDict( "As_total" ),
            "path_AStotal_rare": {
                "AS_rare_avg": (pl.col('rare_num')/ pl.col('upds_num')).sum().alias('AS_rare_avg'),
                "AS_rare_sum":  pl.col('rare_num').sum().alias('AS_rare_sum')
            }
        }
    },
    "peerPfx": {    # 大类3：属dynamic类+ editdistance类的特征，采集前需与历史peer-pfx表结合。
        "peerPfx_dynamic": {
            "is_WA":    (pl.col('type_diff')== 1).sum().alias('is_WA'),
            "is_AW":    (pl.col('type_diff')==-1).sum().alias('is_AW'),
            "is_WAW":   (pl.col('type_diff2')== -2).sum().alias('is_WAW'),
            "is_longer_path":       (pl.col('path_len_diff')>0 ).sum().alias('is_longer_path'),
            "is_shorter_path":      (pl.col('path_len_diff')<0 ).sum().alias('is_shorter_path'),
            "is_longer_unq_path":   (pl.col('path_unq_len_diff')>0 ).sum().alias('is_longer_unq_path'),
            "is_shorter_unq_path":  (pl.col('path_unq_len_diff')<0 ).sum().alias('is_shorter_unq_path'),
            #"is_MOAS": pl.col('is_MOAS').sum(),

            "is_new":       pl.col('is_new').sum(),
            "is_dup_ann":   pl.col('is_dup_ann').sum(),     # 重复宣告，这里只考虑pfx重复，而ori_AS、path、其他attr等均不考虑
            "is_AWnA":      pl.col('is_AWnA').sum(),
            "is_imp_wd":    pl.col('is_imp_wd').sum(),

            "is_WnA":   ((pl.col('msg_type')== 1) & (pl.col('type_diff2')== 1)).sum().alias('is_WnA'),
            "is_AWn":   ((pl.col('msg_type')== 0) & (pl.col('type_diff2')== 1)).sum().alias('is_AWn'),
            "is_AnW":   ((pl.col('msg_type')== 1) & (pl.col('type_diff2')==-1)).sum().alias('is_AnW'),
            "is_WAn":   ((pl.col('msg_type')== 0) & (pl.col('type_diff2')==-1)).sum().alias('is_WAn'),
            "is_dup_wd":((pl.col('msg_type')== 0) & (pl.col('type_diff')== 0 )).sum().alias('is_dup_wd'),
            
            "is_dup":   ((pl.col('is_dup_ann')== 1) & (pl.col('hash_attr_diff')== 0)).sum().alias('is_dup'),    # 完全重复的宣告，不仅pfx重复，其他所有字段均一模一样。
            "is_flap":  ( (pl.col('is_AWnA') == 1 ) & (pl.col('hash_attr_diff')== 0)).sum().alias('is_flap'),
            "is_NADA":  ( (pl.col('is_AWnA') == 1 ) & (pl.col('hash_attr_diff')!= 0)).sum().alias('is_NADA'),
            
            "is_imp_wd_spath": ( (pl.col('is_imp_wd')== 1) & (pl.col('hash_path_diff')== 0)).sum().alias('is_imp_wd_spath'),
            "is_imp_wd_dpath": ( (pl.col('is_imp_wd')== 1) & (pl.col('hash_path_diff')!= 0)).sum().alias('is_imp_wd_dpath'),          
        },
        "peerPfx_relateHijack": {
            "type_0": pl.col('type_0').sum(),
            "type_1": pl.col('type_1').sum(),
            "type_2": pl.col('type_2').sum(),
            "type_3": pl.col('type_3').sum()
        },
        "peerPfx_editdist": {
            "peerPfx_editdist_sim": {
                "ED_max": pl.col('ED').max().suffix('_max'),    # .cast(pl.Int8) debug: 有时候64→8 会报错
                "ED_avg": (pl.col('ED').sum()/pl.col('ED').count()).cast(pl.Float32).suffix('_avg'),
            },
            "peerPfx_editdist_num": dict([("ED_"+str(i), pl.col("ED_"+str(i)).sum() ) for i in range(11)])
        }
    },
    "ratio": {      # 大类4：属于二次加工特征，耗时低。无需放入特征树进行链式操作。
       'ratio_firstOrder': [ 'v_pfx_A_max','v_A' ],
       'ratio_ann': [ 'v_A','v_total' ],
       'ratio_wd': [ 'v_W','v_total' ],
       'ratio_origin0': [ 'v_IGP','v_total' ],
       'ratio_origin1': [ 'v_EGP','v_total' ],
       'ratio_origin2': [ 'v_ICMP','v_total' ],
       'ratio_dup_ann': [ 'is_dup_ann','v_A' ],
       'ratio_flap': [ 'is_flap','v_A' ],
       'ratio_NADA': [ 'is_NADA','v_A' ],
       'ratio_imp_wd': [ 'is_imp_wd','v_A' ],
       'ratio_imp_wd2': [ 'is_imp_wd','is_imp_wd','v_W' ],
       'ratio_exp_wd': [ 'v_W','is_imp_wd','v_W' ],
       'ratio_imp_wd_dpath': [ 'is_imp_wd_dpath','is_imp_wd' ],
       'ratio_imp_wd_spath': [ 'is_imp_wd_spath','is_imp_wd' ],
       'ratio_new': [ 'is_new','v_A' ],
       'ratio_wd_dups': [ 'is_dup_wd','v_W' ],
       'ratio_longer_path': [ 'is_longer_path','v_A' ],
       'ratio_shorter_path': [ 'is_shorter_path','v_A' ],
       'ratio_longer_path2': [ 'is_longer_path','is_longer_path','is_shorter_path' ],
       'ratio_shorter_path2': [ 'is_shorter_path','is_longer_path','is_shorter_path' ]    
    }, 
    "graph": {  # 大类5：图特征。从此不再需要用pl.expr了，也不需要链式函数了。因此以下，每个特征就直接对应一个计算函数。
                # 注：根据2020/10/04，rrc00,70min的特征提取耗时测试，特别耗时的13个图特征放弃提取（如下被注释掉的行）
                # 因此：nx的特征有2+5=7个 ；nk的特征有12个

        "graphNode_nx": {      # 4个 该类特征先得到每个节点的特征的值→ 后算均值
            #'nd_load_centrality': None,    # 45s
            'nd_degree' : None,
            #'nd_square_clustering': None,  # 53.6s
            'nd_average_neighbor_degree' : None            
        }, 
        "graphNode_nk": {   # 13个。该类特征也是先得到每个节点的特征的值→ 后算均值。只不过是networkit版本
            'nd_degree_centrality': None,
            'nd_node_clique_number': None,
            'nd_number_of_cliques': None,       # 1.3s。rib生成的无删减topo图下(8万node, 11万边)的耗时为 571s
            'nd_closeness_centrality': None,    # 上述场景下，23s 
            'nd_betweenness_centrality': None,  # 上述场景下，407s
            #'nd_local_efficiency': None,   # 33s
            'nd_harmonic_centrality': None,     # 上述场景下，289s
            'nd_eigenvector_centrality': None,
            'nd_pagerank': None,
            'nd_clustering': None,
            'nd_triangles': None,
            'nd_eccentricity': None,            # 上述场景下，361s
            'nd_average_shortest_pth_length': None  # 上述场景下，s
        },
        "graphInterAS":{    # 15 个nx。
            'gp_nb_of_nodes': None,
            'gp_nb_of_edges': None,
            'gp_diameter': None,
            'gp_assortativity': None,
            #'gp_largest_eigenvalue': None,          # adjacency_eigenvalues
            #'gp_algebraic_connectivity': None,      # laplacian_eigenvalues
            #'gp_effective_graph_resistance': None,  # laplacian_eigenvalues
            #'gp_symmetry_ratio': None,              # adjacency_eigenvalues
            #'gp_natural_connectivity': None,        # adjacency_eigenvalues
            #'gp_node_connectivity': None,          # 312s
            #'gp_edge_connectivity': None,          # 71s
            #'gp_weighted_spectrum_3': None,         # normalized_laplacian_eigenvalues
            #'gp_weighted_spectrum_4': None,         # normalized_laplacian_eigenvalues
            'gp_percolation_limit': None,
            #'gp_nb_spanning_trees': None           # laplacian_eigenvalues 
        }
    }
}



def getAllFeats( ):
    all= []
    def recur( key, dic ):
        if isinstance(dic, dict):
            for k,v in dic.items():
                recur( k, v )
        else:
            all.append( key )
    recur('', featTree )
    return all

def getDepend( feats ):
    '''- 对feats列表的扩充：若含有ratio类特征，获取与之相关的依赖特征并扩充进feats，后去重'''
    adds= []
    for f in feats:
        if 'ratio_' in f:
            adds+= featTree['ratio'][f]
    #res= list(set( feats+ adds ))       # debug: 我还想在去重的同时，保持顺序，则换成如下操作
    res= deepcopy(feats)
    for f in adds:
        if f not in res:
            res.append(f)

    return res

def getCateFeats( cate_list ):
    '''- 当FET对象初始化时指定了目标特征类别，这里可从类别列表中获取对应的具体特征列表。'''
    all_feats= getAllFeats()
    res=[]
    dic= {'volume': (0, 37), 'path': (37,46), 'dynamic': (46,71), 'editdistance': (71,84),
          'ratio': (84, 104), 'nodegraph': (104, 121), 'ASgraph': (121, 136) }
    for k,v in dic.items():
        if k in cate_list:
            res+= all_feats[ v[0]:v[1] ]
    # 针对ratio类，解决特征依赖问题
    res= getDepend( res )
    return res