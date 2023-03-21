'''
Description: 
version: 1.0
Author: JamesRay
Date: 2022-09-19 18:24:57
LastEditors: Please set LastEditors
LastEditTime: 2022-10-05 12:23:57
'''
'''
用于特征提取的组件函数集合。
'''
import multiprocessing
import polars as pl
import pandas as pd
import numpy as np
import editdistance
import time

from fastFET import utils
from fastFET.MultiProcess import ProcessingQueue


######################## 
# feat tree node func. #
########################  

##### 以下为非叶子节点的同名函数，是pl.Expr链条的中间步骤。均为输入ldf，输出ldf
#### volume类
def volume(ldf, obj):    # 占位func
    return ldf
def vol_sim(ldf, obj):    # 经过阉割预处理后的lazyDF
    return ldf.groupby("time_bin")

def vol_pfx(ldf, obj):   # 占位func
    return ldf
def vol_pfx_total(ldf_vol_pfx, obj):
    ldf= ldf_vol_pfx.groupby(["time_bin", "dest_pref"]) \
        .agg([
            pl.col("msg_type").count().alias("v_pfx_t")
        ]) \
        .groupby("time_bin")
    return ldf
def vol_pfx_A(ldf_vol_pfx, obj):
    ldf= ldf_vol_pfx.groupby(["time_bin", "msg_type", "dest_pref"])\
        .agg([pl.col("timestamp").count().alias("v_pfx_A")]) \
        .filter(pl.col("msg_type")== 1).groupby("time_bin")
    return ldf
def vol_pfx_W(ldf_vol_pfx, obj):
    ldf= ldf_vol_pfx.groupby(["time_bin", "msg_type", "dest_pref"])\
        .agg([pl.col("timestamp").count().alias("v_pfx_W")]) \
        .filter(pl.col("msg_type")== 0 ).groupby("time_bin")
    return ldf
def vol_pfx_peer(ldf_vol_pfx, obj):  # 占位func
    return ldf_vol_pfx
def vol_pfx_peer_total( ldf_vol_pfx_peer, obj ):
    ldf= ldf_vol_pfx_peer.groupby(['time_bin','peer_AS','dest_pref']) \
    .agg([pl.col('index').count().alias("v_pp_t")]) \
    .groupby('time_bin')
    return ldf
def vol_pfx_peer_A( ldf_vol_pfx_peer, obj ):
    ldf= ldf_vol_pfx_peer.groupby(["time_bin", "msg_type",'peer_AS', "dest_pref"])\
        .agg([pl.col("timestamp").count().alias("v_pp_A")]) \
        .filter(pl.col("msg_type")== 1).groupby("time_bin")
    return ldf
def vol_pfx_peer_W( ldf_vol_pfx_peer, obj ):
    ldf= ldf_vol_pfx_peer.groupby(["time_bin", "msg_type",'peer_AS', "dest_pref"])\
        .agg([pl.col("timestamp").count().alias("v_pp_W")]) \
        .filter(pl.col("msg_type")== 0).groupby("time_bin")
    return ldf

def vol_oriAS(ldf, obj): # 该类下，自动滤掉W消息
    return ldf.filter(pl.col('msg_type')== 1)
def vol_oriAS_total( ldf_vol_oriAS, obj ):
    ldf= ldf_vol_oriAS \
        .groupby(['time_bin', 'origin_AS']) \
        .agg(pl.col('msg_type').count().alias("v_oriAS_t")) \
        .groupby('time_bin')
    return ldf
def vol_oriAS_peer( ldf_vol_oriAS, obj ):
    ldf= ldf_vol_oriAS \
        .groupby(['time_bin', 'origin_AS', 'peer_AS']) \
        .agg(pl.col('msg_type').count().alias("v_oriAS_peer")) \
        .groupby('time_bin')
    return ldf
def vol_oriAS_pfx( ldf_vol_oriAS, obj ): 
    '''该类下的3个特征，可能和前缀劫持强相关'''
    ldf= ldf_vol_oriAS \
        .groupby(['time_bin', 'origin_AS', 'dest_pref']) \
        .agg(pl.col('msg_type').count().alias("v_oriAS_pfx")) \
        .groupby('time_bin')
    return ldf
def vol_oriAS_peer_pfx( ldf_vol_oriAS, obj ):
    ldf= ldf_vol_oriAS.groupby(['time_bin','peer_AS','dest_pref', 'origin_AS']) \
            .agg([pl.col('index').count().alias('v_oriAS_pp')]) \
            .groupby('time_bin')
    return ldf

#### path类
    ## 该类下一些辅助函数
def get_rareAS_tag(grouped_df, obj):
    '''
    description:get the tags whether each AS belongs to rare ASes, and update the rare AS set.
    - param: grouped_df { DF[(index, u32), (time_bin, i16), (AS, str), (grp_num, int)] } 
    - param: obj.df_AS 专用来存每个AS的出现次数 { DF[('AS_number', UInt32), ('counts', UInt32)] }
    - return: cur_tag { DF['tag', 'index'] } 
    '''
    #grouped_ldf= grouped_df.lazy()  # 不管groupby前的是df还是ldf，进入groupby.apply后的参数都是未lazy状态，返回值也必须是未lazy的
    # first get the return vaule。先在df_AS中找到rare_AS最新列表
    value_005= obj.df_AS['counts'].quantile(0.05, 'nearest')
    if value_005:
        list_AS_rare=  obj.df_AS[ obj.df_AS['counts']<= value_005, 'AS_number'].to_series().to_list()
    else:
        list_AS_rare=['*']
    cur_tag= grouped_df.select( [pl.col('AS').cast(pl.Int64).is_in(list_AS_rare).alias('tag'),
        'index',
        ])   

    # 支线任务：update the rare set: 找到当前group中每个AS的出现次数，再合并到obj.df_AS大表中，按count值排序
    cur_df_AS= (grouped_df
        .select([
            pl.col('AS').value_counts().alias('count_struct')
        ])
        .select([
            pl.col('count_struct').struct.field('AS').alias('AS_number').cast(pl.UInt32),
            pl.col('count_struct').struct.field('counts').alias('counts').cast(pl.UInt32)
        ])
    )  
    obj.df_AS= (pl.concat([obj.df_AS, cur_df_AS ])
                .groupby('AS_number')
                .agg(pl.col('counts').sum())
                .sort('counts')
            )
    
    return cur_tag
    ##

def path(ldf, obj):  # 占位func
    return ldf
def path_sim(ldf, obj):
    return ldf.filter(pl.col('msg_type')== 1).groupby('time_bin')

def path_AStotal(ldf:pl.LazyFrame, obj ): 
    '''得到当前df中所有的AS。ldf['index', 'time_bin', 'AS']。
    - 注：AS在一个index标签下无重复，但整体来说有大量重复
    - 复杂度：explode('AS')操作使之升级为 O(n*k), n是行数，k是最大path长度''' 
    ldf_all_AS= (ldf
        .filter(pl.col('msg_type')==1)  # debug: 一定要把撤销消息排除掉再进行explode
        .select([
            #'index',   # 删了部分行后，需要重新index，见下with_row_count
            'time_bin',
            pl.col('path_unq').alias('AS')
        ])
        .drop_nulls()                   # debug: 排除掉’path_unq’字段为空的行。
        .with_row_count('index')
        .explode('AS')
    )        
    return ldf_all_AS
def path_AStotal_count( ldf_path_AStotal, obj ):
    ldf= ldf_path_AStotal.groupby(['time_bin','AS']) \
        .agg([pl.col('index').count().alias("As_total")
        ]).groupby('time_bin')
    return ldf
@utils.timer
def _path_AStotal_rare( ldf_path_AStotal:pl.LazyFrame, obj, space= 8 ):   # 原来的
    '''- 很耗时：自定义函数，得到每一个AS是否为rare-AS布尔值。AS出现次数的统计具有事件累积性，没办法并行。
    - 自定义函数中会自动更新每个AS出现次数到大表obj.df_AS中
    - '''
    # 先拿到每个AS的标记tag
    # debug: 放弃groupby.apply()，效率低且不好调试。直接用for循环
    # debug: 经测试maintain_order在非lazy下没有效果。。。。
        # 1 先设置组号
    
    df_path_AStotal= ( ldf_path_AStotal 
        .with_column( (pl.col('index')//obj.ASslot).alias('grp_num'))   # 每5000条为一组
        .collect())     # 4列['index','time_bin','AS','grp_num']

    grp_cnt= df_path_AStotal['grp_num'].max()+1   # 得到组数

        # 2 后按序计算各组，判断每一个AS是不是rare的tag，竖着合并到all_tag
        #       这里要读取历史大表`df_AS`，并在计算完tag后更新`df_AS`
        #       TODO: 尝试把df_path_AStotal 切成大块（根据分组数来切），并行，分别来for
    all_tag= pl.DataFrame( columns= [('tag', pl.Boolean), ('index', pl.UInt32)])
        
    for i in range( grp_cnt ):
        grpoued_df= ( df_path_AStotal.filter( pl.col('grp_num')== i) )
        cur_tag= get_rareAS_tag( grpoued_df, obj )
        
        all_tag= pl.concat( [all_tag, cur_tag] )
    
    all_tag= all_tag.sort('index').rename({'index':'index_tag'})#['tag']   # 标签df排序后，只留tag一列(删不删无所谓)
        
        # 3 后把算得的标记 all_tag['tag'] 并入 df_path_AS_total['index', 'time_bin', 'AS', 'grp_num']   -----> 得到5列
        #   后计算rareAS
        # debug: 使用join时，on的那一列的值应全各异。而这里index值会有重复，不适合用join来把tag合并到AStotal。还是用hstack吧
    df_path_AStotal= (df_path_AStotal.hstack( all_tag , in_place= False).groupby(['time_bin', 'index'])
        .agg([ pl.col('tag').sum().alias('AS_rare_num') ]) )    # TODO: 验证tag按序。
    ldf= ( df_path_AStotal.lazy()
        .groupby(['time_bin', 'index'])
        .agg([ pl.col('AS_rare_num').sum() ])
        .groupby('time_bin')
    )

    return ldf
@utils.timer
def __path_AStotal_rare( ldf_path_AStotal:pl.LazyFrame, obj, space= 8 ):   # 测试用
    '''重新设计‘稀有AS’的算法：
    - 摒弃：2个旧特征：一个slot中所有条目中rareAS的最大值、一个slot中rareAS的总个数（有重复）
    - 创建：2个新特征：一个slot中rareAS的总个数（无重复）、一个slot中rareAS在宣告类型条目中的均值
    - 新算法优点：不再以每5000行计算一次rareAS，而是简化为每个slot(分钟)算一次，这样也方便 groupby_rolling()中直接拿time_bin列来分组了。
    - 缺点：计算稀有AS的粒度更粗糙了一点。
    - args: ldf_path_AStotal ['index', 'time_bin', 'AS']
    - return: '''
    # debug: 放弃groupby.apply()，效率低且不好调试。直接用for循环
    # debug: 经测试maintain_order在非lazy下没有效果。。。。
    # 1、先按组（每个time_bin为一组）算出每组中每个AS的出现次数
    #       3列['time_bin':i32, 'AS':i32, 'counts':i32]；行数不再是ldf_path_AStotal的行数
        
    df_path_AStotal= ( ldf_path_AStotal
        .groupby(['time_bin','AS'])
        .agg([
            pl.col('index').count().alias("counts")
        ])
        .sort(['time_bin','counts'], reverse=[False, True])
        .collect()
    )     

    # 2 后用groupby_rolling得到每个AS在当前slot下的出现总次数
    num= df_path_AStotal['time_bin'].max()+1
    period= f'{num}i'
    a= ( df_path_AStotal
        .sort('time_bin')       # debug: 用作groupby_rolling的index一定要有序
        .groupby_rolling(index_column='time_bin', period='5000i', by='AS')     # 注：time_bin列需丢弃，改用time_bin_N
        .agg([
            pl.col('time_bin').last().alias('time_bin_N'),
            pl.col('counts').sum().alias('cur_sum_cnt')
        ])
        .select(['time_bin_N', 'AS', 'cur_sum_cnt'])
        .sort('time_bin_N')     # 接下来，需要在每个time_bin中补充一些行（在前面time_bin中出现过而未在当前time_bin中出现过的AS）
    )
    
    '''aaa= a.filter(
        (pl.col('grp_num')==(1)) & (pl.col('AS_number')==25152)
    )
        '''
    
    grp_cnt= df_path_AStotal['grp_num'].max()+1   # 得到组数

        # 2 后按序计算各组，判断每一个AS是不是rare的tag，竖着合并到all_tag
        #       这里要读取历史大表`df_AS`，并在计算完tag后更新`df_AS`
        #       TODO: 尝试把df_path_AStotal 切成大块（根据分组数来切），并行，分别来for
    all_tag= pl.DataFrame( columns= [('tag', pl.Boolean), ('index', pl.UInt32)])
        
    for i in range( grp_cnt ):
        grpoued_df= ( df_path_AStotal.filter( pl.col('grp_num')== i) )
        cur_tag= get_rareAS_tag( grpoued_df, obj )
        
        all_tag= pl.concat( [all_tag, cur_tag] )
    
    all_tag= all_tag.sort('index').rename({'index':'index_tag'})#['tag']   # 标签df排序后，只留tag一列(删不删无所谓)
        
        # 3 后把算得的标记 all_tag['tag'] 并入 df_path_AS_total['index', 'time_bin', 'AS', 'grp_num']   -----> 得到5列
        #   后计算rareAS
        # debug: 使用join时，on的那一列的值应全各异。而这里index值会有重复，不适合用join来把tag合并到AStotal。还是用hstack吧
    df_path_AStotal= (df_path_AStotal.hstack( all_tag , in_place= False).groupby(['time_bin', 'index'])
        .agg([ pl.col('tag').sum().alias('AS_rare_num') ]) )    # TODO: 验证tag按序。
    ldf= ( df_path_AStotal.lazy()
        .groupby(['time_bin', 'index'])
        .agg([ pl.col('AS_rare_num').sum() ])
        .groupby('time_bin')
    )

    return ldf
@utils.timer
def path_AStotal_rare( ldf_path_AStotal:pl.LazyFrame, obj, space= 8 ):   # 最新版
    '''重新设计‘稀有AS’的算法：
    - 摒弃：2个旧特征：一个slot中所有条目中rareAS的最大值、一个slot中rareAS的总个数（有重复）
    - 创建：2个新特征：一个slot中rareAS的总个数（无重复）、一个slot中rareAS在宣告类型条目中的均值
    - 新算法优点：不再以每5000行计算一次rareAS，而是简化为每个slot(分钟)算一次;然后用for分组串行计算。也不用groupby_rolling()了。
    - 缺点：计算稀有AS的粒度更粗糙了一点。
    - args: ldf_path_AStotal ['index', 'time_bin', 'AS']，该参数获得的复杂度是 O(n*k)
    - return: ldf.groupby ['time_bin':i16, 'rare_num':i32, 'upds_num':u32] '''
    # debug: 放弃groupby.apply()，效率低且不好调试。直接用for循环
    # debug: 经测试maintain_order在非lazy下没有效果。。。。
    # 1、先按组（每个time_bin为一组）算出每组中每个AS的出现次数
    #       3列['time_bin':i16, 'AS':str, 'counts':u32]；行数不再是ldf_path_AStotal的行数
    #       该操作复杂度：O(nk)log(O(nk))
    df_path_AStotal= ( ldf_path_AStotal
        .groupby(['time_bin','AS'])
        .agg([
            pl.col('index').count().alias("counts")
        ])
        .sort('time_bin')
        .collect()
    )   
    #       拿到每个slot下的宣告条目数，用于计算稀有AS的均值
    upds= ldf_path_AStotal.groupby('time_bin').agg(pl.col('index').unique().count().alias('upds_num')).collect()

    # 2 for
    rareAS_num=[]
    slots= df_path_AStotal['time_bin'].max()+1
    #       for内复杂度：n*O(xlogx),其中x= O(nk)log(O(nk)),  n是行数，k是最大path长度。简化后O(n^2klog^2(nk))
    for i in range( slots ):
        num= (df_path_AStotal
            .filter( pl.col('time_bin') <= i) 
            .groupby('AS').agg([
                pl.col('counts').sum()
            ])
            .select(
                ((pl.col('counts')- pl.col('counts').quantile(0.05))<= 0).sum()
            )
        )[0,0]
        rareAS_num.append(num)
    
    df= (pl.DataFrame({'time_bin': list(range(slots)), 'rare_num': rareAS_num})
        .with_columns( [
            pl.col('time_bin').cast(pl.Int16),
            pl.col('rare_num').cast(pl.Int32)
        ])
        .join(upds, on='time_bin')
        .sort('time_bin')
    )
    
    return df.lazy().groupby('time_bin')

#### peerPfx类
@utils.timer
def peerPfx(ldf: pl.LazyFrame, obj ):
    '''把历史peer-pfx表合并到当前df（9~12列）
    - arg: ldf：预处理得到的ldf维度可能是9~13列（因为有4列是选择性添加的）
    - return: ldf: 最多13列(删2增1) -> 12列；最少9列(删1增1) -> 9列。在行上要合并历史DF。 
    注：新老DF合并后，index列不再具有排序功能。'''
    
    ldf_cur= ldf.select( pl.exclude([ 'path_unq', 'origin'])) \
        .with_column( pl.col('peer_AS').cast(pl.Boolean).alias('tag_hist_cur') )   # 该列tag_hist_cur先全初始化为true
    # 大表df_peer_pfx的列初始化为12列，这里需裁剪为和ldf_cur的列一样
    obj.df_peer_pfx= obj.df_peer_pfx.select(ldf_cur.columns)
    # concat的复杂度：
    df_all= pl.concat([ obj.df_peer_pfx, ldf_cur.collect() ])  # debug: 奇怪,[lazyDF]被concat后，不能直接用lazy模式，否则报错 `ShapeError: Could not vertically stack DataFrame`。但是我用样例测试又没有问题。
    
    # 顺便开个支线任务：更新历史 peer_pfx大表
    obj.df_peer_pfx= ( df_all.lazy()
        .groupby(['peer_AS', 'dest_pref', 'msg_type'])
        .tail(1)
        .with_column((pl.col('tag_hist_cur')*0).cast(pl.Boolean))   # 当前chunk解析完时，把df_peer_pfx的tag_hist_cur列改为全0（即属于历史的）
    ).collect()
    # return前，同时把对应的df存入obj.midnode_res，被self.recur调用，防止重复计算。一定要collect计算后再存入。
    obj.midnode_res[ peerPfx.__name__ ]= df_all

    return df_all.lazy()
    
def peerPfx_dynamic(ldf_peerPfx: pl.LazyFrame, obj):
    '''args:
    - ldf_peerPfx: 当前chunk的df
    - obj：调用obj.ldf_peer_pfx历史大表，寻求旧表与新表数据衔接；
        调用obj.df_MOAS，以存储MOAS的情况
        调用obj.feats_dict，获取dynamic的那部分目标特征名'''
    feats= obj.feats_dict[ ('peerPfx', 'peerPfx_dynamic') ] # 元组是键
        # 第一层计算（若全特征参与，则该层得到13个中间列）
            # 实现 预处理的一些(表达式, 中间过程特征名) 和 特征名 的映射
    candidate= {
            # debug: 下述 value 元组的第二个值是列名，用于groupby后对成为了list的列使用explode展开时的参数。而'has_new'列的计算方法得到的是数值，
            #   不是list，因此不能放在explode([])里，它会在整表展开时自动填充，像groupby列那样。
        ("is_new",):  
            (pl.col('tag_hist_cur').all().alias('has_new'), ),  # 在当前chunk下，但凡有一个0（代表属于历史条目），这个peer-pfx都不是新的
        ("is_dup_ann","is_imp_wd","is_WnA","is_AWn","is_AnW","is_WAn","is_dup_wd","is_dup","is_imp_wd_spath","is_imp_wd_dpath"):    
            (pl.col('msg_type'), 'msg_type'),
        ("is_WA","is_AW","is_dup_ann","is_AWnA","is_imp_wd","is_dup_wd","is_dup","is_flap","is_NADA","is_imp_wd_spath","is_imp_wd_dpath"):    
            (pl.col('msg_type').diff().alias('type_diff'), 'type_diff'),
        ("is_WAW","is_AWnA","is_WnA","is_AWn","is_AnW","is_WAn","is_flap","is_NADA"):    
            (pl.col('msg_type').diff().diff().alias('type_diff2'), 'type_diff2'),
        ("is_imp_wd_spath","is_imp_wd_dpath"):    
            (pl.col('path_raw').hash(k0= 42).diff().alias('hash_path_diff'), 'hash_path_diff'),
            
        ("is_longer_path","is_shorter_path"):    
            (pl.when(pl.col('path_len')== 0).then(None).otherwise(pl.col('path_len')).fill_null('forward').diff().alias('path_len_diff'), 'path_len_diff'),
        ("is_longer_unq_path","is_shorter_unq_path"):    
            (pl.when(pl.col('path_unq_len')== 0).then(None).otherwise(pl.col('path_unq_len')).fill_null('forward').diff().alias('path_unq_len_diff'), 'path_unq_len_diff'),
        #("is_MOAS",):    (pl.col('origin_AS').alias('is_MOAS'), 'is_MOAS'),    #                 

        ("is_imp_wd","is_dup","is_flap","is_NADA","is_imp_wd_spath","is_imp_wd_dpath"):    
            (pl.when(pl.col('msg_type')== 0).then(None).otherwise(pl.col('hash_attr')).fill_null('forward').diff().alias('hash_attr_diff'), 'hash_attr_diff'),
        }
    agg_apend, explode_apend = [], []
    for k, v in candidate.items():
        if (set(k) & set(feats)):
            agg_apend.append(v[0])
            try:     explode_apend.append(v[1])
            except:  pass    # 该处是为了防止'has_new'的情况，因为has_new不需要explode
    agg_list= [pl.col('index'),
            pl.col('time_bin'),
            pl.col('tag_hist_cur').alias('belong_cur'), # 1表该条目属于当前chunk，0表属于历史数据        
            ]+ agg_apend
    explode_list= ['index','time_bin', 'belong_cur']+ explode_apend
    # debug: 因为在有的时候64→8 会报错，因此以下不再擅自转换格式
    '''modify_list= []
    if 'path_len_diff' in explode_list: modify_list.append( pl.col('path_len_diff').cast(pl.Int8) )
    if 'path_unq_len_diff' in explode_list: modify_list.append( pl.col('path_unq_len_diff').cast(pl.Int8) )'''
        # True:即非零，即AS前后不同，是MOAS；False:即0，即AS前后相同，不是MOAS 。
    #if 'is_MOAS' in explode_list: modify_list.append( pl.col('is_MOAS').diff().cast(pl.Boolean).cast(pl.Int8) )
    
    ldf_13= (ldf_peerPfx.groupby(['peer_AS','dest_pref']) 
        .agg(agg_list)
        .explode(explode_list)  # 缺一个 has_new, 在一个group里是一个值（在其他列explode时会自动填充），而不是一个list, 因此不需要
        .filter( pl.col('belong_cur')== True )  # 只保留了当前chunk的条目。因为第二三层共4个特征的计算是对第一层中间特征的再加工，而跟原始数据无关，因此可直接删除历史原始数据。
        #.with_columns(modify_list) # 修改格式
    )
    
        # 第二层计算（至多3个中间特征）
            # 实现 预处理的一些(表达式, 中间过程特征名) 和 特征名 的映射
    candidate2= {
        ('is_new',):
            [(pl.col('has_new')- (pl.col('has_new').shift_and_fill(1, 0)) ).alias('is_new'), 'is_new'], 
        ('is_dup_ann', 'is_dup', 'is_imp_wd', 'is_imp_wd_spath', 'is_imp_wd_dpath'):
           [ ( (pl.col('msg_type')== 1) & (pl.col('type_diff')== 0)).alias('is_dup_ann'), 'is_dup_ann'],
        ('is_AWnA', 'is_flap', 'is_NADA'):
            [((pl.col('type_diff')== 1) & (pl.col('type_diff2')>=1)).alias('is_AWnA'), 'is_AWnA'],
    }
    agg2_apend, explode2_apend= [], []

    for k,v in candidate2.items():
        if (set(k) & set(feats)): 
            agg2_apend.append(v[0])
            explode2_apend.append(v[1])
    ldf_3= (ldf_13.groupby(['peer_AS','dest_pref'])
            .agg([pl.col('index')]+ agg2_apend)
            .explode(['index']+ explode2_apend)
            )
    
        # 第三层计算（至多1个中间特征），同时把预处理阶段的≤18个预特征合并。
    modify_list3= []
    if 'is_new' in feats: 
        modify_list3.append( pl.col('is_new').cast(pl.Boolean) )
    if len(set(['is_imp_wd', 'is_imp_wd_spath', 'is_imp_wd_dpath']) & set(feats)): 
        modify_list3.append( ((pl.col('is_dup_ann')== 1) & (pl.col('hash_attr_diff')!= 0)).alias('is_imp_wd') )
    ldf_res17= ( ldf_13.join( 
                    ldf_3.select(['index']+ explode2_apend),    # 把ldf_3中的peer、pfx两个多余列删除
                    on='index')
                .with_columns( modify_list3 )
                )
    
    return ldf_res17.groupby('time_bin')

def peerPfx_relateHijack( ldf_peerPfx: pl.LazyFrame, obj ):
    '''- args: ldf_peerPfx：列12，行结合了历史peer-pfx表。
    - return: 一个新的ldf(最多5+4+4=13列)[ index, time_bin, 'tag_hist_cur', peer, dest_pref]+ ['path_loc0(i.e. is_MOAS)', 'path_loc1/2/3' ] + [type_0,1,2,3]
    - 如何使用：path_loc0123即ARTEMIS模型中的劫持类型Type0,1,2,3。注意，在判断loc1的两AS是否相同时，前提是loc0的AS必须相同。以此类推。
    '''
    feats= obj.feats_dict[ ('peerPfx', 'peerPfx_relateHijack') ]    # 元组是键
    # 先将ldf_peerPfx中path_raw列分解为loc0,1,2,3。在pl.str.split()中，找不到则返回None
    sel_apend= [pl.col('origin_AS').alias('path_loc0'), # 列path_loc0即此前的`is_MOAS`（这里为UInt32格式）
                pl.col('path_raw').str.extract(' (\d+) \d+$', 1).cast(pl.UInt32).alias('path_loc1'),
                pl.col('path_raw').str.extract(' (\d+) \d+ \d+$', 1).cast(pl.UInt32).alias('path_loc2'),
                pl.col('path_raw').str.extract(' (\d+) \d+ \d+ \d+$', 1).cast(pl.UInt32).alias('path_loc3')
    ]
    if 'type_3' not in feats: 
        sel_apend= sel_apend[:3]    
        if 'type_2' not in feats:
            sel_apend= sel_apend[:2]
            if 'type_1' not in feats:
                sel_apend= sel_apend[:1]
                if 'type_0' not in feats:
                    sel_apend=[]
    ldf_locAS= ldf_peerPfx.select( [ pl.col('index'), 'time_bin', 'peer_AS', 'dest_pref', 'path_raw', 'tag_hist_cur']+ sel_apend )

    # 后按peer-pfx分组，算path-loc0123中每列的diff()。算得的值为0，即该位置AS没变，无劫持；值为1，即AS变了，可能是劫持。
    agg_apend= []
    locNms= [nm for nm in ldf_locAS.columns if 'path_loc' in nm ]
    for colNm in locNms:
        agg_apend.append( pl.col(colNm ).diff().cast(pl.Boolean) )     ###### main
    ldf_locAS= ldf_locAS.groupby(['peer_AS', 'dest_pref']) \
        .agg( [ 'index', 'time_bin', 'tag_hist_cur' ]+ agg_apend ) \
        .explode( [ 'index', 'time_bin', 'tag_hist_cur' ]+ locNms ) \
        .filter( pl.col('tag_hist_cur')== True )    # 至多 5+4列；行：已过滤掉历史Peer-pfx.

    # 最后，判断确诊条目是否属于type-0123的劫持情况。# 至多 +4列。一个条目只属于type-0123中的一种，或者都不属于。
        # 若loc0的AS本就不同，那再看loc1的AS同不同就没意义了。因此有如下expr
    if 'type_0' in feats: ldf_locAS= ldf_locAS.with_column( pl.col('path_loc0').cast(pl.Int8).alias('type_0') )
    try:
        mid_expr= pl.col('path_loc0')
    except:
        pass
    for i in range(1, 4):
        if 'type_'+str(i) in feats:
            ldf_locAS= ldf_locAS.with_column( ( (mid_expr==0) & (pl.col('path_loc'+str(i) )==1) ).cast(pl.Int8).alias( 'type_'+str(i) ) )
        try:
            mid_expr= (mid_expr | pl.col('path_loc'+str(i) ))
        except:
            pass 
    # 最最后，要更新大表obj.ldf_MOAS
    # 因为路由震荡普遍存在，所以符合type-123的条目将是海量，我不打算全都dump下来。老方法，只dump type-0（即MOAS，即path_loc0）的情况
    if 'path_loc0' in locNms:  # 当目标feats列表中有收集'MOAS'的需求
        # 拿到所有属于MOAS的行索引号
        indexs= (ldf_locAS.filter(pl.col('path_loc0')== True)
            .select(pl.col('index')).collect()
            .to_series()
            .to_list())
        # 直接导出属于MOAS的条目到文件
        '''####立马删
        indexs.sort()
        with open('idx_locAS.txt', 'w') as f: 
            f.write(str(indexs))
        ldf_peerPfx.collect()['index']
        ####'''
        # TODO: 注：这里index有错
        content= ldf_peerPfx.collect()[indexs].to_csv(has_header= False)
        with open(obj.path_df_MOAS, 'a') as f:
            f.write(content)
        
    return ldf_locAS.groupby('time_bin')    # 需要按index排序吗? 不需要，因为下一步骤即按time_bin分组算统计量，排序意义不大

    ## _cal_ED中多进程辅助函数：计算ED

def _cal_edit_distance(res_lis, lis:list):
    ''' subfunction of multithread in `peerPfx_editdist`, in `lis[(idx, time_bin, p1, p2), (), ...]` the p1 and p2 are type of `list`. 
    - args: ED: 增量存储当前chunk下数据每个条目的ED值。'''
    #
    sub_res= []
    for idx, time_bin, p1, p2 in lis:
        if p2:  # 只对有标记的、第二个参数有值的行进行ED计算
            sub_res.append( (idx, time_bin, editdistance.eval(p1, p2)) )
        else:
            sub_res.append( (idx, time_bin, None) )
    res_lis.append(sub_res)
    ##
@utils.timer
def _cal_ED(df:pl.DataFrame, space):
    '''-arg: df:  ['index', 'time_bin', 'path_raw_list', 'path_raw_shift' ]
    - return: df: ['index', 'time_bin', 'ED']
    这里集成了多种方法获取ED，最终敲定：用pl.apply()的方式。
    - 且该函数不再使用，而是融合到 父函数中'''
    # 1、用进程池计算ED。报错：pool的子进程不能创建子进程。
    '''cores= multiprocessing.cpu_count()//2
    pool= multiprocessing.Pool(processes= cores)
    print('using %d cores in processing EditDistance!' % cores)
    result= pool.map(cal_edit_distance, tasks)  # worker返回tuple(index, time_bin, ED_val)到list[result]中
    pool.close()
    pool.join()'''
    # 2、用自定义多进程类。
    #       一个条目一个进程太耗时，因此仅需总共设置2-5个进程即可，同时主进程的计算能力也要用上。
    '''cores= 2
    chunksize= len(tasks)//cores
    tt= [ tasks[chunksize*i: chunksize*(i+1  )] for i in range(cores-1)]
    tt.append(tasks[chunksize*(cores-1):])
    processingQueue = ProcessingQueue( nbProcess= cores )
    manager= multiprocessing.Manager()
    res_lis= manager.list()

    t4= time.time()
    for t in tt:    # t是一个子list
        processingQueue.addProcess( cal_edit_distance, args= (res_lis, t) )
    processingQueue.run()
    print(f'多进程计算编辑距离 {time.time()- t4}sec')

    #       导出多进程的结果
    res= []
    for item in res_lis:
        res+= item.copy()'''

    # 3、普通for循环   (8s/100万条)
    #   法1：df转为行list再用for，转换过程耗时，但与pl.apply()相当。但缺点是：结果list还要在耗时来转为pl.df
    #   （不要用）法2：用for直接遍历df的每行，且直接把结果放进df，超级耗时
    '''res=[]
    tasks= df.rows()
    for idx, time_bin, p1, p2 in tasks:
        if p2:
            res.append( (idx, time_bin, editdistance.eval(p1, p2)))
        else:
            res.append( (idx, time_bin, None) )'''
            
    # 4、普通apply，准备先后尝试pd\pl的apply
    #    (8s/100万条)   pl.apply，即对每一行都使用一次func，而func的参数为一行的tuple。return单列DF
    def func( tup ):
        if tup[3]:
            r= editdistance.eval(tup[2], tup[3])
        else:
            r= None
        return r
    col_ED= df.apply(func)
    df['ED']= col_ED['apply']
    df= df.select(['index', 'time_bin', 'ED'])

    #    (40s/100万条)   pd.apply，默认下(axis=0)，对每一列使用一次func，而func的参数为一列的series；行相反。return series
    '''def func( tup ):
        try:
        #if str(tup[3]) != 'None':   # debug: tup[3]是一个list或None，当它不空时会报错`The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()`
                                    #       即存在 `if [True, True, False]`的语法错误。因此需把它str化，方便得到该处是否真的为空。
            r= editdistance.eval(tup[2], tup[3])
        except:     #else
            r= None
        return r
    pd_df= df.to_pandas()   # 要克隆数据，double内存
    col_ED= pd_df.apply(func, axis=1)
    pd_df['ed_val']= col_ED'''
    
    return df

def cal_edit_dist( tup ):
    if tup[3]:
        r= editdistance.eval(tup[2], tup[3])
    else:
        r= None
    return r

@utils.timer
def peerPfx_editdist(ldf_peerPfx: pl.LazyFrame, obj):
    '''- args: ldf_peerPfx：列9~12，行结合了历史peer-pfx表。
    - return: df: 列`[index: u32, time_bin: i16, ED: i8]`；行过滤了历史pp表，过滤了W条目。
    - 思路：之前规定需要计算ED的行为：`is_dup_ann | is_AWnA`，似乎有点复杂，现规定，在一组peer-pfx中，只要是宣告，就计算ED 。
    - 时间复杂度：O(N* k1* k2), 其中N是累加了历史peer-pfx的当前chunk条目，k1, k2是chunk中的最长path'''
    # 1、拿到最需要的4列 ['index', 'time_bin', 'path_raw_list', 'path_raw_shift' ]
    pre_df_ed= ( ldf_peerPfx.groupby(['peer_AS','dest_pref'])
        .agg([
            'index', 'time_bin', 'msg_type', 'tag_hist_cur', 
            pl.col('path_raw').str.split(' ').suffix('_list'),  # 原始path_list列无需在撤销行进行填充，毕竟计算ED前会删掉所有`撤销行`
            pl.col('path_raw').fill_null('forward').shift(1).str.split(' ').suffix('_shift')    # shift()不能作用于list，所以先在str上shift后转为list
        ])
        .explode(['index', 'time_bin', 'msg_type', 'tag_hist_cur', 'path_raw_list', 'path_raw_shift' ])  
        .filter( (pl.col('tag_hist_cur')== True) & (pl.col('msg_type')== 1 ) )  # 只保留属于当前chunk的、属于宣告的条目。
        .select( pl.exclude(['peer_AS','dest_pref','msg_type', 'tag_hist_cur']))    # 12列→ 聚合时得到8列→ 算ED只需4列
        #.sort('index')
    ).collect()
    
    # 2、计算编辑距离。不找歪门邪道了，直接apply引入自定义函数
    #tasks= pre_df_ed.rows()     # list[tuple[4个元素]]  debug: 把pl.df大表转为以行为元素的list，特别耗时
    col_ED= pre_df_ed.apply(cal_edit_dist)
    pre_df_ed['ED']= col_ED['apply']
    pre_df_ed= pre_df_ed.select([
        pl.col('index'),
        pl.col('time_bin'), 
        pl.col('ED')])      # debug: .cast(pl.Int8) 数据类型大变小（64→8）在有的情况下会报错

    '''# 3、把计算结果转为df
        #       如何把list[tuple(a,b,c)]转为df? 
        #           方法1失败：别想着遍历list，把a,b,c分别装入lista,listb,listc，特别耗时
        #           方法2失败：pl.DataFrame( nd.array( result )，因为所有列的dtype为object，无法后续操作，也无法转格式
        #           方法3：pd.DataFrame( nd.array( result )，再转pl.df
        ttt= time.time()
        while True: # 循环删除首行，直到首行没有None，这是为了保证pl读取时别少读一列。
            if result[0][2]== None:
                result.pop(0)
            else:
                break
        df_= pd.DataFrame( np.array( result ) )    # 4 ->3 fields
        df_ed= pl.from_pandas(df_)
        print(f'编辑距离中转换list和变成df, {time.time()-ttt}sec')'''
    # 3、result直接就是df，无需上述转换
      # debug: 因为我在模型中统一了'time_bin'列的数据类型为Int16。而从pd.df转过来的3列格式均为i64，
        #   因此在后继使用到time_bin时，要提前转换格式。
        #   但又为什么在这里全转换，因为peerPfx_editdist_num函数中需要用到pivot函数，那里要求被操作列必须是Int64

    # return前，同时把对应的df存入obj.midnode_res，被self.recur调用，防止重复计算
    obj.midnode_res[ peerPfx_editdist.__name__ ]= pre_df_ed

    return pre_df_ed.lazy()

def peerPfx_editdist_sim( df_peerPfx_editdist, obj ):
    # 如果是从obj.midnode_res拿df_ed的数据（df_ed在obj.recur()中被统一lazy化了），要先判断参数是否lazy
    if not isinstance( df_peerPfx_editdist, pl.LazyFrame ): 
        ldf_res= df_peerPfx_editdist.lazy() 
    else:
        ldf_res= df_peerPfx_editdist
    return ldf_res.with_column( pl.col('time_bin').cast(pl.Int16)).groupby('time_bin')
    
def peerPfx_editdist_num( df_peerPfx_editdist: pl.DataFrame, obj ):
    '''return: 行数：time_bin数；列数11：`[time_bin, ED_0~10]`'''
    # 先把参数非lazy化（pivot不能应用于lazyDF）
    if isinstance( df_peerPfx_editdist, pl.LazyFrame ):
        df_peerPfx_editdist= df_peerPfx_editdist.collect()

        # debug: 执行下行报错：`pyo3_runtime.PanicException: av Int16(7) not implemented`
        #   是因为df.pivot的被操作列('time_bin')的数据格式不能是Int16，只能是原汁原味的Int64
    res= (df_peerPfx_editdist
        .with_column(pl.col('time_bin').cast(pl.Int64))
        .pivot(values= 'index', index= 'time_bin', columns= 'ED', aggregate_fn= 'count')
        #.select( [pl.col('time_bin')]+[str(i) for i in range(11)] )    # .cast(pl.Int16)
        .fill_null("zero")      # pivot后会存在空值，需要填充0
    )
    # debug: 上面select语句可能会报错。因为可能存在0~10中的某个编辑距离值从来都没被计算到过。要手动填充0。如下
        # 先填充可能缺失的列
    target_res= res.select('time_bin')
    for i in range(11):
        if str(i) not in res.columns:
            target_res= target_res.with_column( pl.Series('ED_'+ str(i), [0]* res.height))
        else:
            target_res= target_res.with_column( res[str(i)].rename('ED_'+ str(i)))
    res= target_res.lazy().with_column(pl.col('time_bin').cast(pl.Int16)).groupby('time_bin')
    return res

'''def ratio(df, space):   # space8
    '' 对volume、path、peer-pfx三大类特征的二次加工。  耗时低。
    - 该函数不在特征树节点中。它属于传统特征提取中的`后处理`步骤。
    - 使用该函数条件：需要保证对`'volume', 'path', 'dynamic',  'editdistance'`4类进行全特征提取。且不能自定义需要提取的特征，要20个全部提取。
    - arg：df[time_bin行数， 84(此前算得的传统特征数)+1(time_bin)列数]
    - return： df[time_bin行数， 20+1 列数]
    ''
    columns=  ['ratio_A', 'ratio_W', 'ratio_1stOrder', 'ratio_IGP', 'ratio_EGP', 'ratio_ICMP', 'ratio_dup_ann', 'ratio_flap', 
                'ratio_NADA', 'ratio_imp_wd', 'ratio_new', 'ratio_longer_path', 'ratio_shorter_path',
                'ratio_imp_wd_dpath', 'ratio_imp_wd_spath', 'ratio_dup_wd']                   
    dividend= ['v_A', 'v_W', 'v_pfx_A_max', 'v_IGP', 'v_EGP', 'v_ICMP', 'is_dup_ann', 'is_flap',
                'is_NADA', 'is_imp_wd', 'is_new', 'is_longer_path', 'is_shorter_path',
                'is_imp_wd_dpath', 'is_imp_wd_spath', 'is_dup_wd']
    divisor = ['v_total']*2 + ['v_A']*11+ ['is_imp_wd']*2 +['v_W']
    exprs= []
    for dd, dr, col in zip(dividend, divisor, columns):
        exprs.append( (pl.col(dd)/pl.col(dr)).cast(pl.Float32).alias(col) )
    exprs+= [
        (pl.col('is_imp_wd')/ (pl.col('is_imp_wd')+ pl.col('v_W'))).cast(pl.Float32).alias('ratio_imp_wd2'),
        (pl.col('v_W')/ (pl.col('is_imp_wd')+ pl.col('v_W'))).cast(pl.Float32).alias('ratio_exp_wd'),
        (pl.col('is_longer_path')/ (pl.col('is_longer_path')+ pl.col('is_shorter_path'))).cast(pl.Float32).alias('ratio_longer_path2'),
        (pl.col('is_shorter_path')/ (pl.col('is_longer_path')+ pl.col('is_shorter_path'))).cast(pl.Float32).alias('ratio_shorter_path2'),
    ]

    df_ratio= (df.lazy().select(['time_bin']+ exprs)).collect()
    return df_ratio
'''
@utils.timer
def ratio( ratio_feats:list, featTree:dict, df_res_tradi:pl.DataFrame):
    '''- 根据df_res_tradi计算ratio特征，并并入到df_res_tradi中返回'''
    exprs= []
    for feat in ratio_feats:
        dividend = pl.col(featTree['ratio'][feat][0])   # 被除数
        divisor  = [pl.col(f) for f in featTree['ratio'][feat][1:] ]  # 除数list
        divisor_ = divisor[0] if len(divisor)==1 else (divisor[0]+ divisor[1])
        expr= ( dividend/ divisor_).cast(pl.Float32).alias( feat )
        exprs.append(expr)
    df_res_tradi= df_res_tradi.lazy().with_columns( exprs ).collect()

    return df_res_tradi




