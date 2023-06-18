'''
Description: 
version: 1.0
Author: JamesRay
Date: 2022-09-19 18:24:57
- LastEditors: Please set LastEditors
- LastEditTime: 2023-06-17 06:35:32
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

### 
def volume(ldf, obj):     
    return ldf
def vol_sim(ldf, obj):     
    return ldf.groupby("time_bin")

def vol_pfx(ldf, obj):    
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
def vol_pfx_peer(ldf_vol_pfx, obj):   
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

def vol_oriAS(ldf, obj):  
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
    ''''''
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
def get_rareAS_tag(grouped_df, obj):
    '''
    description:get the tags whether each AS belongs to rare ASes, and update the rare AS set. 
    '''
    value_005= obj.df_AS['counts'].quantile(0.05, 'nearest')
    if value_005:
        list_AS_rare=  obj.df_AS[ obj.df_AS['counts']<= value_005, 'AS_number'].to_series().to_list()
    else:
        list_AS_rare=['*']
    cur_tag= grouped_df.select( [pl.col('AS').cast(pl.Int64).is_in(list_AS_rare).alias('tag'),
        'index',
        ])   
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

def path(ldf, obj):   
    return ldf
def path_sim(ldf, obj):
    return ldf.filter(pl.col('msg_type')== 1).groupby('time_bin')

def path_AStotal(ldf:pl.LazyFrame, obj ): 
    '''得到当前df中所有的AS''' 
    ldf_all_AS= (ldf
        .filter(pl.col('msg_type')==1)   
        .select([
            #'index',    
            'time_bin',
            pl.col('path_unq').alias('AS')
        ])
        .drop_nulls()                    
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
def _path_AStotal_rare( ldf_path_AStotal:pl.LazyFrame, obj, space= 8 ):
    ''''''
    df_path_AStotal= ( ldf_path_AStotal 
        .with_column( (pl.col('index')//obj.ASslot).alias('grp_num'))    
        .collect())      

    grp_cnt= df_path_AStotal['grp_num'].max()+1    
    all_tag= pl.DataFrame( columns= [('tag', pl.Boolean), ('index', pl.UInt32)])
        
    for i in range( grp_cnt ):
        grpoued_df= ( df_path_AStotal.filter( pl.col('grp_num')== i) )
        cur_tag= get_rareAS_tag( grpoued_df, obj )        
        all_tag= pl.concat( [all_tag, cur_tag] )
    
    all_tag= all_tag.sort('index').rename({'index':'index_tag'})#['tag'] 
    df_path_AStotal= (df_path_AStotal.hstack( all_tag , in_place= False).groupby(['time_bin', 'index'])
        .agg([ pl.col('tag').sum().alias('AS_rare_num') ]) )     
    ldf= ( df_path_AStotal.lazy()
        .groupby(['time_bin', 'index'])
        .agg([ pl.col('AS_rare_num').sum() ])
        .groupby('time_bin')
    )

    return ldf
@utils.timer
def __path_AStotal_rare( ldf_path_AStotal:pl.LazyFrame, obj, space= 8 ):    
    ''''''
    df_path_AStotal= ( ldf_path_AStotal
        .groupby(['time_bin','AS'])
        .agg([
            pl.col('index').count().alias("counts")
        ])
        .sort(['time_bin','counts'], reverse=[False, True])
        .collect()
    )     
    num= df_path_AStotal['time_bin'].max()+1
    period= f'{num}i'
    a= ( df_path_AStotal
        .sort('time_bin')        
        .groupby_rolling(index_column='time_bin', period='5000i', by='AS')      
        .agg([
            pl.col('time_bin').last().alias('time_bin_N'),
            pl.col('counts').sum().alias('cur_sum_cnt')
        ])
        .select(['time_bin_N', 'AS', 'cur_sum_cnt'])
        .sort('time_bin_N')      
    )
    
    grp_cnt= df_path_AStotal['grp_num'].max()+1    
    all_tag= pl.DataFrame( columns= [('tag', pl.Boolean), ('index', pl.UInt32)])
        
    for i in range( grp_cnt ):
        grpoued_df= ( df_path_AStotal.filter( pl.col('grp_num')== i) )
        cur_tag= get_rareAS_tag( grpoued_df, obj )
        
        all_tag= pl.concat( [all_tag, cur_tag] )
    
    all_tag= all_tag.sort('index').rename({'index':'index_tag'})#['tag']  
    df_path_AStotal= (df_path_AStotal.hstack( all_tag , in_place= False).groupby(['time_bin', 'index'])
        .agg([ pl.col('tag').sum().alias('AS_rare_num') ]) )   
    ldf= ( df_path_AStotal.lazy()
        .groupby(['time_bin', 'index'])
        .agg([ pl.col('AS_rare_num').sum() ])
        .groupby('time_bin')
    )

    return ldf
@utils.timer
def path_AStotal_rare( ldf_path_AStotal:pl.LazyFrame, obj, space= 8 ):    
    '''获取稀有AS'''
    df_path_AStotal= ( ldf_path_AStotal
        .groupby(['time_bin','AS'])
        .agg([
            pl.col('index').count().alias("counts")
        ])
        .sort('time_bin')
        .collect()
    )   
    upds= ldf_path_AStotal.groupby('time_bin').agg(pl.col('index').unique().count().alias('upds_num')).collect()

    rareAS_num=[]
    slots= df_path_AStotal['time_bin'].max()+1
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
    ''''''    
    ldf_cur= ldf.select( pl.exclude([ 'path_unq', 'origin'])) \
        .with_column( pl.col('peer_AS').cast(pl.Boolean).alias('tag_hist_cur') )
    obj.df_peer_pfx= obj.df_peer_pfx.select(ldf_cur.columns)
    df_all= pl.concat([ obj.df_peer_pfx, ldf_cur.collect() ])   
    
    obj.df_peer_pfx= ( df_all.lazy()
        .groupby(['peer_AS', 'dest_pref', 'msg_type'])
        .tail(1)
        .with_column((pl.col('tag_hist_cur')*0).cast(pl.Boolean))    
    ).collect()
    obj.midnode_res[ peerPfx.__name__ ]= df_all

    return df_all.lazy()
    
def peerPfx_dynamic(ldf_peerPfx: pl.LazyFrame, obj):
    ''''''
    feats= obj.feats_dict[ ('peerPfx', 'peerPfx_dynamic') ] 
    candidate= {
        ("is_new",):  
            (pl.col('tag_hist_cur').all().alias('has_new'), ),   
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
        #("is_MOAS",):    (pl.col('origin_AS').alias('is_MOAS'), 'is_MOAS'),     

        ("is_imp_wd","is_dup","is_flap","is_NADA","is_imp_wd_spath","is_imp_wd_dpath"):    
            (pl.when(pl.col('msg_type')== 0).then(None).otherwise(pl.col('hash_attr')).fill_null('forward').diff().alias('hash_attr_diff'), 'hash_attr_diff'),
        }
    agg_apend, explode_apend = [], []
    for k, v in candidate.items():
        if (set(k) & set(feats)):
            agg_apend.append(v[0])
            try:     explode_apend.append(v[1])
            except:  pass     
    agg_list= [pl.col('index'),
            pl.col('time_bin'),
            pl.col('tag_hist_cur').alias('belong_cur'),  
            ]+ agg_apend
    explode_list= ['index','time_bin', 'belong_cur']+ explode_apend
     
    '''modify_list= []
    if 'path_len_diff' in explode_list: modify_list.append( pl.col('path_len_diff').cast(pl.Int8) )
    if 'path_unq_len_diff' in explode_list: modify_list.append( pl.col('path_unq_len_diff').cast(pl.Int8) )'''
         
    #if 'is_MOAS' in explode_list: modify_list.append( pl.col('is_MOAS').diff().cast(pl.Boolean).cast(pl.Int8) )
    
    ldf_13= (ldf_peerPfx.groupby(['peer_AS','dest_pref']) 
        .agg(agg_list)
        .explode(explode_list)   
        .filter( pl.col('belong_cur')== True )   
        #.with_columns(modify_list)  
    )

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
    
    modify_list3= []
    if 'is_new' in feats: 
        modify_list3.append( pl.col('is_new').cast(pl.Boolean) )
    if len(set(['is_imp_wd', 'is_imp_wd_spath', 'is_imp_wd_dpath']) & set(feats)): 
        modify_list3.append( ((pl.col('is_dup_ann')== 1) & (pl.col('hash_attr_diff')!= 0)).alias('is_imp_wd') )
    ldf_res17= ( ldf_13.join( 
                    ldf_3.select(['index']+ explode2_apend),     
                    on='index')
                .with_columns( modify_list3 )
                )
    
    return ldf_res17.groupby('time_bin')

def peerPfx_relateHijack( ldf_peerPfx: pl.LazyFrame, obj ):
    '''- args: ldf_peerPfx：列12，行结合了历史peer-pfx表。
    - return: 一个新的ldf(最多5+4+4=13列)[ index, time_bin, 'tag_hist_cur', peer, dest_pref]+ ['path_loc0(i.e. is_MOAS)', 'path_loc1/2/3' ] + [type_0,1,2,3]
    - 如何使用：path_loc0123即ARTEMIS模型中的劫持类型Type0,1,2,3。注意，在判断loc1的两AS是否相同时，前提是loc0的AS必须相同。以此类推。
    '''
    feats= obj.feats_dict[ ('peerPfx', 'peerPfx_relateHijack') ]     
     
    sel_apend= [pl.col('origin_AS').alias('path_loc0'),  
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
     
    agg_apend= []
    locNms= [nm for nm in ldf_locAS.columns if 'path_loc' in nm ]
    for colNm in locNms:
        agg_apend.append( pl.col(colNm ).diff().cast(pl.Boolean) )     ###### main
    ldf_locAS= ldf_locAS.groupby(['peer_AS', 'dest_pref']) \
        .agg( [ 'index', 'time_bin', 'tag_hist_cur' ]+ agg_apend ) \
        .explode( [ 'index', 'time_bin', 'tag_hist_cur' ]+ locNms ) \
        .filter( pl.col('tag_hist_cur')== True )     
        
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

    if 'path_loc0' in locNms:            
        indexs= (ldf_locAS.filter(pl.col('path_loc0')== True)
            .select(pl.col('index')).collect()
            .to_series()
            .to_list())
         
        content= ldf_peerPfx.collect()[indexs].to_csv(has_header= False)
        with open(obj.path_df_MOAS, 'a') as f:
            f.write(content)
        
    return ldf_locAS.groupby('time_bin')     

def _cal_edit_distance(res_lis, lis:list):
    ''' subfunction of multithread in `peerPfx_editdist`, in `lis[(idx, time_bin, p1, p2), (), ...]` the p1 and p2 are type of `list`. 
    '''
    sub_res= []
    for idx, time_bin, p1, p2 in lis:
        if p2:   
            sub_res.append( (idx, time_bin, editdistance.eval(p1, p2)) )
        else:
            sub_res.append( (idx, time_bin, None) )
    res_lis.append(sub_res)
    ##
@utils.timer
def _cal_ED(df:pl.DataFrame, space):
    ''''''    
    def func( tup ):
        if tup[3]:
            r= editdistance.eval(tup[2], tup[3])
        else:
            r= None
        return r
    col_ED= df.apply(func)
    df['ED']= col_ED['apply']
    df= df.select(['index', 'time_bin', 'ED'])    
    return df

def cal_edit_dist( tup ):
    if tup[3]:
        r= editdistance.eval(tup[2], tup[3])
    else:
        r= None
    return r

@utils.timer
def peerPfx_editdist(ldf_peerPfx: pl.LazyFrame, obj):
    ''''''
    pre_df_ed= ( ldf_peerPfx.groupby(['peer_AS','dest_pref'])
        .agg([
            'index', 'time_bin', 'msg_type', 'tag_hist_cur', 
            pl.col('path_raw').str.split(' ').suffix('_list'),   
            pl.col('path_raw').fill_null('forward').shift(1).str.split(' ').suffix('_shift')     
        ])
        .explode(['index', 'time_bin', 'msg_type', 'tag_hist_cur', 'path_raw_list', 'path_raw_shift' ])  
        .filter( (pl.col('tag_hist_cur')== True) & (pl.col('msg_type')== 1 ) )   
        .select( pl.exclude(['peer_AS','dest_pref','msg_type', 'tag_hist_cur']))     
        #.sort('index')
    ).collect()
    
    #tasks= pre_df_ed.rows()      
    col_ED= pre_df_ed.apply(cal_edit_dist)
    pre_df_ed['ED']= col_ED['apply']
    pre_df_ed= pre_df_ed.select([
        pl.col('index'),
        pl.col('time_bin'), 
        pl.col('ED')])       

    obj.midnode_res[ peerPfx_editdist.__name__ ]= pre_df_ed

    return pre_df_ed.lazy()

def peerPfx_editdist_sim( df_peerPfx_editdist, obj ):
    if not isinstance( df_peerPfx_editdist, pl.LazyFrame ): 
        ldf_res= df_peerPfx_editdist.lazy() 
    else:
        ldf_res= df_peerPfx_editdist
    return ldf_res.with_column( pl.col('time_bin').cast(pl.Int16)).groupby('time_bin')
    
def peerPfx_editdist_num( df_peerPfx_editdist: pl.DataFrame, obj ):
    
    if isinstance( df_peerPfx_editdist, pl.LazyFrame ):
        df_peerPfx_editdist= df_peerPfx_editdist.collect()

    res= (df_peerPfx_editdist
        .with_column(pl.col('time_bin').cast(pl.Int64))
        .pivot(values= 'index', index= 'time_bin', columns= 'ED', aggregate_fn= 'count')
        #.select( [pl.col('time_bin')]+[str(i) for i in range(11)] )     
        .fill_null("zero")       
    )
    
    target_res= res.select('time_bin')
    for i in range(11):
        if str(i) not in res.columns:
            target_res= target_res.with_column( pl.Series('ED_'+ str(i), [0]* res.height))
        else:
            target_res= target_res.with_column( res[str(i)].rename('ED_'+ str(i)))
    res= target_res.lazy().with_column(pl.col('time_bin').cast(pl.Int16)).groupby('time_bin')
    return res

@utils.timer
def ratio( ratio_feats:list, featTree:dict, df_res_tradi:pl.DataFrame):
    ''''''
    exprs= []
    for feat in ratio_feats:
        dividend = pl.col(featTree['ratio'][feat][0])    
        divisor  = [pl.col(f) for f in featTree['ratio'][feat][1:] ]   
        divisor_ = divisor[0] if len(divisor)==1 else (divisor[0]+ divisor[1])
        expr= ( dividend/ divisor_).cast(pl.Float32).alias( feat )
        exprs.append(expr)
    df_res_tradi= df_res_tradi.lazy().with_columns( exprs ).collect()

    return df_res_tradi




