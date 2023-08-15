#! /usr/bin/env python
# coding=utf-8
'''
- Description: BGP异常检测中常用的辅助函数
- version: 1.0
- Author: JamesRay
- Date: 2023-02-06 13:10:54 
- LastEditTime: 2023-08-15 10:21:44
'''
import os, json, time, re, glob, jsonpath
import requests
from bs4 import BeautifulSoup
from functools import partial
from typing import Union, List, Dict

import multiprocessing, subprocess
from multiprocessing import Pool
import tqdm
from datetime import datetime, timedelta, timezone
import networkx as nx
import pandas as pd
import polars as pl
import numpy  as np
from scipy.stats import kurtosis
from scipy.fft import fft, ifft
import statistics
import pycountry

from sklearn.feature_selection import VarianceThreshold, chi2, SelectKBest, mutual_info_classif 
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.ensemble import RandomForestClassifier as RFC
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.dates import DateFormatter, AutoDateLocator
from matplotlib_venn import venn2

from fastFET import utils
from fastFET.RIPEStatAPI import ripeAPI
from fastFET.featGraph import graphInterAS
logger= utils.logger

#######################
#  地理位置的研究：peers, AS, prefix, IP
#######################
from geopy.geocoders import Bing
import geoip2.database

class CommonTool(object):
    '''分析BGP事件时常用工具集'''

    @staticmethod
    def ip2coord(IPs:list):
        '''
        - description: 获取ip坐标。
        - 首选方法：利用`https://ipinfo.io/{ip}?token={my_token}`接口获取。
            - 优点: 精确; 缺点: 可能收费, 量大时很慢
        - 次选方法：利用`geoip2`库得到坐标和城市，若得不到城市，继续调用`Bing map API`获取城市。
            - 优点：快；缺点：不保证精度
            - 前提: 保证`geoLite2-City.mmdb`文件在指定目录，否则执行以下命令进行下载：
                `wget https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=ClwnOBc8c31uvck8&suffix=tar.gz ; \
                    tar -zxvf geoip* ; \
                    mv GeoLite*/GeoLite2-City.mmdb geoLite2-City.mmdb ; \
                    rm -r GeoLite* geoip* `
            - 若授权码失效, 进入`https://www.maxmind.com/en/accounts/current/license-key`重新获取。
        - param  {list[str]}: IPs
        - return {dict}: {ip: [latitude, longitude, cityName], ...}   
        '''    
        res= {}
        count=0
        test= requests.get(f'https://ipinfo.io/8.8.8.8?token=e9ae5d659e785f').json()
        if 'city' in test.keys():
            for ip in IPs:
                curJson= requests.get(f'https://ipinfo.io/{ip}?token=e9ae5d659e785f').json()
                coord= curJson['loc'].split(',')
                city = f"{curJson['city']}, {curJson['country']}"
                res[ip]= [ coord[0], coord[1], city ]

                logger.info(f'done {count} ...')
                count+=1
            return res
        else:
            path_db= 'geoLite2-City.mmdb'
            try:
                assert os.path.exists(path_db) == True
            except:
                raise RuntimeError(f'there is no `{path_db}`, please execute command as follow:\n \
                    wget https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=ClwnOBc8c31uvck8&suffix=tar.gz ;tar -zxvf geoip* ; mv GeoLite*/GeoLite2-City.mmdb geoLite2-City.mmdb ; rm -r GeoLite* geoip* '
                )
                
            reader = geoip2.database.Reader(path_db)
            geolocator = Bing(api_key='Ag7S7BV4AkTdlUzzm_pgSZbQ9c_FBf9IbvSnSlui2x-kE6h-jnYKlT7EHYzRfxjC')
            coord_city_dic= {}
            for ip in IPs:
                response = reader.city(ip)
                latitude = response.location.latitude
                longitude = response.location.longitude

                cityName = response.city.name
                if cityName!= None:
                    cityName+= ','+ response.country.name
                else:     
                    if (latitude, longitude) not in coord_city_dic:
                        location = geolocator.reverse((latitude, longitude))
                        cityName= ' '
                        if location:
                            try:
                                cityName = location.raw['address']['adminDistrict2']+ ', '+ location.raw['address']['countryRegion']
                            except:
                                cityName = location.address
                        time.sleep(0.15)     
                        coord_city_dic[(latitude, longitude)]= cityName
                    else:
                        cityName= coord_city_dic[(latitude, longitude)]
                res[ip]= [latitude, longitude, cityName]
                    
                logger.info(f'done: {count} coord2city')
                count+=1
            reader.close()
            return res

    @staticmethod
    def cal_geo_dist(lat1, lon1, lat2, lon2):
        '''
        - description: 计算两坐标地理距离
        '''
        import math
        R = 6371
        dLat = math.radians(lat2 - lat1)
        dLon = math.radians(lon2 - lon1)
        lat1 = math.radians(lat1)
        lat2 = math.radians(lat2)
        a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        distance = R * c
        return distance


class PeersData(object):
    '''收集RIPE-NCC和RouteViews项目中的peers信息'''

    @staticmethod
    def fromRV():
        '''
        - description: 采集来自RV的原始数据, 删了peerIP-v6部分
        - 注意：
            - `http://www.routeviews.org/peers/peering-status.html`仅展示32个采集点
                - 包括`route-view, route-views6`
            - `http://archive.routeviews.org/`数据库展示了37个采集点
                - 不包括`route-view`
                - 包括`route-views6`和6个新增点`jinx,saopaulo,seix,mwix,bdix,ny`
            - 当前函数仅收录31个采集点，因为'route-views6'只含peerIP-v6
        - return {dict}: `{ collector: {ip1: {'asn':, 'ip':, 'v4_pfx_cnt': }, ip2: {},...}, ...}`
        - return {list}: `[ip, ...]`
        '''
        url= "http://www.routeviews.org/peers/peering-status.html"
        resRV={}
        respon= requests.get(url).text
        if not respon:
            logger.info('* * * Please crawl \'%s\' again.' % url)
            return {}, []
        rawList= re.findall('route-view.+', respon)
        
        IPs= set()
        for row in rawList:                       
            rowlist= row.split()
            if ':' in rowlist[2]:   
                continue
            
            collector= re.search('.*(?=.routeviews.org)', rowlist[0]).group()
            if collector not in resRV.keys():
                resRV[collector]= {}
                #logger.info('start collecting with %s ~' % collector)

            curpeer= {}
            curpeer['asn']= rowlist[1]
            curpeer['ip'] = rowlist[2]
            IPs.add( rowlist[2] )
            curpeer['v4_prefix_count']= rowlist[3]
            resRV[collector][ rowlist[2] ]= curpeer

        return resRV, list(IPs)

    @staticmethod
    def fromRRC():
        '''
        - description: 采集来自RRC的原始数据, 删了peerIP-v6部分
        - return {dict}: `{ collector: {ip1: {'asn':, 'ip':, 'v4_pfx_cnt': }, ip2: {},...}, ...}`
        - return {list}: `[ip, ...]`
        '''
        url= "https://stat.ripe.net/data/ris-peers/data.json?query_time=2023-02-22T00:00"
        data= requests.get(url).json()['data']['peers']
        IPs= set()
        data_new= {}
        for rrc, peer_lis in data.items():
            peer_lis_new= {}
            for peer in peer_lis:
                if ":" not in peer['ip']:
                    peer.pop('v6_prefix_count')
                    peer_lis_new[ peer['ip'] ]= peer
                    IPs.add( peer['ip'])
            data_new[rrc]= peer_lis_new
        return data_new, list(IPs)

    @staticmethod
    def get_peers_info(path_out= 'peers_info.json'):
        '''
        - description: 获取所有peers的信息, 结果存储在`./peers_info.json`
        - return {dict}: `{ 'rou': [{'asn', 'ip', 'v4_prefix_count', 'longitude', 'latitude', 'collector'}, {}, ...],
                            'rrc': [...] }`
        '''
        rv_info, rv_ips= PeersData.fromRV()
        rc_info, rc_ips= PeersData.fromRRC()

        ip_map= CommonTool.ip2coord(set(rv_ips+ rc_ips))

        res={}
        for data in (rv_info, rc_info):
            cur_res= []
            for rrc, rrc_dic in data.items():
                for ip, peer_dic in rrc_dic.items():
                    peer_dic['latitude']=  ip_map[ip][0]
                    peer_dic['longitude']= ip_map[ip][1]
                    peer_dic['cityName'] = ip_map[ip][2] if ip_map[ip][2]!= None else ' '
                    peer_dic['collector']= rrc
                    cur_res.append(peer_dic)

            colors = [
                '#1F75FE', '#057DCD', '#3D85C6', '#0071C5', '#4B86B4',
                '#17A589', '#52BE80', '#2ECC71', '#00B16A', '#27AE60',
                '#E74C3C', '#FF5733', '#C0392B', '#FF7F50', '#D35400',
                '#9B59B6', '#8E44AD', '#6A5ACD', '#7D3C98', '#BF55EC',
                '#E67E22', '#FFA500', '#FF8C00', '#FF6347', '#FF4500',
                '#F1C40F', '#FFD700', '#F0E68C', '#FFA07A', '#FFB900',
                '#555555', '#BDC3C7', '#A9A9A9', '#D3D3D3', '#808080'
            ]
            
            collector2idx= { val: idx for idx, val in enumerate( list(data.keys())) }
            for peer in cur_res:
                peer['color']= colors[collector2idx[ peer['collector'] ]]

            key= list(data.keys())[0][:3]
            res[ key]= cur_res

        with open(path_out, 'w') as f:
            json.dump(res, f)
        logger.info( f"rrc: {len( res['rrc'])} peers.\nrou: {len( res['rou'])} peers.\n### all peers info stored at `{path_out}`")

        return res

    @staticmethod
    def get_rrc_info(path_in= './peers_info.json', path_out= 'peers_info_about_collector.csv'):
        '''
        - description: 获取每个rrc的peers数量、城市列表
        '''
        if not os.path.exists(path_in):
            PeersData.get_peers_info()
        with open(path_in) as f:
            datas= json.load(f)
        datas= datas['rou']+ datas['rrc']
        
        rrc_city= {}
        rrc_count= []
        for dic in datas:
            rrc_count.append( dic['collector'])

            if not dic['collector'] in rrc_city.keys():
                rrc_city[dic['collector']]= [ dic['cityName'] ]
            else:
                if dic['cityName']!= ' ':
                    rrc_city[dic['collector']].append( dic['cityName'])

        peer_num_in_RRC= pd.value_counts(rrc_count).sort_index().to_frame()
        
        for rrc, city_lis in rrc_city.items():
            rrc_city[rrc]= [str(set(city_lis))]
        rrc_city_pd= pd.DataFrame(rrc_city).T
        
        res= pd.concat([peer_num_in_RRC, rrc_city_pd], axis=1)
        res.to_csv(path_out, header=['peer_num', 'cities'])

    @staticmethod
    def prepare_peer_worldMap(path_in= './peers_info.json', path_out= './peers_info_for_drawing.json'):
        '''
        - description: 调整peers_info数据格式，用于eChart作图。
        - return {*} :  [{value: [经度, 纬度], itemStyle: { normal: { color: 颜色}}, 其他key-value}, {}, ... ]
        '''
        if not os.path.exists(path_in):
            PeersData.get_peers_info()
        with open(path_in) as f:
            data_all= json.load(f)
        for project, data in data_all.items():
            data_new=[]
            for p in data:
                #if p['collector']== 'rrc00':
                p['value']= [p['longitude'], p['latitude']]
                p['itemStyle']= { 'normal': { 'color': p['color']}}
                for k in ['longitude', 'latitude', 'color' ]:
                    p.pop(k)
                data_new.append(p) 
            data_all[ project ]= data_new

        with open(path_out,'w') as f:
            json.dump(data_all, f)

    @staticmethod
    def peerAS2country(project= 'rrc', p= './peers_info.json'):
        '''- 获取每个国家的peers数量'''
        if not os.path.exists(p):
            PeersData.get_rrc_info()
        with open(p) as f:
            data= json.load(f)[project]
        aa= pl.DataFrame(data)[['asn', 'cityName']]
        aa['cityName']= aa['cityName'].str.split(', ').arr.last()
        a_=aa.groupby('asn').agg(
            pl.col('cityName').first().apply(PeersData._get_country_name).alias('country')
        ).groupby('country').agg(
            pl.col('asn').count()
        ).sort('asn',reverse=True)
        return a_
    
    @staticmethod
    def _get_country_name(code):
        '''-input国家代码, output国家名'''
        try:
            country = pycountry.countries.get(alpha_2=code)
            return country.name
        except:
            return None

    @staticmethod
    def get_peers_distance_DF(path= './peers_info.json', df_out_path= './peers_distance_each_other.csv'):
        '''
        - description: 计算peers两两之间的地理距离
        - `peers_info.json`的数据源: peerIP来自`http://www.routeviews.org/peers/peering-status.html`和`https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/archive/ris-raw-data`
                                    坐标来自`https://ipinfo.io/{ip}`
        '''
        with open(path) as f:
            data= json.load(f)
        coords= {'asn':[], 'latitude':[], 'longitude':[]}
        for dic in data['rou']+data['rrc']:
            coords['asn'].append( f"AS{dic['asn']}-{dic['collector'][:3]}")
            coords['latitude'].append( float(dic['latitude']))
            coords['longitude'].append( float(dic['longitude']))

        peer_num  = len(coords['latitude'])
        distances = np.zeros((peer_num, peer_num))
        for i in range(peer_num):
            for j in range(i, len(coords['latitude'])):
                lat1, lon1 = coords['latitude'][i], coords['longitude'][i]
                lat2, lon2 = coords['latitude'][j], coords['longitude'][j]
                d = CommonTool.cal_geo_dist(lat1, lon1, lat2, lon2)
                distances[i][j] = d
                distances[j][i] = d

        df = pd.DataFrame(distances, index=coords['asn'], columns=coords['asn'])
        df.to_csv(df_out_path, index=True)
        return df


#######################
# 分析MRT原始数据相关接口
#######################

class MRTfileHandler():
    '''用于处理RIPE和RouteViews项目中的MRT格式数据'''

    @staticmethod
    def collector_list(project=None):
        '''
        - description: 获取采集点列表。
        - args-> project {*}: one of `RIPE, RouteViews` or None
        - return {list}
        '''
        
        collector_list_rrc= [f'rrc{i:02d}' for i in range(27)]
        collector_list_rrc.pop(17)
        
        collector_list_rou= ["route-views.ny","route-views2","route-views.amsix","route-views.chicago","route-views.chile",
        "route-views.eqix","route-views.flix","route-views.fortaleza","route-views.gixa","route-views.gorex","route-views.isc",
        "route-views.jinx","route-views.kixp","route-views.linx","route-views.napafrica","route-views.nwax","route-views.perth",
        "route-views.phoix","route-views.rio","route-views.saopaulo","route-views.sfmix","route-views.sg","route-views.soxrs",
        "route-views.sydney","route-views.telxatl","route-views.wide","route-views2.saopaulo","route-views3","route-views4",
        "route-views5","route-views6","route-views.peru","route-views.seix","route-views.mwix","route-views.bdix",
        "route-views.bknix","route-views.uaeix","route-views"]
        
        if project== 'RIPE':
            return collector_list_rrc
        elif project== 'RouteViews':
            return collector_list_rou
        else:
            return collector_list_rrc+ collector_list_rou
    
    @staticmethod
    def get_download_url(type:str, monitor:str, tarTime):
        '''
        - description: 获取MRT文件下载链接
        - args-> type {str}: any of `updates, rib, rib., ribs, bview, bview.`
        - args-> monitor {str}: 
        - args-> tarTime {str| datetime}: like `20210412.0800`
        - return {str}
        '''
        if isinstance(tarTime, datetime):
            tarTime= tarTime.strftime('%Y%m%d.%H%M')
        month= f'{tarTime[:4]}.{tarTime[4:6]}'
        type= type if type== 'updates' else 'ribs'
        dic= {
            'rrc':{
                'updates': f"https://data.ris.ripe.net/{monitor}/{month}/updates.{tarTime}.gz",
                'ribs'   : f"https://data.ris.ripe.net/{monitor}/{month}/bview.{tarTime}.gz"
            },
            'rou':{
                'updates': f"http://archive.routeviews.org/{monitor}/bgpdata/{month}/UPDATES/updates.{tarTime}.bz2",
                'ribs'   : f"http://archive.routeviews.org/{monitor}/bgpdata/{month}/RIBS/rib.{tarTime}.bz2"
            }
        }
        return dic[ monitor[:3]][type]

    @staticmethod
    def _convert_file_size(size_str):
        '''
        - description: 将`5M`转换为`5`, 单位为MB或Byte
        '''
        if not size_str:
            return 0.0
        elif size_str.endswith('M'):
            return float(size_str[:-1])
        elif size_str.endswith('K'):
            return float(size_str[:-1]) / 1000
        elif size_str[-1] == 'G':
            return float(size_str[:-1])*1000
        else:
            return float(size_str)/10**6
    
    @staticmethod
    def _get_month_list(time_start='20210416', time_end='20210718'):
        '''- 获取指定时间段内的月份列表，如：['2021.04', '2021.05',...]'''
        t1= time.time()
        # Convert the start and end dates to datetime objects
        start_date = datetime.strptime(time_start[:8], '%Y%m%d')
        end_date = datetime.strptime(time_end[:8], '%Y%m%d')

        # Generate a list of months between the start and end dates
        month_list = []
        while start_date <= end_date:
            month = start_date.strftime('%Y.%m')
            if month not in month_list:
                month_list.append(month)
            start_date += timedelta(days=1)
        print(f"time cost for getting `months_list`: {(time.time()-t1):.1f} s")
        return month_list

    @staticmethod
    def _get_collector_file_size(collector, month_list=None) -> dict:
        '''
        - description: 从一个collector获取指定`月份`内的`MRT file size`的变化
        - args-> collector {str}: 
        - args-> month_list {list}: from `_get_month_list()`
        - return {dict}: like: {collector: {'20210401.0000': '6M', '20210401.0005': '5M',...} }, value可为空
        '''
        if not month_list:
            collector, month_list= collector
            month_list= [month_list]
            
        diveded_map={'rrc':5, 'rou':15}
        pattern_dir={
            'rrc': r'href="updates\.(\d{8}\.\d+)\.gz.*:\d\d\s*(\d+\.?\d*[MKG]?)\s*', 
            'rou': r'updates\.(\d{8}\.\d+)\.bz2.*"right">\s*(\d+\.?\d*[MKG]?)\s*'
        }
        res_dic= {}

        for month in month_list:
            if 'rrc' in collector:
                url= f'https://data.ris.ripe.net/{collector}/{month}/'
            else:
                if collector== 'route-views2':
                    url= f'http://archive.routeviews.org/bgpdata/{month}/UPDATES/'
                else:
                    url= f'http://archive.routeviews.org/{collector}/bgpdata/{month}/UPDATES/'

            try:
                respon= requests.get(url, timeout= 4)
                if respon.status_code==200:
                    pageInfo= respon.text
                else:
                    print(f'请求失败: {collector}--> {url}')
            
                matches= re.findall(pattern_dir[collector[:3]], pageInfo)
                for time, size in matches:
                    if int(time[-2:])% diveded_map[collector[:3]] ==0:
                        res_dic[time]= size
            except Exception as e:
                print(f'请求异常：{collector}--> {e}')

        # print(f'* * done: {collector}')
        return {collector: res_dic}

    @staticmethod
    def _get_collectors_file_size(time_start='20211004.1200', time_end=None, project=None, custom_rrcs= None):
        '''
        - description: 并行使用`_get_collector_file_size`。返回值排除了空数据的采集点。
        - args-> project {str}: either-or of 'RouteViews' and 'RIPE'
        - args-> custom_rrcs {list}: 自定义采集点列表
        - return {dict}: `{'rrc': DF(collectors* dates), 'rou': ~same~}`
        '''
        if time_end==None:
            time_end= time_start
        proj_map  = {'RouteViews': MRTfileHandler.collector_list('RouteViews'), 'RIPE': MRTfileHandler.collector_list('RIPE')}
        if custom_rrcs:
            collector_list= custom_rrcs
        else:
            collector_list= proj_map[project] if project else proj_map['RIPE']+ proj_map['RouteViews']
        month_list= MRTfileHandler._get_month_list(time_start, time_end)
        
        from tqdm import tqdm
         
        with Pool(processes=50) as pool:
            total = len(collector_list)* len(month_list)
            candi_list= [(c, m) for c in collector_list for m in month_list ]
            desc = 'Processing collector files'
            results = list(tqdm(pool.imap(MRTfileHandler._get_collector_file_size, candi_list), total=total, desc=desc))

        res= {}
        res_rrc= {}
        res_rou= {}
        for r in results:
            rrc_name= list(r.keys())[0]
            if 'rrc' in rrc_name:
                if rrc_name in list(res_rrc.keys()):
                    res_rrc[rrc_name].update( r[rrc_name])
                else:
                    res_rrc.update(r)
            else:
                if rrc_name in list(res_rou.keys()):
                    res_rou[rrc_name].update( r[rrc_name])
                else:
                    res_rou.update(r)
                    
        for dic in [res_rou, res_rrc]:
            if dic=={}:
                continue
            
            df= pd.DataFrame(dic )#.astype(str)
            df.sort_index()
            df.index = pd.to_datetime(df.index, format='%Y%m%d.%H%M')
            a= datetime.strptime(time_start, '%Y%m%d.%H%M')
            b= datetime.strptime(time_end, '%Y%m%d.%H%M')
            df = (df.loc[(df.index >= a) & (df.index <= b)]
                    .fillna('0')
                    .applymap(lambda x: MRTfileHandler._convert_file_size(x))
                    )
            
            empty_collectors=[]
            low_var_collectors= []
            for coll in df.columns:
                # 筛掉404的采集点
                if df[coll].sum()==0:
                    empty_collectors.append(coll)
                    df.drop(coll, axis=1, inplace=True)
                # 过滤低方差的采集点
                elif df[coll].var()<= 0.01:
                    low_var_collectors.append(coll)
                    df.drop(coll, axis=1, inplace=True)
            print(f'`{empty_collectors=}`')
            print(f'`{low_var_collectors=}`')
                            
            res[list(dic.keys())[0][:3]]= df
            
        return res

    @staticmethod
    def draw_collectors_file_size(eventName='', 
        time_start='20211004.1200', 
        time_end: str=None, 
        event_period: tuple=None,
        project: str= None,
        custom_rrcs: list= None
        ):
        '''
        - description: 画图对比各采集点的`file_size`走势
        - args-> data {*}: all of values returned by `_get_collectors_file_size()`
        - args-> event_period {`('20211004.1200', '20211004.1200')`}: 当需要在图中作异常区间阴影时使用
        - return {*}
        '''
        if time_end== None:
            time_end= time_start
        #if not data:
        data= MRTfileHandler._get_collectors_file_size(time_start, time_end,project= project, custom_rrcs= custom_rrcs)
        
        for project, df in data.items():
            #size_map= {'RIPE': 23, 'RouteViews': 32}
            print(f'{df.shape=}')
            title= f"{time_start[:8]}_{eventName}_{project}.jpg"
            utils.makePath(f'plot_file_sizes/{title}')
            ax= df.plot( # y=df.columns[3],
                    figsize=(10, df.shape[1]),
                    subplots=True 
            )
            
            if event_period != None:
                sat= datetime.strptime(event_period[0], '%Y%m%d.%H%M')
                end= datetime.strptime(event_period[1], '%Y%m%d.%H%M')
                for a in ax:
                    a.axvspan(sat, end, color='y', alpha= 0.35)

            plt.savefig(title)
            os.system(f"mv {title} plot_file_sizes/{title}")
            print(f'plot_path= ./plot_file_sizes/{title}')
        return data

    @staticmethod
    def select_collector_based_kurt(time_start='20211004.1200', time_end=None, data=None):
        '''
        - description: 根据峰度获得所有采集点排名，默认为RIPE和RouteViews的总排名。
        - 注：起止时间范围越宽，采集点的峰度分数越有代表性。
        - 注：对于双峰数据(如泄露事件的异常形成与恢复过程),峰度排名不再有效，仍需画图观察
        - return {*}
        '''
        if time_end== None:
            time_end= time_start
        if not data:
            data= MRTfileHandler._get_collectors_file_size(time_start, time_end)
        res= pd.Series()
        for project, df in data.items():
            kurt = df.apply(kurtosis)
            score = (10 * (kurt - kurt.min()) / (kurt.max() - kurt.min())).sort_values(ascending=False)
            res= pd.concat([res, score])
        res= res.sort_values(ascending=False)
        return res, data

class DownloadParseFiles():
    '''- 简单场景下的MRT文件的下载和解析'''
    def __init__(self, mode= 'a', time_str= '20230228.0000', time_end= None, coll= None, target_dir= './raw_data/', core_num= 60) -> None:
        ''' 
        - args-> mode {'all'/'a'}: 
            - `all`: 所有采集点模式，用于下载并解析`指定时刻time_str`的`所有采集点`的(rib)表；
            - `a`  : 单一采集点模式，用于下载并解析`指定时间段time_str ~ time_end`的`指定采集点coll`的(updates)表
        - args-> time_str {*}: 
        - args-> time_end {*}: 当mode='a'时有效
        - args-> coll {*}: 当mode='a'时有效
        - args-> target_dir {*}: 
        - args-> core_num {*}: 默认60核
        '''
        self.mode= mode
        self.time_str= time_str
        self.time_end= time_end
        self.coll= coll
        self.core_num= core_num
        self.p_down= utils.makePath(f'{target_dir}/raw/')
        os.system(f'rm -r {target_dir}/raw/*')
        self.p_pars= utils.makePath(f'{target_dir}/parsed/')
        os.system(f'rm -r {target_dir}/parsed/*')
        print(f"will download and parse at: {target_dir}")

    def _get_url_list(self):
        if self.mode== 'all':
            collectors= MRTfileHandler.collector_list()
            url_list= [ MRTfileHandler.get_download_url('ribs', coll, self.time_str) for coll in collectors]
        else:
            interval= utils.intervalMin('updates', self.coll[:3])
            # 拿到标准起止时间
            satTime= datetime.strptime(self.time_str, '%Y%m%d.%H%M')
            endTime= datetime.strptime(self.time_end, '%Y%m%d.%H%M')
            satTime, endTime= utils.normSatEndTime(interval, satTime, endTime)
            # 拿到时间点列表
            need=[]
            while satTime.__le__( endTime ):
                need.append( satTime.strftime( '%Y%m%d.%H%M' ))
                satTime += timedelta(seconds= interval* 60)
                
            url_list= [ MRTfileHandler.get_download_url('updates', self.coll, n) for n in need]

        return url_list

    def _download_file(self, queue, urls:list):
        ''''''
        for url in urls:
            # 先判url有效性
            response = requests.head(url).status_code
            if response==404:
                print(f"FAILD: {url=}")
            else:
                url_nodes= url.split('/')
                output_file = f"{ self.p_down}{url_nodes[3]}_{url_nodes[-1]}"
                subprocess.call(['wget', '-q', '-O', output_file, url])
                queue.put(output_file)

    def _parse_file(self, queue):
        ''''''
        target_files=[]
        while True:      
            source = queue.get()     
            if source== None:
                break
            output_file= self.p_pars+ os.path.basename(source)+ '.txt'
            subprocess.call(f'bgpdump -q -m {source} > {output_file}', shell=True)
            target_files.append(output_file)
            #print(f"done : {os.path.basename(source)}.txt")
        return target_files

    #@utils.timer
    def run(self):
        
        t1= time.time()
        url_list= self._get_url_list()
        queue= multiprocessing.Manager().Queue()     
        cores_p= self.core_num//2
        cores_c= self.core_num//2
        pool1 = multiprocessing.Pool(processes=cores_p)
        pool2 = multiprocessing.Pool(processes=cores_c)
        
        sub_set_size= round( len(url_list)/ cores_p )
        for i in range(cores_p):
            if i+1== cores_p:
                sub_set= url_list[i*sub_set_size:]
            else:
                sub_set= url_list[i*sub_set_size: (i+1)*sub_set_size]
            pool1.apply_async( self._download_file, (queue, sub_set))
        pool1.close()

        print("has parsed files:")
        with tqdm.tqdm(total= len(url_list), dynamic_ncols= True) as pbar:            
            results= []
            real_res= []

            for i in range(cores_c):
                res= pool2.apply_async(self._parse_file, (queue,))
                results.append(res)
            pool1.join()

            for i in range(cores_c):
                queue.put(None)

            while len(results)>0:
                for i in range(len(results)):
                    r= results[i]
                    if r.ready():
                        results.pop(i)
                        r_= r.get()
                        real_res+= r_
                        pbar.update(len(r_))
                        break
                    else:
                        time.sleep(0.1)

            pool2.close()
            pool2.join()

        '''real_res=[]
        for r in results:
            real_res+= r.get()'''
        real_res= sorted(real_res)
        print(f'download and parse cost: {(time.time()-t1):.2f}s')
        return real_res

class COmatrixPfxAndPeer():
    ''' - 从所有采集点的rib表获取全局 prefix和peer_AS的共现矩阵
    '''
    @staticmethod
    def _COmatrix_a_rib(path):
        
        try:
            t1= time.time()
            #logger.info('start one rib...')
            df= pl.read_csv(path, sep='|',has_header=False,  ignore_errors= True)
            df.columns= utils.raw_fields

            df['mask']= pl.Series([True]* df.shape[0])
            pfx_2_peer= df.pivot(values='mask', index='dest_pref', columns='peer_AS').fill_null(False)
            
            oriAS_2_peer= (df.select(['peer_AS', pl.col('path').str.split(' ').arr.last().alias('origin_AS'), 'mask'])
                            .pivot(values='mask', index= 'origin_AS', columns='peer_AS').fill_null(0))
            
            #logger.info(f"done:({(time.time()-t1):.1f}sec)  {os.path.basename(path)}")
            return (pfx_2_peer, oriAS_2_peer), os.path.basename(path)
        except Exception as e:
            print(e)
            print(f'ERROR: {path}')
            return (0,0), os.path.basename(path)

    @staticmethod
    def _COmatrix_post_handler(df_list: List[pl.DataFrame], out_path):
        
        DF= df_list[0]
        index_tag= DF.columns[0]
        for id, df in enumerate(df_list[1:]):
            if isinstance(df, pl.DataFrame):
                df.columns =[index_tag]+ [f"{col}_{id}" for col in df.columns[1:]]
                DF= DF.join(df, on= index_tag, how= 'outer')
        DF= DF.fill_null(False)
        
        ASset= set()
        cols= DF.columns[1:]
        for col in cols:
            curAS= col.split('_')[0]
            if curAS not in ASset:
                ASset.add(curAS)
                DF= DF.rename({col: curAS})
            else:
                DF[curAS]= DF[curAS]| DF[col]      
                DF= DF.drop(col)
        DF.select([
            pl.col('dest_pref'),
            pl.exclude('dest_pref').cast(pl.Int8)
        ]).to_csv(out_path)

    @staticmethod
    def get_pfx2peer_COmatrix_parall(ribs_dir='/data/fet/ribs_all_collector_20230228.0000/parsed/',out_path= './COmatrix/', processes=4):
        
        utils.makePath(out_path)
        paths= sorted(glob.glob(ribs_dir+'*'))
        
        with Pool(processes=processes) as pool: 
            results=[]
            with tqdm.tqdm(total= len(paths),dynamic_ncols= True) as pbar:
                for result, fname in pool.imap_unordered(COmatrixPfxAndPeer._COmatrix_a_rib, paths):
                    pbar.update()
                    pbar.set_postfix_str(f"{fname}")
                    results.append( result )

        p2p_list, o2p_list = zip(*results)

        COmatrixPfxAndPeer._COmatrix_post_handler(p2p_list, out_path+'COmatrix_pfx_2_peer.csv')
        COmatrixPfxAndPeer._COmatrix_post_handler(o2p_list, out_path+'COmatrix_oriAS_2_peer.csv')
        logger.info(f"ENDING: COmatrix about prefix and peer. result path: `{out_path}`")
    
    @staticmethod
    def peers_rank_from_COmat():
        '''- 计算全球peerAS的视野排名'''
        zz= PeerSelector._get_existing_COmat()

        df= (zz.sum().to_pandas().T
            .drop('dest_pref')
            .reset_index()
            .rename(columns={'index': 'peerAS', 0: 'count'})
            .sort_values('count', ascending=False, ignore_index=True)
        )
        df['percent']= df['count'].apply(lambda x: '{:.2%}'.format(x/zz.shape[0]))
        return df


class PeerSelector():
    '''- 从rib表得到peers列表'''

    @staticmethod
    def _get_existing_COmat():        
        try:
            df= pl.read_csv('/data/fet/ribs_all_collector_20230228.0000/COmatrix/COmatrix_pfx_2_peer.csv')
        except:
            print('no COmatrix!')
            df= None
        return df
    
    @staticmethod
    def _df2graph(df= None):
        all_edge= ( df
            .groupby([ 'peer_AS', 'dest_pref' ])
                .tail(1)     
                .drop_nulls()
                .select([
                #pl.col('dest_pref'),
                pl.col('path').str.split(' ').alias('path_list_raw'),
                pl.col('path').str.split(" ").arr.shift( -1 ).alias('path_list_sft')
            ])
            .explode( ['path_list_raw', 'path_list_sft'] )   
            .filter( (pl.col('path_list_sft') != None)&
                    (pl.col('path_list_raw') != pl.col('path_list_sft')) &
                    (~pl.col('path_list_sft').str.contains('\{'))
                    )        
             
        ).to_numpy()

        G = nx.DiGraph()
        weights = {}
        for edge in all_edge:
            u, v = edge
            if (u, v) in weights:
                    weights[(u, v)] += 1
            else:
                    weights[(u, v)] = 1
                    G.add_edge(u, v, weight=1)        
         
        for u, v, w in G.edges(data='weight'):
            G[u][v]['weight'] = weights[(u, v)]
        return G

    @staticmethod
    def _greaterthan_avg_degree(G: nx.Graph):
        _df=pd.DataFrame({    
                            'gp_nb_nodes_gt_avg_tol_degree': dict(G.degree),
                            'gp_nb_nodes_gt_avg_out_degree': dict(G.out_degree()), 
                            'gp_nb_nodes_gt_avg_in_degree': dict(G.in_degree())
                        })
        res= _df.apply(lambda x: (x > x.mean()).sum()).to_dict()
        return res

    @staticmethod
    def _simple_feats_graph(G: nx.Graph, source_peer):
        ''''''
        #Gsub, _= GraphBase.get_subgraph_without_low_degree(G)  

        res1= {
            'gp_nb_of_nodes':       len(G.nodes),            
            'gp_nb_of_edges':       len(G.edges),            
            'gp_density':           nx.density(G),   

            'nd_degree':            graphInterAS.avgNode(G.degree),         
            'nd_in_degree':         graphInterAS.avgNode(G.in_degree),    
            'nd_out_degree':        graphInterAS.avgNode(G.out_degree),   
            'nd_degree_centrality': graphInterAS.avgNode(nx.degree_centrality, G),    
            'nd_pagerank':          graphInterAS.avgNode(nx.pagerank, G)            
        }

        res2= {
            #'nd_clustering':            graphInterAS.avgNode(nx.clustering, Gsub),
            #'nd_closeness_centrality':  graphInterAS.avgNode(nx.closeness_centrality, Gsub),
            #'nd_eigenvector_centrality':graphInterAS.avgNode(nx.eigenvector_centrality, Gsub),
        }
        
        res3= PeerSelector._greaterthan_avg_degree(G)

        res= {
            'peer_degree': G.degree[str(source_peer)]}
        for r in [res1, res2, res3]:
            res.update(r)
        return res

    @staticmethod
    def select_peer_from_a_rib(path_or_df, method= 'simple'):
        '''- 从一个rib表中选出最佳的peer
            - args-> path_or_df {`str | pl.df`}: 
            - args-> method
                - 当为默认值，最佳peer = 能观察到pfx的数量的peer（即视野）
                - 当为其他值，需对每个peer下的路由提取信息，分别做全球AS拓扑图，根据图的属性等获取评分，分数最高的peer最佳
            - return {list}: idx=0处的peer最佳
            - 其他方法：调用`ripeAPI.full_table_peer_list`直接获取。
        '''
        if isinstance(path_or_df, str):  
            df= utils.csv2df(path_or_df).select(['peer_AS', 'dest_pref', 'path'])
        else:
            df= path_or_df.select(['peer_AS', 'dest_pref', 'path'])

        peer_list= list(
            df.groupby('peer_AS').agg(pl.col('dest_pref').unique().count())
            .sort('dest_pref', reverse=True)[:,0])
        
        if method== 'simple':
            return peer_list     
        else:    
            #COmat= PeerSelector._get_existing_COmat()
            result= {}
            for cur_peer in peer_list:   
                newdf= df.filter(pl.col('peer_AS')== cur_peer)
                G= PeerSelector._df2graph(newdf)
                #Gsub,_= GraphBase.get_subgraph_without_low_degree(G)

                cur_res= {
                    'peer_vision': newdf['dest_pref'].unique().shape[0],
                    'num_route':   newdf.shape[0],}
                cur_res.update( PeerSelector._simple_feats_graph(G, cur_peer) )
                result[cur_peer]= cur_res
                print(f'done: {cur_peer}')

            dfscore = pd.DataFrame(result)
            dfscore = pd.DataFrame(MinMaxScaler().fit_transform(dfscore.T).T, columns=dfscore.columns)
            dfscore = dfscore.sum()
            print(f"{dfscore=}")

            return dfscore.idxmax()
 
#######################
# 提取特征前，预处理原始消息
#######################

class   UpdsMsgPreHandler():
    '''- 提取特征前，预处理原始消息
    '''
    @staticmethod
    def pfx_oriAS_mapping_from_global_rib(rib_path= ''):
        '''
        - description: 从一张rib表获取完整的全球prefix与originAS的映射。一般选取rrc00的最新rib表。
        - args-> rib_path {path or url}: 
            - 默认为: `'https://data.ris.ripe.net/rrc00/latest-bview.gz'`
            - 也可考虑RIB汇总表(过大, bgpdump无法解析): `https://publicdata.caida.org/datasets/as-relationships/serial-1/20230301.all-paths.bz2`
        '''
        if rib_path=='' or rib_path.startswith('http'):
            if rib_path=='':
                url= 'https://data.ris.ripe.net/rrc00/latest-bview.gz'
            else:
                url= rib_path
            base_name= url.split('/')[-1]
            rib_path= os.getcwd()+'/'+ base_name+ '.txt'
            os.system(f"time wget {url}; time bgpdump -m {base_name} > {rib_path}; rm {base_name}")
                    
        df= utils.csv2df(rib_path)[['peer_AS', 'dest_pref', 'path']]
        num_peer= df['peer_AS'].unique().shape[0]
        num_pfx = df['dest_pref'].unique().shape[0]
        print(f"在{rib_path}中, 有{num_peer}种peerAS, 有{num_pfx}种前缀。")
        
        max_peer= (df.groupby('peer_AS').agg(pl.col('dest_pref').unique().count())
            .sort('dest_pref', reverse=True)[0,0])
        
        df= (df.filter((pl.col('peer_AS')== max_peer))
                .groupby('dest_pref').agg(
                    pl.col('path').last().str.split(' ').arr.last()
                )
                .sort('dest_pref', reverse=False)
                .rename({'dest_pref':'pfx', 'path':'originAS'})
                .filter(~(pl.col('originAS').str.contains('\{')))
        )
        return df

    ## main steps
    @staticmethod
    def peers_select_by_updates(df, topN=12):
        
        peerIP_rank= df.groupby('peer_IP').agg(pl.col('dest_pref').unique().count()).sort('dest_pref', reverse=True).to_pandas()#['peer_IP'][0]
        peers_reserved= peerIP_rank['peer_IP'].tolist()[:topN]
        df= df.filter(pl.col('peer_IP').is_in(peers_reserved))
        return df

    def find_peak(df:pl.DataFrame, coef_std= 3, coef_mean= 1, changepoint_prior_scale= 0.05):
        '''
        - description: 定位疑似异常峰点, 并绘图查看峰点。
        - args-> df {}: 原始路由消息
        - args-> coef_std {*}: `prophet超参数`, 标准差的权重，用于设置异常点阈值
        - args-> coef_mean {*}: `prophet超参数`, 均值的权重，用于设置异常点阈值
        - args-> changepoint_prior_scale {0-100}: `prophet超参数`, 针对预测曲线 控制算法中的拐点数量和位置的灵活性。
                该参数越小，算法越趋向于学习平滑的拟合曲线，越大则趋向于学习波动较大的拟合曲线。
        - return : 
            - df {pl}: 筛选后的原始df
            - {dict} key：`count`被判定为异常的行号/分钟序号
            - {dict} value：`count`被判定为异常时，prophet模型的预测值
        '''
        from prophet import Prophet
        event_timestamp= df[0, 'timestamp']
        df_out= RawMrtDataAnaly(df).df
        df= ( df_out
            #.filter(
            #    ( pl.col('time_bin')>=0)
            #    &( pl.col('time_bin')<500)
            #)
            .groupby('time_bin', maintain_order=True).agg([
                pl.col('timestamp').first().apply( lambda x: datetime.fromtimestamp(x, tz= timezone.utc).strftime('%Y/%m/%d %H:%M')).alias('date'),
                pl.col('timestamp').count().alias('count')
            ])
            .select(pl.exclude('time_bin'))
            .to_pandas()#.loc[:100]
        )

        cols= df.columns
        df = df.rename(columns={cols[0]: 'ds', cols[1]: 'y'})
        df['ds']= pd.to_datetime(df['ds'], infer_datetime_format=True)

        model = Prophet(changepoint_prior_scale= changepoint_prior_scale)
        model.fit(df)

        future = model.make_future_dataframe(periods=2, freq='min')
        forecast = model.predict(future)
        
        forecast_yhat= forecast['yhat'][:len(df)]
        #df['error'] = np.abs(df['y'] - forecast_yhat)
        df['error']= df['y'] - forecast_yhat
        std = df['error'].std()
        mean = df['error'].mean()
        
        threshold = coef_std * std + coef_mean * mean
        
        anomalies = df[df['error'] > threshold]

        fig = plt.figure(figsize=(8, 6)) 
        plt.plot(df['ds'], df['y'], color='black', label='Original')
        plt.plot(forecast['ds'], forecast['yhat'], color='black', linestyle='--', label='Predicted')
        plt.scatter(anomalies['ds'], anomalies['y'], color='grey', marker='x', label='Candidates')
        plt.legend(loc='best')
        plt.xlabel('Time', fontsize=12)
        plt.ylabel('Number of messages', fontsize=12)
        plt.xticks(fontsize=10)
        plt.yticks(fontsize=10)
        plt.ylim(bottom=0)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.show()

        p = f'./draw_abnormal_points_with_prophet_{event_timestamp}.png'
        plt.savefig(p, dpi=300, bbox_inches='tight')

        print(f"Peak plot stored in：{p}")

        row_numbers = df[df['error'] > threshold].index.tolist()
        #diff_values = df[df['error'] > threshold]['error_with_neg'].tolist() 
        row_yhat= forecast_yhat[df['error'] > threshold].tolist() 
        #return {'row_nums': row_numbers, 'row_yhat': row_yhat}
        a= zip(row_numbers, row_yhat)
        dic_= {tup[0]:tup[1] for tup in a}
        return df_out, dic_

    def distinguish_norm_peak(df: pl.DataFrame, peak_info: dict, global_mapping: list, thd= 0.85):
        '''- peak clipping'''
        
        peak_not_event=( df
            .filter( 
                pl.col('time_bin').is_in(list(peak_info.keys()))
                & (pl.col('msg_type')== 'A')
            )
            .with_column(
                (pl.col('dest_pref')+pl.lit(' ')+ pl.col('originAS')).alias('pfx_oriAS')
            )
            .groupby('time_bin', maintain_order=True).agg([
                (pl.col('pfx_oriAS').is_in(global_mapping).sum()/pl.col('pfx_oriAS').count()).alias('ratio_old_mapping')
            ])
            .filter(pl.col('ratio_old_mapping')> thd)
            ['time_bin']
            .to_list()
        )
        print(f"Suspected abnormal peaks: {len(peak_info)}; Harmless peaks: {len(peak_not_event)}")
        return peak_not_event

    @staticmethod
    def cut_peak(df:pl.DataFrame, dic_suspi, peak_not_event):
        '''- '''
        a=( df.filter( pl.col('time_bin').is_in(peak_not_event)   
            # & (pl.col('msg_type')== 'A')   
            )
            .groupby('time_bin').apply(
                lambda group_df: (group_df.sample( int(dic_suspi[group_df['time_bin'][0]]), shuffle=True ) )
            ))
        a= df.filter(~pl.col('time_bin').is_in(peak_not_event)).vstack(a)

        a= a.sort('id')
        print(f"after clipping peaks: {a.shape=}")
        return a

    @staticmethod
    def run_cut_peak(df:pl.DataFrame, mapping_path='', coef_std=3, coef_mean=1, thd=0.85):
        '''- main func'''
        global_mapping= UpdsMsgPreHandler.pfx_oriAS_mapping_from_global_rib(mapping_path)

        df= UpdsMsgPreHandler.peers_select_by_updates(df)
        df, dic_suspi= UpdsMsgPreHandler.find_peak(df, coef_std= coef_std, coef_mean=coef_mean)
        lis_peak_normal= UpdsMsgPreHandler.distinguish_norm_peak(df, peak_info= dic_suspi, global_mapping=global_mapping,thd= thd)
        df= UpdsMsgPreHandler.cut_peak(df, dic_suspi, lis_peak_normal)

        df_plot= ( df
            #.filter(
            #    ( pl.col('time_bin')>=0)
            #    &( pl.col('time_bin')<500)
            #)
            .groupby('time_bin', maintain_order=True).agg([
                pl.col('timestamp').first().apply( lambda x: datetime.fromtimestamp(x, tz= timezone.utc).strftime('%Y/%m/%d %H:%M')).alias('date'),
                pl.col('timestamp').count().alias('count')
            ])
            .select(pl.exclude('time_bin'))
            .to_pandas()#.loc[:100]
        )
        df_plot['date']= pd.to_datetime(df_plot['date'], infer_datetime_format=True)
        print('See the effect after peak shaving: ')
        df_plot.set_index('date').plot(figsize=(8,6))

        return df


class RawMrtDataAnaly():
    '''- 从原始updates路由数据中分析事件, 基于`pl.expr`'''

    def __init__(self, df:pl.DataFrame) -> None:
        
        first_ts= df[0, 'timestamp']
        self.df= (df
            .filter(pl.col('msg_type')!= 'STATE')
            .with_columns([
                ((pl.col('timestamp')- first_ts)// 60).cast(pl.Int16).alias('protocol'),
                pl.col('path').str.replace(' \{.*\}', '')
            ])
            .with_column(pl.col('path').str.split(' ').arr.last().alias('originAS'))
            .rename({'protocol': 'time_bin'})
            .with_row_count('id')
            #.groupby('time_bin')
        )
        
        full_table_peer= ripeAPI.full_table_peer_list( datetime.fromtimestamp(first_ts).strftime('%Y-%m-%dT%H:%M'))[0]
        full_table_peer= [tup[1] for tup in full_table_peer]
        _, peer_reserve= self.list_peers_rank()
        
        self.figsize= (8,6)
        self.ts2dt_str= pl.col('timestamp').first().apply( lambda x: datetime.fromtimestamp(x, tz= timezone.utc).strftime('%Y/%m/%d %H:%M')).alias('date')
        self.filter_dict={
            'full-table': pl.col('peer_IP').is_in(full_table_peer),     
            'num_gt_5pmin': pl.col('peer_IP').is_in(list(peer_reserve)), 
        }

    def _fmt_sci(self):
        '''- set the axis'''
        formatter = ticker.ScalarFormatter(useMathText=True)
        formatter.set_scientific(True)
        formatter.set_powerlimits((-2, 2))
        return formatter
    def _fmt_date(self, ax, str_fmt= '%dT%H'):
        '''- set the format of date scale'''
        date_format = DateFormatter(str_fmt)
        date_locator = AutoDateLocator()
        ax.xaxis.set_major_locator(date_locator)
        ax.xaxis.set_major_formatter(date_format)
    def _set_titles(self,ax, title=None, xlabel=None, ylabel=None):
        if title:
            ax.set_title(title)
        if xlabel:
            ax.set_xlabel(xlabel)
        if ylabel:
            ax.set_ylabel(ylabel)
        
    def TS_whole(self, out_file_name= None, filter_expr= None):
        '''- 总览消息量的走势'''
        if isinstance(filter_expr, pl.Expr):
            ldf= self.df.lazy().filter(filter_expr)
        else:
            ldf= self.df.lazy()
            
        df= (ldf
            .groupby(['time_bin'], maintain_order=True)
            .agg([
                self.ts2dt_str,
                pl.col('timestamp').count().alias('count')
            ])
            .sort('time_bin')
            #.select(pl.exclude('time_bin'))
            .collect()
            .to_pandas()
            #.set_index('date')
        )

        df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)

        plt.style.use('seaborn-v0_8-paper')
        fig, ax = plt.subplots(figsize= self.figsize)
        ax.plot(df['date'], df['count'], color='b', linewidth=2)

        self._set_titles(ax,'number of updates', 'Date', 'Count')
        #self._fmt_date(ax)
        ax.yaxis.set_major_formatter(self._fmt_sci())
        fig.tight_layout()
        if out_file_name:
            plt.savefig(f"{out_file_name}.jpg", dpi=300)
        
        plt.show()
        return df

    def TS_AW_compare(self, out_file_name= None, filter_expr= None):
        '''- 对比宣告、撤销消息量的走势'''
        if isinstance(filter_expr, pl.Expr):
            ldf= self.df.lazy().filter(filter_expr)
        else:
            ldf= self.df.lazy()
        df_= (ldf
            .groupby(['msg_type','time_bin'], maintain_order=True)
            .agg([
                self.ts2dt_str,
                pl.col('timestamp').count().alias('count')
            ])
            .sort('time_bin')
            #.select(pl.exclude('time_bin'))
            .collect()
            .to_pandas()
            #.set_index('date')
        )
        df_['date'] = pd.to_datetime(df_['date'], infer_datetime_format=True)

        grouped = df_.groupby(['msg_type']).groups

        plt.style.use('seaborn-v0_8-paper')
        fig, ax = plt.subplots(figsize= self.figsize)
        linestyle= {'A': '-', 'W':'--'}
        for msg_type, idxs in grouped.items():
            x= df_.loc[idxs, 'date']
            y= df_.loc[idxs, 'count']
            ax.plot(x, y, label= msg_type, linewidth=1, linestyle=linestyle[msg_type], color='black')    #  s=3,
            ax.legend()
        try:
            start_date= df_.loc[(df_['msg_type']=='A'),:].iloc[125, 2]
            end_date  = df_.loc[(df_['msg_type']=='A'),:].iloc[185, 2]
            print(start_date, end_date)
            plt.fill_between(
                df_['date'], df_['count'].min(), df_['count'].max(), 
                where=((df_['date'] >= start_date) & (df_['date'] <= end_date)), 
                color='gray', alpha=0.3)
        except:
            print('cant draw shadow')
        self._set_titles(ax,'number of updates', 'Date', 'Count')
        #self._fmt_date(ax)
        ax.yaxis.set_major_formatter(self._fmt_sci())
        fig.tight_layout()
        if out_file_name:
            plt.savefig(f"{out_file_name}.jpg", dpi=300)
        
        plt.show()
        return df_

    def TS_peer_compare(self, out_file_name= None, filter_expr= None):
        '''- 对比各个peer消息量的走势'''
        if isinstance(filter_expr, pl.Expr):
            ldf= self.df.lazy().filter(filter_expr)
        else:
            ldf= self.df.lazy()

        df_= (ldf
            .groupby(['peer_IP','time_bin'], maintain_order=True)
            .agg([
                self.ts2dt_str,     
                pl.col('timestamp').count().alias('count')
            ])
            .sort('time_bin')
            #.select(pl.exclude('time_bin'))
            .collect()
            .to_pandas()
            #.set_index('date')
        )
        df_['date'] = pd.to_datetime(df_['date'], infer_datetime_format=True)

        grouped = df_.groupby(['peer_IP']).groups

        plt.style.use('seaborn-v0_8-paper')
        fig, axes = plt.subplots(nrows=len(grouped), figsize=(10, len(grouped)), sharex=True)
        i=0
        for peer_IP, idxs in grouped.items():
            x= df_.loc[idxs, 'date']
            y= df_.loc[idxs, 'count']
            ax = axes[i]
            ax.plot(x, y, label=peer_IP)    #  s=3,, linewidth=1
            #ax.set_title(f'{peer_IP}')
            #ax.set_ylabel('Count')
            ax.legend()
            i+=1
              
        fig.tight_layout()
        if out_file_name:
            plt.savefig(f"{out_file_name}.jpg", dpi=300)
        
        plt.show()
        return df_

    def list_peers_rank(self, thd_per_min= 100):
        '''- 阈值法筛选peers'''
        thd= self.df['time_bin'].max()* thd_per_min
        peers_rank= self.df['peer_IP'].value_counts()
        peers_set= set((peers_rank
            .filter(pl.col('counts')>=thd)
            ['peer_IP'].to_list())
            )
        return peers_rank, peers_set

    def df_Apeer(self, out_file_name= None, filter_expr= None):
        '''- 获取一个peer下的完整消息'''
        if isinstance(filter_expr, pl.Expr):
            ldf= self.df.lazy().filter(filter_expr)
        else:
            ldf= self.df.lazy()
        df_= (ldf
            .select([
                'time_bin', 
                self.ts2dt_str,
                'dest_pref',
                pl.col('path').str.extract(' (\d+)$', 1).cast(pl.Utf8).alias('loc_0'),
                pl.col('path').str.extract(' (\d+) \d+$', 1).cast(pl.Utf8).alias('loc_1'),
                pl.col('path').str.extract(' (\d+) \d+ \d+$', 1).cast(pl.Utf8).alias('loc_2'),
                pl.col('path').str.extract(' (\d+) \d+ \d+ \d+$', 1).cast(pl.Utf8).alias('loc_3'),
                pl.col('path').str.extract(' (\d+) \d+ \d+ \d+ \d+$', 1).cast(pl.Utf8).alias('loc_4'),
                pl.col('path').str.extract(' (\d+) \d+ \d+ \d+ \d+ \d+$', 1).cast(pl.Utf8).alias('loc_5'),
                pl.col('path').str.extract(' (\d+) \d+ \d+ \d+ \d+ \d+ \d+$', 1).cast(pl.Utf8).alias('loc_6'),
                pl.col('path').str.extract(' (\d+) \d+ \d+ \d+ \d+ \d+ \d+ \d+$', 1).cast(pl.Utf8).alias('loc_7'),
                pl.col('path').str.extract(' (\d+) \d+ \d+ \d+ \d+ \d+ \d+ \d+ \d+$', 1).cast(pl.Utf8).alias('loc_8'),
            ])
            .collect()
            .to_pandas()
            #.set_index('date')
        )
        df_['date'] = pd.to_datetime(df_['date'], infer_datetime_format=True)
        return df_

    def set_local_mapping_and_draw_Veen(self, out_file_name= None, filter_expr= None, set_global_mapping= None):
        '''
        - 获取pfx_oriAS映射集合, 用Venn图比较全局pfx_oriAS映射和当前映射集合的关系
        '''
        if isinstance(filter_expr, pl.Expr):
            ldf= self.df.lazy().filter(filter_expr)
        else:
            ldf= self.df.lazy()
        df_= set(ldf
            .select(pl.col('dest_pref')+pl.lit(' ')+ pl.col('originAS'))
            .collect().to_series()        )
        print(f"当前df子集中，含pfx-oriAS映射{len(df_)} 对。")
        
        if set_global_mapping!= None:
            ratio= len(df_.difference(set_global_mapping))/len(df_)
            print(f"本地新映射占本地总映射之比：{ratio:.2%}")
            venn2([df_, set_global_mapping], set_labels=('pfx-AS_event', 'pfx-AS_global'))
            plt.show()

        return df_

#######################
# 分析特征
#######################

class FeatMatHandler:
    '''- 谱残差'''
    @staticmethod
    def spectral_residual(data, smooth_window=1):
        '''
        - args-> smooth_window {*}: 较大的窗口可能会捕捉到更低频的周期成分，而较小的窗口可能会捕捉到更高频的周期成分。
        - return {*}
        '''
        data_fft = fft(data)
        
        amplitude_spectrum = np.abs(data_fft)
        phase_spectrum = np.angle(data_fft)
        log_data = np.log(amplitude_spectrum)  
        
        smoothed_amplitude = np.convolve(log_data, np.ones(smooth_window) / smooth_window, mode='same')
        
        spectral_residual = log_data - smoothed_amplitude
        residual_signal = np.real(ifft(np.exp(spectral_residual + 1j * phase_spectrum)))
        
        anomaly_scores = np.square(residual_signal)

        return anomaly_scores

class Filter:
    '''- 特征过滤'''
    def __init__(self, df:pd.DataFrame):
        df= df.fillna(0.0)        
        self.df= df
        self.x= df.iloc[:,2:-1]
        self.y= df.iloc[:,-1]

    def variance_filter(self,thd):
        '''- excluding low variance feats, return DF filtered several feats'''
        selector_vt = VarianceThreshold( thd )
        x_varThd = selector_vt.fit_transform(self.x)
        return pd.DataFrame(x_varThd, columns= selector_vt.get_feature_names_out())

    def chi2_filter(self, x_varThd):        
        ''' 
        description: to get the filtered feats which p_value >0.05 from chi2 and the rank(descending) of all feats by chi2
        '''                
        chival, pval= chi2(x_varThd, self.y)
        k_chi2= (pval<= 0.05).sum()         # get hyperparameters: k feats to be remained after chi2 filtering
        selector_chi2 = SelectKBest(chi2, k= k_chi2)
        x_chi2= selector_chi2.fit_transform(x_varThd, self.y)
        x_chi2= pd.DataFrame(x_chi2, columns= selector_chi2.get_feature_names_out() )

        df_chi2= pd.DataFrame(selector_chi2.scores_, columns=['score_chi2'],index= selector_chi2.feature_names_in_)
        df_chi2= df_chi2.sort_values( 'score_chi2',ascending=0)

        return x_chi2, df_chi2

    def mic_filter(self, x_varThd):
        '''return x_mic, df_mic '''
        df_mic= mutual_info_classif(x_varThd, self.y)
        k= (df_mic>= 0.05).sum()
        selector_mic = SelectKBest(mutual_info_classif, k=k)
        x_mic= selector_mic.fit_transform(x_varThd, self.y)
        x_mic= pd.DataFrame(x_mic, columns= selector_mic.get_feature_names_out() )
                
        df_mic= pd.DataFrame(selector_mic.scores_, columns=['score_mic'],index= selector_mic.feature_names_in_)
        df_mic= df_mic.sort_values( 'score_mic',ascending=0)

        return x_mic, df_mic

    def redu_filter(self, x_flted1: pd.DataFrame, df_ranked1: pd.DataFrame, redu_thd: float):
        '''
        description: filter redundant feats by deleting corr between feats
        '''
        df_corr= abs(x_flted1.corr(method= 'spearman'))
        df_corr_= df_corr[df_corr> redu_thd]
        df_corr_= pd.DataFrame( np.tril(df_corr_.values, -1), index= df_corr_.index, columns= df_corr_.columns)
        df_corr_= df_corr_.replace(np.nan, 0)

        #fig= plt.figure(figsize=(10,8))
        #sns.heatmap(df_corr_, annot= False)
        rele_table=[]   
        li_ranked1= df_ranked1.index.tolist()

        while df_corr_.sum().sum()> 0:
            cur_arr= df_corr_.values
            max_val= cur_arr.max()
            max_idx= np.unravel_index(cur_arr.argmax(), cur_arr.shape)
            feat1= df_corr_.index[max_idx[0]]
            feat2= df_corr_.columns[max_idx[1]]
            tar_col, tar_col2= (feat1, feat2) if li_ranked1.index(feat1)> li_ranked1.index(feat2) else (feat2,feat1)
            rele_table.append( (max_val, tar_col, tar_col2))

            df_corr_= df_corr_.drop(index= tar_col, columns= tar_col)
            
        new_cols= df_corr_.columns
        x_del_corr= x_flted1[new_cols ]
        # cross_val_score(RFC(n_estimators=10,random_state=0),x_del_corr ,y,cv=5).mean()

        rele_table= pd.DataFrame(rele_table, columns=['corr', 'feat_del', 'feat_save'])
        return rele_table, x_del_corr
                
    def get_redu_thd(self, x_flted1: pd.DataFrame, df_ranked1: pd.DataFrame):
        '''
        description: to find hyperparameters redundant threshold in df.corr()
        '''
        mdl_score= []
        for thd in np.arange(0,1, 0.01):
            _, x_new= self.redu_filter(x_flted1, df_ranked1, redu_thd= thd)
            score= cross_val_score(RFC(n_estimators=10,random_state=0),x_new ,self.y,cv=5).mean()
            mdl_score.append( (thd, score) )
        max_thd= max(mdl_score, key= lambda x: x[1])[0]

        return max_thd, mdl_score

    def run(self):
        x_varThd= self.variance_filter()
        x_chi2, df_chi2= self.chi2_filter(x_varThd)
        #max_thd, mdl_score= self.get_redu_thd( x_chi2, df_chi2)
        #rele_table, x_del_corr= self.redu_filter( x_chi2, df_chi2, redu_thd= max_thd)
        return df_chi2


def txt2df(paths: Union[str, list], need_concat:bool= True):
    '''
    - description: 读取数据文件为pandas.df对象
    '''
    if isinstance(paths, str):
        df= pd.read_csv(paths)
        return df
    if isinstance(paths, list):
        df_list= []
        for path in paths:
            df_list.append( pd.read_csv(path))
        if need_concat:
            df= pd.concat(df_list, ignore_index=True)
            
            df['time_bin']= df.index
            label_map= {'normal': 0, 'hijack': 1, 'leak': 1, 'outage': 1}
            df['label']= df['label'].map( label_map )

            return df
        else:
            return df_list


def df2Xy(df, test_size= 0):
    '''- Split training and testing sets'''
    df = df.iloc[:, 2:]
    X = df.iloc[:, :-1]
    y = df.iloc[:, -1]

    if test_size==0:
        return X, y
    else:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, shuffle=True)
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

        return X_train, X_test, y_train, y_test

if __name__=='__main__':
    data= MRTfileHandler.draw_collectors_file_size('RosTel_hijack_50', '20170425.0000', '20170430.0000', ('20170426.2030','20170426.2300'))

