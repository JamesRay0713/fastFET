#! /usr/bin/env python
# coding=utf-8
'''
- Description: BGP异常检测中常用的辅助函数
- version: 1.0
- Author: JamesRay
- Date: 2023-02-06 13:10:54 
- LastEditTime: 2023-03-20 00:25:45
'''
import os, json, time, re, glob, jsonpath
import requests
from bs4 import BeautifulSoup
from functools import partial
from typing import Union, List, Dict

import multiprocessing, subprocess
from multiprocessing import Pool
import tqdm
from datetime import datetime, timedelta
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

from fastFET import utils
from fastFET.featGraph import graphInterAS
logger= utils.logger

#######################
#  地理位置的研究：peers, AS, prefix, IP
#######################
from geopy.geocoders import Bing
import geoip2.database

class CommonTool(object):
    '''分析BGP事件时常用工具集：
        - AS, prefix, IP等实体到地理位置的转换
        - AS, prefix, IP间的互相搜索, 更多API参考: `https://stat.ripe.net/docs/02.data-api/`
        - 获取保留IP地址块列表
        '''

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
                # reader对象用以ip2coord
            reader = geoip2.database.Reader(path_db)
                # geolocator对象用以 coord2city
            geolocator = Bing(api_key='Ag7S7BV4AkTdlUzzm_pgSZbQ9c_FBf9IbvSnSlui2x-kE6h-jnYKlT7EHYzRfxjC')
                # 坐标池，用以加速coord2city, 经纬度是key, 城市名是value
            coord_city_dic= {}
            for ip in IPs:
                response = reader.city(ip)
                latitude = response.location.latitude
                longitude = response.location.longitude

                cityName = response.city.name
                if cityName!= None:
                    cityName+= ','+ response.country.name
                else:     #改用Bing map api来求。
                    if (latitude, longitude) not in coord_city_dic:
                        location = geolocator.reverse((latitude, longitude))
                        cityName= ' '
                        if location:
                            try:
                                cityName = location.raw['address']['adminDistrict2']+ ', '+ location.raw['address']['countryRegion']
                            except:
                                cityName = location.address
                        time.sleep(0.15)     # Bing map API限速
                        coord_city_dic[(latitude, longitude)]= cityName
                    else:
                        cityName= coord_city_dic[(latitude, longitude)]
                res[ip]= [latitude, longitude, cityName]
                    
                logger.info(f'done: {count} coord2city')
                count+=1
            reader.close()
            return res

    
class PeersData(object):
    '''收集RIPE-NCC和RouteViews项目中的peers信息, 用于观察peers分布等'''

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
            - 当前函数仅收录31个采集点，即`32 - route-views6`, 因为那个点只含peerIP-v6
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
                # 把peerIP-v6排除
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
        - description: 获取所有peers的信息. 
        - 结果存储在`./peers_info.json`
        - return {dict}: `{ 'rou': [{'asn', 'ip', 'v4_prefix_count', 'longitude', 'latitude', 'collector'}, {}, ...],
                            'rrc': [...] }`
        '''
        rv_info, rv_ips= PeersData.fromRV()
        rc_info, rc_ips= PeersData.fromRRC()

        ip_map= CommonTool.ip2coord(set(rv_ips+ rc_ips))

        res={}
        for data in (rv_info, rc_info):
            cur_res= []
            # 3. 把坐标属性并入peer字典
            for rrc, rrc_dic in data.items():
                for ip, peer_dic in rrc_dic.items():
                    peer_dic['latitude']=  ip_map[ip][0]
                    peer_dic['longitude']= ip_map[ip][1]
                    peer_dic['cityName'] = ip_map[ip][2] if ip_map[ip][2]!= None else ' '
                    peer_dic['collector']= rrc
                    cur_res.append(peer_dic)

            # 4. 并入颜色属性到字典
                # 下标对应采集点的编号
            colors = [
                '#1F75FE', '#057DCD', '#3D85C6', '#0071C5', '#4B86B4',
                '#17A589', '#52BE80', '#2ECC71', '#00B16A', '#27AE60',
                '#E74C3C', '#FF5733', '#C0392B', '#FF7F50', '#D35400',
                '#9B59B6', '#8E44AD', '#6A5ACD', '#7D3C98', '#BF55EC',
                '#E67E22', '#FFA500', '#FF8C00', '#FF6347', '#FF4500',
                '#F1C40F', '#FFD700', '#F0E68C', '#FFA07A', '#FFB900',
                '#555555', '#BDC3C7', '#A9A9A9', '#D3D3D3', '#808080'
            ]
                # 把采集点名字映射为下标
            collector2idx= { val: idx for idx, val in enumerate( list(data.keys())) }
            for peer in cur_res:
                peer['color']= colors[collector2idx[ peer['collector'] ]]

            key= list(data.keys())[0][:3]
            res[ key]= cur_res

        # 5. 导出
        with open(path_out, 'w') as f:
            json.dump(res, f)
        logger.info( f"rrc: {len( res['rrc'])} peers.\nrou: {len( res['rou'])} peers.\n### all peers info stored at `{path_out}`")

        return res

    @staticmethod
    def get_rrc_info(path_in= './peers_info.json', path_out= 'peers_info_about_collector.csv'):
        '''
        - description: 获取每个rrc的peers数量、城市列表. 这是对`get_peers_info()`输出的汇总。
        '''
        if not os.path.exists(path_in):
            PeersData.get_peers_info()
        with open(path_in) as f:
            datas= json.load(f)
        datas= datas['rou']+ datas['rrc']

        # 得到每个rrc所在城市的列表
        rrc_city= {}
        rrc_count= []
        for dic in datas:
            rrc_count.append( dic['collector'])

            if not dic['collector'] in rrc_city.keys():
                rrc_city[dic['collector']]= [ dic['cityName'] ]
            else:
                if dic['cityName']!= ' ':
                    rrc_city[dic['collector']].append( dic['cityName'])

        # 得到RRC的规模（拥有多少peer）
        peer_num_in_RRC= pd.value_counts(rrc_count).sort_index().to_frame()
        # 得到RRC的城市的去重列表
        for rrc, city_lis in rrc_city.items():
            rrc_city[rrc]= [str(set(city_lis))]
        rrc_city_pd= pd.DataFrame(rrc_city).T
        # 合并上述两列
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

    #TODO: 针对两个项目中为公开peers的采集点，如何得到其peers和相应的地理信息？
    #       考虑从采集点的RIB表中收集。
    def get_peers_from_rib(rib_path):
        pass

    @staticmethod
    def peerAS2country(project= 'rrc', p= './peers_info.json'):
        '''- 获取每个国家的peers数量。可用于量化‘peers在欧美与非欧美国家间分布严重不均匀’'''
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
        '''-input国家代码; output国家名'''
        try:
            country = pycountry.countries.get(alpha_2=code)
            return country.name
        except:
            return None


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

        # 26个采集点
        collector_list_rrc= [f'rrc{i:02d}' for i in range(27)]
        collector_list_rrc.pop(17)
        # 38个采集点
        collector_list_rou= ["route-views.ny","route-views2","route-views.amsix","route-views.chicago","route-views.chile","route-views.eqix","route-views.flix","route-views.fortaleza","route-views.gixa","route-views.gorex","route-views.isc","route-views.jinx","route-views.kixp","route-views.linx","route-views.napafrica","route-views.nwax","route-views.perth","route-views.phoix","route-views.rio","route-views.saopaulo","route-views.sfmix","route-views.sg","route-views.soxrs","route-views.sydney","route-views.telxatl","route-views.wide","route-views2.saopaulo","route-views3","route-views4","route-views5","route-views6","route-views.peru","route-views.seix","route-views.mwix","route-views.bdix","route-views.bknix","route-views.uaeix","route-views"]
        
        if project== 'RIPE':
            return collector_list_rrc
        elif project== 'RouteViews':
            return collector_list_rou
        else:
            return collector_list_rrc+ collector_list_rou
    
    @staticmethod
    def get_download_url(type:str, monitor:str, tarTime):
        '''
        - description: 获取MRT文件下载链接。注意, 得到的链接可能404
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
            return float(size_str)
    
    @staticmethod
    def _get_month_list(time_start='20210416', time_end='20210718'):
        '''- 获取指定时间段内的月份列表，如：['2021.04', '2021.05',...]'''
        
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
        return month_list

    @staticmethod
    def _get_collector_file_size(collector, month_list) -> dict:
        '''
        - description: 从一个collector获取指定`月份`内的`MRT file size`的变化
        - args-> collector {str}: 
        - args-> month_list {list}: from `_get_month_list()`
        - return {dict}: like: {collector: {'20210401.0000': '6M', '20210401.0005': '5M',...} }, value可能为空
        '''
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

            pageInfo= requests.get(url).text
            matches= re.findall(pattern_dir[collector[:3]], pageInfo)
            for time, size in matches:
                # 很烦心的一个事：两个数据集中的时间格式并不是完全按照每5/15分钟一跳来存储MRT文件的。
                # 在此我通过删除不能被5/15整除的时间，来简单地排除异常。
                if int(time[-2:])% diveded_map[collector[:3]] ==0:
                    res_dic[time]= size

        #logger.info(f'* * done: {collector}')
        return {collector: res_dic}

    @staticmethod
    def _get_collectors_file_size(time_start='20211004.1200', time_end=None, project=None):
        '''
        - description: 并行使用`_get_collector_file_size`。返回值排除了空数据的采集点。
        - args-> project {str}: either-or of 'RouteViews' and 'RIPE'
        - return {dict}: `{'rrc': DF(collectors* dates), 'rou': ~same~}`
        '''
        if time_end==None:
            time_end= time_start
        proj_map  = {'RouteViews': MRTfileHandler.collector_list('RIPE'), 'RIPE': MRTfileHandler.collector_list('RouteViews')}
        collector_list= proj_map[project] if project else proj_map['RIPE']+ proj_map['RouteViews']
        month_list= MRTfileHandler._get_month_list(time_start, time_end)

        # 并行获取每个collector的数据
        with Pool(processes = 54) as pool:
            results= pool.map(partial(MRTfileHandler._get_collector_file_size, month_list= month_list), collector_list)

        # 处理汇总数据
        res= {}
        res_rrc= {}
        res_rou= {}
        for r in results:
            if 'rrc' in list(r.keys())[0]:
                res_rrc.update(r)
            else:
                res_rou.update(r)
        for dic in [res_rou, res_rrc]:
            # 压缩日期到数天，并统一单位
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
    def draw_collectors_file_size(eventName='', time_start='20211004.1200', time_end=None, event_period=None):
        '''
        - description: 画图对比各采集点的`file_size`走势。有图像导出。
        - args-> data {*}: all of values returned by `_get_collectors_file_size()`
        - args-> event_period {`('20211004.1200', '20211004.1200')`}: 当需要在图中作异常区间阴影时使用
        - return {*}
        '''
        if time_end== None:
            time_end= time_start
        #if not data:
        data= MRTfileHandler._get_collectors_file_size(time_start, time_end)
        
        for project, df in data.items():
            #size_map= {'RIPE': 23, 'RouteViews': 32}
            print(f'{df.shape=}')
            title= f"{time_start[:8]}_{eventName}_{project}.jpg"
            utils.makePath(f'plot_file_sizes/{title}')
            ax= df.plot( # y=df.columns[3],
                    figsize=(10, df.shape[1]),
                    subplots=True
            )
            # 有并列子图时的造阴影
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
            # 计算峰度
            kurt = df.apply(kurtosis)
            score = (10 * (kurt - kurt.min()) / (kurt.max() - kurt.min())).sort_values(ascending=False)
            res= pd.concat([res, score])
        res= res.sort_values(ascending=False)
        return res, data

class DownloadParseFiles():
    '''- 简单场景下的MRT文件的下载和解析操作, 含2种模式。'''
    def __init__(self, mode= 'all', time_str= '20230228.0000', time_end= None, coll= None, target_dir= os.getcwd(), core_num= 40) -> None:
        ''' 
        - args-> mode {'all'/'a'}: 
            - `all`: 所有采集点模式，用于下载并解析`指定时刻time_str`的`所有采集点`的(rib)表；
            - `a`  : 单一采集点模式，用于下载并解析`指定时间段time_str ~ time_end`的`指定采集点coll`的(updates)表
        - args-> time_str {*}: 
        - args-> time_end {*}: 当mode='a'时有效
        - args-> coll {*}: 当mode='a'时有效
        - args-> target_dir {*}: cwd
        - args-> core_num {*}: 默认40核
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
        '''- 生产者: 下载所有urls, 将下载后的路径存入queue'''
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
        '''- 消费者: 解析queue中的数个file, 返回其路径列表'''
        target_files=[]
        while True:     # 即只要队列中还有元素，则该消费者子进程持续执行
            source = queue.get()    # 当queue中没有元素，则会一直空转等待元素。
            if source== None:
                break
            output_file= self.p_pars+ os.path.basename(source)+ '.txt'
            subprocess.call(f'bgpdump -q -m {source} > {output_file}', shell=True)
            target_files.append(output_file)
            #print(f"done : {os.path.basename(source)}.txt")
        return target_files

    #@utils.timer
    def run(self):
        '''- return {list}: 解析后的所有路径
        '''
        t1= time.time()
        url_list= self._get_url_list()
        queue= multiprocessing.Manager().Queue()    # 要用可在进程间共享的队列
        cores_p= self.core_num//2
        cores_c= self.core_num//2
        pool1 = multiprocessing.Pool(processes=cores_p)
        pool2 = multiprocessing.Pool(processes=cores_c)
        
        # 生产者
        sub_set_size= round( len(url_list)/ cores_p )
        for i in range(cores_p):
            if i+1== cores_p:
                sub_set= url_list[i*sub_set_size:]
            else:
                sub_set= url_list[i*sub_set_size: (i+1)*sub_set_size]
            pool1.apply_async( self._download_file, (queue, sub_set))
        pool1.close()

        # 消费者
        print("has parsed files:")
        with tqdm.tqdm(total= len(url_list), dynamic_ncols= True) as pbar:            
            results= []
            real_res= []

            for i in range(cores_c):
                res= pool2.apply_async(self._parse_file, (queue,))
                results.append(res)
            pool1.join()

                # 添加迫使消费者结束的哨兵
            for i in range(cores_c):
                queue.put(None)

            # 等待任务完成并更新进度条
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
    ''' - 从所有采集点的rib表获取全局 prefix和peer_AS的共现矩阵，以得到各peer的视野大小，判断其在全球路由收集中的重要性。
        - 前提：用`DownloadParseFiles`类下载解析全局rib表。
        - 另：通过共现矩阵pl.df查看peerAS的视野排名:`a= df[:,1:].sum().to_pandas().T.rename(columns={0: 'pfx_num'}).sort_values('pfx_num', ascending=False)`
    '''
    @staticmethod
    def _COmatrix_a_rib(path):
        '''- 获取一张路由表中的pfx-peer_AS共现矩阵 & originAS-peer_AS共现矩阵'''
        # 被并行的函数一定要try,不然一个断，所有进程都断了
        try:
            t1= time.time()
            #logger.info('start one rib...')
            df= pl.read_csv(path, sep='|',has_header=False,  ignore_errors= True)
            df.columns= utils.raw_fields

            df['mask']= pl.Series([True]* df.shape[0])
            # 获取pfx-peer_AS共现矩阵
            pfx_2_peer= df.pivot(values='mask', index='dest_pref', columns='peer_AS').fill_null(False)
            
            #  获取originAS-peer_AS共现矩阵（可得到：一个peer在AS层面的视野范围；一个AS能被多少个peer观察到）
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
        '''
        - description: 合并和处理各个采集点上得到的pfx2peer/ oriAS2peer共现矩阵。
        - args-> df_list {*}: 共现df列表
        - return {*}
        '''
        DF= df_list[0]
        index_tag= DF.columns[0]
        for id, df in enumerate(df_list[1:]):
            if isinstance(df, pl.DataFrame):
                df.columns =[index_tag]+ [f"{col}_{id}" for col in df.columns[1:]]
                DF= DF.join(df, on= index_tag, how= 'outer')
        DF= DF.fill_null(False)
        
        # 此时DF的情况：行上需区分v4,v6; 列上需合并AS号相同的peer
        ASset= set()
        cols= DF.columns[1:]
        for col in cols:
            curAS= col.split('_')[0]
            if curAS not in ASset:
                ASset.add(curAS)
                DF= DF.rename({col: curAS})
            else:
                DF[curAS]= DF[curAS]| DF[col]     # bool或运算
                DF= DF.drop(col)

        #DF_v4= DF.filter(pl.col(index_tag).str.contains(':')== False)
        #DF_v6= DF.filter(pl.col(index_tag).str.contains(':'))
        DF.select([
            pl.col('dest_pref'),
            pl.exclude('dest_pref').cast(pl.Int8)
        ]).to_csv(out_path)

    @staticmethod
    def get_pfx2peer_COmatrix_parall(ribs_dir='/data/fet/ribs_all_collector_20230228.0000/parsed/',out_path= './COmatrix/', processes=4):
        ''' - 输入各个采集点rib文件的列表, 获取pfx2peer/ oriAS2peer共现矩阵
        # TODO: 考虑改用` https://publicdata.caida.org/datasets/as-relationships/serial-1/20230301.as-rel.txt.bz2`
        '''
        utils.makePath(out_path)
        paths= sorted(glob.glob(ribs_dir+'*'))
        #paths= [paths[8],paths[13],paths[14],paths[4]]
        
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

class PeerSelector():
    '''- 用于选择最佳peer'''

    # 来自COmatrixPfxAndPeer类
    @staticmethod
    def _get_existing_COmat():
        '''- 拿到共现矩阵'''
        try:
            df= pl.read_csv('/data/fet/ribs_all_collector_20230228.0000/COmatrix/COmatrix_pfx_2_peer.csv')
        except:
            print('no COmatrix!')
            df= None
        return df
    
    @staticmethod
    def _df2graph(df= None):
        '''- 由 df['peer_AS', 'dest_pref','path'] 生成带权有向图.
        - 效率：操作97万行的df耗时10s, 3s拿到边集合, 7s造图 '''
        all_edge= ( df
            .groupby([ 'peer_AS', 'dest_pref' ])
                .tail(1)    # rib表中一个peer-pfx下还有多条路由，说明这个peerAS有多个peerIP，只保留一条路由即可。当然这不算严谨，可能会失去一些边关系。
                .drop_nulls()
                .select([
                #pl.col('dest_pref'),
                pl.col('path').str.split(' ').alias('path_list_raw'),
                pl.col('path').str.split(" ").arr.shift( -1 ).alias('path_list_sft')
            ])
            .explode( ['path_list_raw', 'path_list_sft'] )  # 2列展开
            .filter( (pl.col('path_list_sft') != None)&
                    (pl.col('path_list_raw') != pl.col('path_list_sft')) &
                    (~pl.col('path_list_sft').str.contains('\{'))
                    )       # 自此，每行的2列就是拓扑图中一个边的连接关系。 
            # .unique()       # 去重效果：去重前274万边，去重后11万边。这是因为不同路由(pfx)间有大量重复的边，特别是tier-1与运营商之间
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
        # 更新边的权重
        for u, v, w in G.edges(data='weight'):
            G[u][v]['weight'] = weights[(u, v)]
        return G

    @staticmethod
    def _greaterthan_avg_degree(G: nx.Graph):
        '''- 获取`大于平均度(3种)的节点数量`'''
        _df=pd.DataFrame({    
                            'gp_nb_nodes_gt_avg_tol_degree': dict(G.degree),
                            'gp_nb_nodes_gt_avg_out_degree': dict(G.out_degree()), 
                            'gp_nb_nodes_gt_avg_in_degree': dict(G.in_degree())
                        })
        res= _df.apply(lambda x: (x > x.mean()).sum()).to_dict()
        return res

    @staticmethod
    def _simple_feats_graph(G: nx.Graph, source_peer):
        '''- 获取图的一个特征字典'''
        # 数据准备：
        #Gsub, _= GraphBase.get_subgraph_without_low_degree(G)  

        # 特征集合1：无需k核子图就能即时获取的
        res1= {
            # 全图特征
            'gp_nb_of_nodes':       len(G.nodes),           # 节点数
            'gp_nb_of_edges':       len(G.edges),           # 边数
            'gp_density':           nx.density(G),          # 密度

            # 节点平均特征
                ## 度相关
            'nd_degree':            graphInterAS.avgNode(G.degree),        # 平均度
            'nd_in_degree':         graphInterAS.avgNode(G.in_degree),   # 平均入度
            'nd_out_degree':        graphInterAS.avgNode(G.out_degree),  # 平均出度
            'nd_degree_centrality': graphInterAS.avgNode(nx.degree_centrality, G),   # 平均度中心性
            'nd_pagerank':          graphInterAS.avgNode(nx.pagerank, G)
            
            # 混合特征：同时涉及全图特征、节点平均特征
            
        }
        # 特征集合2：需要K核子图才能即时获取(<1s)
        res2= {
            # 
            
            #
            #'nd_clustering':            graphInterAS.avgNode(nx.clustering, Gsub),
            #'nd_closeness_centrality':  graphInterAS.avgNode(nx.closeness_centrality, Gsub),
            #'nd_eigenvector_centrality':graphInterAS.avgNode(nx.eigenvector_centrality, Gsub),
            #
        }
        
        res3= PeerSelector._greaterthan_avg_degree(G)

        res= {
            'peer_degree': G.degree[str(source_peer)]}
        for r in [res1, res2, res3]:
            res.update(r)
        return res

    @staticmethod
    def select_peer_from_a_rib(path_or_df, method= 'simple'):
        '''- 从一个rib表中选出最佳的peer, 以致最小的路由数来构建全局拓扑
            - args-> path_or_df {`str | pl.df`}: 
            - args-> method
                - 当为默认值，最佳peer = 能观察到pfx的数量的peer
                - 当为其他值，需对每个peer下的路由提取信息，分别做全球AS拓扑图，
                根据图的属性等获取评分，分最高的peer最佳
            - return {list}: idx=0处的peer最佳
        '''
        if isinstance(path_or_df, str): # 已经解析好的rib表
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
            for cur_peer in peer_list:  ####for
                newdf= df.filter(pl.col('peer_AS')== cur_peer)
                G= PeerSelector._df2graph(newdf)
                #Gsub,_= GraphBase.get_subgraph_without_low_degree(G)

                cur_res= {
                    # peer的视野（能观察到pfx的数量）
                    #'peer_vision': COmat[str(cur_peer)].sum() if COmat else 0,
                    'peer_vision': newdf['dest_pref'].unique().shape[0],
                    'num_route':   newdf.shape[0],}
                cur_res.update( PeerSelector._simple_feats_graph(G, cur_peer) )
                result[cur_peer]= cur_res
                print(f'done: {cur_peer}')

            # 获取这张路由表中，每个peer的评分
            dfscore = pd.DataFrame(result)
            dfscore = pd.DataFrame(MinMaxScaler().fit_transform(dfscore.T).T, columns=dfscore.columns)
            dfscore = dfscore.sum()
            print(f"{dfscore=}")

            return dfscore.idxmax()


#######################
# 提取特征前，预处理原始消息。因为‘路由波动造成的无效波峰’影响时序曲线的正常表达
#######################

class UpdsMsgPreHandler():
    '''- 提取特征前，预处理原始消息。因为‘路由波动造成的无效波峰’影响时序曲线的正常表达
    '''

    @staticmethod
    def pfx_oriAS_mapping_from_rib(rib_path= ''):
        '''
        - description: 从一张rib表获取完整的全球prefix与originAS的映射。一般选取rrc00的最新rib表。
        - args-> rib_path {path or url}: 
            - 默认为(2G): `'https://data.ris.ripe.net/rrc00/latest-bview.gz'`
            - 也可用RIB汇总表(5G): `https://publicdata.caida.org/datasets/as-relationships/serial-1/20230301.all-paths.bz2`
        - 注：默认下当rib表5800万行，耗时 30~40s
        - return {`pl.df['pfx', 'originAS']`}: 一般地, pfx有100万左右; oriAS有7.7万左右。
        '''
        if rib_path=='' or rib_path.startswith('http'):
            # 下载，解析非常耗时
            if rib_path=='':
                url= 'https://data.ris.ripe.net/rrc00/latest-bview.gz'
            else:
                url= rib_path
            base_name= url.split('/')[-1]
            rib_path= os.getcwd()+'/'+ base_name+ '.txt'
            os.system(f"time wget {url}; time bgpdump -m {base_name} > {rib_path}; rm {base_name}")
                    
        # 20s, 读取
        df= utils.csv2df(rib_path)[['peer_AS', 'dest_pref', 'path']]
        # 20s, 找到视野最大的peerAS
        max_peer= (df.groupby('peer_AS').agg(pl.col('dest_pref').unique().count())
            .sort('dest_pref', reverse=True)[0,0])
        # 1s, 拿到pfx和oriAS
        df= (df.filter((pl.col('peer_AS')== max_peer))
                .groupby('dest_pref').agg(
                    pl.col('path').last().str.split(' ').arr.last()
                )
                .sort('dest_pref', reverse=False)
                .rename({'dest_pref':'pfx', 'path':'originAS'})
                .filter(~(pl.col('originAS').str.contains('\{')))    # 过滤掉`{ASN}`的特殊情况
        )
        return df

    @staticmethod
    def del_useless_updmsg():
        '''
        - description: 删除无用的宣告消息。何为无用：属于路由波动的条目
        - method: 
            - 以每分钟为最小单元来判断upds中是否有`波动路由`

        - return {*}
        '''
        pass


#######################
# 分析特征
#######################

class FeatMatHandler:
    '''- 得到特征矩阵后进行的加工操作：如谱残差'''
    @staticmethod
    def spectral_residual(data, smooth_window=1):
        '''
        - description: 
        - args-> data {*}: 一个特征的时序数据
        - args-> smooth_window {*}: 较大的窗口可能会捕捉到更低频的周期成分，而较小的窗口可能会捕捉到更高频的周期成分。
        - return {*}
        '''
        data_fft = fft(data)
        # 计算幅度谱和相位谱
        amplitude_spectrum = np.abs(data_fft)
        phase_spectrum = np.angle(data_fft)
        log_data = np.log(amplitude_spectrum)  
        # 平滑幅度谱
        smoothed_amplitude = np.convolve(log_data, np.ones(smooth_window) / smooth_window, mode='same')
        # 计算谱残差
        spectral_residual = log_data - smoothed_amplitude
        residual_signal = np.real(ifft(np.exp(spectral_residual + 1j * phase_spectrum)))
        # 计算异常分数（平方值）
        anomaly_scores = np.square(residual_signal)

        return anomaly_scores

class Filter:
    '''- 进行特征过滤。'''
    def __init__(self, df:pd.DataFrame):
        df= df.fillna(0.0)        
        self.df= df
        self.x= df.iloc[:,2:-1]
        self.y= df.iloc[:,-1]

    def variance_filter(self,thd):
        '''- 方差过滤: excluding low variance feats, return DF filtered several feats'''
        selector_vt = VarianceThreshold( thd )
        x_varThd = selector_vt.fit_transform(self.x)
        return pd.DataFrame(x_varThd, columns= selector_vt.get_feature_names_out())

    def chi2_filter(self, x_varThd):        
        ''' 
        description: to get the filtered feats which p_value >0.05 from chi2 and the rank(descending) of all feats by chi2
        param {*} self
        param {DF} x_varThd
        return {DF, DF} x_chi2[rows* new_feats], df_chi2[all_feats* 1]
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

    '''def RFE_filter(self, x_varThd):
        svc= SVC( kernel='linear')
        rfecv= RFECV(estimator= svc, 
            step=1, 
            cv= StratifiedKFold(2), 
            scoring= 'accuracy', 
            min_features_to_select=1)
        rfecv.fit(x_varThd, self.y)
        return rfecv'''

    def redu_filter(self, x_flted1: pd.DataFrame, df_ranked1: pd.DataFrame, redu_thd: float):
        '''
        description: filter redundant feats by deleting corr between feats
        param {*} self
        param {pd} x_flted1     : [rows*part_feats]
        param {pd} df_ranked1   : [all_feats*1]
        param {float} redu_thd
        return {pd} rele_table  : [num of deleted feats* 3]
        return {pd} x_del_corr  : [rows*part_feats]
        '''
        df_corr= abs(x_flted1.corr(method= 'spearman'))
        df_corr_= df_corr[df_corr> redu_thd]
        df_corr_= pd.DataFrame( np.tril(df_corr_.values, -1), index= df_corr_.index, columns= df_corr_.columns)
        df_corr_= df_corr_.replace(np.nan, 0)

        #fig= plt.figure(figsize=(10,8))
        #sns.heatmap(df_corr_, annot= False)
        rele_table=[]   # 相关系数(spearman)高的特征对的集合, (相关系数，feat1(to del), feat2(to save))
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
                # 精简x特征子集并验证其模型效果
        new_cols= df_corr_.columns
        x_del_corr= x_flted1[new_cols ]
        # cross_val_score(RFC(n_estimators=10,random_state=0),x_del_corr ,y,cv=5).mean()

        rele_table= pd.DataFrame(rele_table, columns=['corr', 'feat_del', 'feat_save'])
        return rele_table, x_del_corr
                
    def get_redu_thd(self, x_flted1: pd.DataFrame, df_ranked1: pd.DataFrame):
        '''
        description: to find hyperparameters redundant threshold in df.corr()
        return {*} max_thd
        return {list(tuple)} mdl_score  : for plotting to find max_thd
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
        max_thd, mdl_score= self.get_redu_thd( x_chi2, df_chi2)
        rele_table, x_del_corr= self.redu_filter( x_chi2, df_chi2, redu_thd= max_thd)
        return df_res


def txt2df(paths: Union[str, list], need_concat:bool= True):
    '''
    - description: 读取数据文件为pandas.df对象。
    - args-> `paths` {*}: 若为str, 则为一个文件路径;若为list, 则为一组文件路径, 可选择concat为一个df, 或输出一组df
    - args-> `need_concat` {bool}: 
    - return {*} 一个df (来自一个文件, 或多个文件的合并); 一组df (来自多个文件分别读取)
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

            # 合并后的大帧做些修改
            df['time_bin']= df.index
            label_map= {'normal': 0, 'hijack': 1, 'leak': 1, 'outage': 1}   # 简化为二分类问题
            df['label']= df['label'].map( label_map )

            return df
        else:
            return df_list


def df2Xy(df, test_size= 0):
    '''- arg(df): 一般第1,2列为time_bin和date, 最后一列为label
       - 有归一化操作。
       - return: 4个子df/series, 即X_train/test, y_train/test; 或2个df/series, 即 X,y (默认, test_size=0)'''
    # load the time series data
    df = df.iloc[:, 2:]

    # separate the input and output columns
    X = df.iloc[:, :-1]
    y = df.iloc[:, -1]

    if test_size==0:
        return X, y
    else:
        # split the data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, shuffle=True)  # 打乱后，逻辑回归的模型效果原地起飞
        # normalize the data
        scaler = StandardScaler()
        X_train = scaler.fit_transform(X_train)
        X_test = scaler.transform(X_test)

        return X_train, X_test, y_train, y_test


def multi_collector_plot(event_path:str):
    '''针对单事件、多采集器的数据。把多采集器的数据整合到一个大图的多个子图中
    - arg: 事件的任意一个数据文件的路径'''
    import matplotlib.pyplot as plt
    import os
    # 先拿到事件相关的所有文件
    event_name= event_path.split('__')[-2]
    dir_name  = os.path.dirname(event_path)
    lis= os.listdir(dir_name )
    lis= [ dir_name+'/'+ s for s in lis if event_name in s ]
    lis.sort()
    lis= lis[:]     # 自定义裁剪df个数

    # 后作大图: 子图矩阵整合的形式
    nrows= 9; ncols= 2
    fig, axes= plt.subplots(nrows= nrows, ncols= ncols, figsize= (10,10) )     # 
    
    plt.suptitle( event_name, fontsize=14)              # 主图标题
    #plt.subplots_adjust( top= 1)                       # 主图标题与主图上边界的距离

    for i in range(nrows):
        for j in range(ncols):
            title= simple_plot( lis[i*2+j], axes[i][j])
            if i == 0:
                axes[i][j].legend(prop={'size': 6})                     # 仅在第一行的
            if i != nrows-1:
                axes[i][j].set_xticklabels([])          # 仅在最后一行的子图有x刻度值
                axes[i][j].set_xlabel('')               # 仅在最后一行的子图有xlabel
            else:
                axes[i][j].set_xlabel('time')
    plt.tight_layout()                                  # 自动调整整体布局
    plt.savefig(event_name+ "18个采集器的子图矩阵对比.jpg", dpi=300)               # 高分辨率把图导出
    
def simple_plot(file_path:str, front_k= -1, has_label= True, subax= None, subplots= False, need_scarler= False):
    '''针对单事件、单采集器的数据。作图观测波峰，以确定真实label'''
    
    # 准备图标题
    lis= file_path.split('__')
    try:
        title= lis[-2]+ '__'+ lis[-1][:-4]
    except:
        pass

    # 准备df，并预处理
    df= pd.read_csv(file_path)
    print(df.shape)
        # 把日期换成只显示时分
    df['date']= df['date'].str.slice(11, 16)
        # 数据归一化
    if need_scarler:
        scaler = MinMaxScaler()
        df.iloc[:, 2:-1]= scaler.fit_transform(df.iloc[:, 2:-1])  # 最后一列是label（str）
    
    # 画图
    num_feats= len(df.columns)-2
    if front_k==-1:
        y= df.columns[2: ]
    else:
        y= df.columns[2: front_k+2]
        num_feats= front_k
    if not subplots:
        num_feats= 4
    ax= df.plot(x='date',
            y= y,
            #y= ['v_IGP','v_pfx_t_cnt', 'v_pp_W_cnt', 'v_A', 'is_longer_unq_path'] ,
            #title= title,
            figsize= (10, num_feats),
            subplots= subplots,
            legend= True,
            #logy=True,
            ax= subax,
        )
    #ax.set_title( title,  fontsize= 10)    # 子图标题 .split('__')[-1]

        # 造阴影区域
    if has_label:
        rows_label= df['label'][df['label'] != 'normal'].index     #  'normal'
        rows_label= rows_label.tolist()
        rows_label.append(-1)
                # 把一堆断断续续的数字变成一段一段的元组，存入 sat_end_llist
        sat_end_list= []
        ptr1= 0; ptr2=0
        while ptr2< len(rows_label)-1:
            if rows_label[ptr2]+ 1== rows_label[ptr2+1]:    # 即下一个数字是连续的
                ptr2+=1
                continue
            else:
                sat_end_list.append( (rows_label[ptr1], rows_label[ptr2]))
                ptr2+=1
                ptr1= ptr2

                # 造多个阴影
        '''for tup in sat_end_list:
            ax.axvspan(tup[0], tup[-1], color='y', alpha= 0.35)'''

                # 有并列子图时的造阴影
        if subplots:
            for a in ax:
                for tup in sat_end_list:
                    a.axvspan(tup[0], tup[-1], color='y', alpha= 0.35)

    plt.tight_layout()
    plt.savefig('temp.jpg', dpi=300)
    return


'''def collectFeat(event: list):
    ''输入事件列表，执行特征采集全过程，存储特征矩阵为.csv''
    ee= FET.EventEditor()
    ee.addEvents(event)
    
    raw_data_path= "/data/fet/"   # "/data/fet/"    '/home/huanglei/work/z_test/Dataset/'
    fet= FET.FET(raw_dir= raw_data_path,increment=5)
    fet.setCustomFeats( 'ALL' )
    p= fet.run()'''




if __name__=='__main__':
    # step1 看哪个rrc合适
    data= MRTfileHandler.draw_collectors_file_size('RosTel_hijack_50', '20170425.0000', '20170430.0000', ('20170426.2030','20170426.2300'))

    #peer_info[0]





