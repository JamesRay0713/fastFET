#! /usr/bin/env python
# coding=utf-8
'''
- Description: BGP异常检测中常用的辅助函数
- version: 1.0
- Author: JamesRay
- Date: 2023-02-06 13:10:54
- LastEditTime: 2023-02-24 09:41:52
'''
import os, json, time, re
import requests

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, MinMaxScaler
import matplotlib.pyplot as plt
from fastFET import FET

#######################
#  peers的研究
#######################
from geopy.geocoders import Bing
import geoip2.database

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

            print(f'done {count} ...')
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
                
            print(f'done: {count} coord2city')
            count+=1
        reader.close()
        return res

class PeersData(object):
    '''收集RIPE-NCC和RouteViews项目中的peers信息, 用于观察peers分布等'''

    @staticmethod
    def fromRV():
        '''
        - description: 采集来自RV的原始数据, 删了peerIP-v6部分
        - return {dict}: `{ collector: {ip1: {'asn':, 'ip':, 'v4_pfx_cnt': }, ip2: {},...}, ...}`
        - return {list}: `[ip, ...]`
        '''
        url= "http://www.routeviews.org/peers/peering-status.html"
        resRV={}
        respon= requests.get(url).text
        if not respon:
            print('* * * Please crawl \'%s\' again.' % url)
            return resRV
        rawList= re.findall('route-view.+', respon)
        
        IPs= set()
        for row in rawList:                       
            rowlist= row.split()
            if ':' in rowlist[2]:   # 把peerIP-v6排除
                continue
            
            collector= re.search('.*(?=.routeviews.org)', rowlist[0]).group()
            if collector not in resRV.keys():
                resRV[collector]= {}
                #print('start collecting with %s ~' % collector)

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
        - return {list}: `[{'asn', 'ip', 'v4_prefix_count', 'longitude', 'latitude', 'collector'}, {}, ...]`
        '''
        rv_info, rv_ips= PeersData.fromRV()
        rc_info, rc_ips= PeersData.fromRRC()

        ip_map= ip2coord(set(rv_ips+ rc_ips))

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
        print( f"rrc: {len( res['rrc'])} peers.\nrou: {len( res['rou'])} peers.\n### all peers info stored at `{path_out}`")

        return res

    @staticmethod
    def get_rrc_info(path_in= './peers_info.json', path_out= 'peers_info_about_collector.csv'):
        '''
        - description: 获取每个rrc的peers数量、城市列表. 这是对`get_peers_info()`输出的汇总。
        '''
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

#######################
# 真实事件分析所需接口
#######################






#######################
#
#######################

def txt2df(paths:"str|list", need_concat:bool= True):
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
    
def simple_plot(file_path:str, subax= None, need_scarler= False, front_k= 2):
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
    ax= df.plot(x='date',
            y= df.columns[2:2+ front_k],
            #y= ['v_IGP','v_pfx_t_cnt', 'v_pp_W_cnt', 'v_A', 'is_longer_unq_path'] ,
            #title= title,
            figsize= (10,4),
            subplots= False,
            legend= True,
            #logy=True,
            ax= subax,
        )
    #ax.set_title( title,  fontsize= 10)    # 子图标题 .split('__')[-1]
        # 造阴影区域
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
    for tup in sat_end_list:
        ax.axvspan(tup[0], tup[-1], color='y', alpha= 0.35)

            # 有并列子图时的造阴影
    '''for a in ax:
        for tup in sat_end_list:
            a.axvspan(tup[0], tup[-1], color='y', alpha= 0.35)'''
    plt.tight_layout()
    plt.savefig('temp.jpg', dpi=300)
    return


def collectFeat(event: list):
    '''输入事件列表，执行特征采集全过程，存储特征矩阵为.csv'''
    ee= FET.EventEditor()
    ee.addEvents(event)
    
    raw_data_path= "/data/fet/"   # "/data/fet/"    '/home/huanglei/work/z_test/Dataset/'
    fet= FET.FET(raw_dir= raw_data_path,increment=5)
    fet.setCustomFeats( 'ALL' )
    p= fet.run()




if __name__=='__main__':
    # 得到RIS的peers信息
    peer_info= PeersData.get_peers_info()
    #peer_info[0]





