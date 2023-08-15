#! /usr/bin/env python
# -*-coding:utf-8-*-
'''
- Description: 可视化工具集合
- version: 1.0
- Author: JamesRay
- Date: 2023-03-21 22:44:06
- LastEditTime: 2023-07-04 03:39:48
'''
import pandas as pd
import polars as pl
import numpy as np
import seaborn as sns
import datetime as dt
from fastFET import utils

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker



class DataDistrib():
    '''- 数据分布相关，如：热力图，PDF，CDF'''

    @staticmethod
    def draw_CDF(data: pd.Series, title= None):
        sorted_data = np.sort(data)
        y = np.arange(len(sorted_data))/float(len(sorted_data))
        plt.plot(sorted_data, y)
        if title:
            plt.title(title)
        #plt.xlabel('Value')
        plt.ylabel('CDF')
        plt.show()

    @staticmethod
    def draw_PDF(data: pd.Series):
        sns.kdeplot(data)
        plt.show()

    @staticmethod
    def draw_PDF_CDF_of_distDF(path_of_dist_df, path_out= None):
        '''- 用于绘制`距离方阵`的pdf, cdf。
        '''       
        df= pd.read_csv(path_of_dist_df).set_index('Unnamed: 0')        
        hist, bin_edges = np.histogram(df.values.flatten(), bins='auto', density=True)
        cumulative = np.cumsum(hist) * np.diff(bin_edges)

        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(10, 4))
        axes[0].plot(bin_edges[:-1], hist, color='blue')
        axes[0].set_xlabel('Distance', fontsize=12)
        axes[0].set_ylabel('PDF', fontsize=12)
        axes[0].tick_params(labelsize=10)
        
        formatter = ticker.ScalarFormatter(useMathText=True)
        formatter.set_scientific(True)
        formatter.set_powerlimits((-2, 2))
        axes[0].yaxis.set_major_formatter(formatter)
        
        axes[1].plot(bin_edges[:-1], cumulative, color='red')
        axes[1].set_xlabel('Distance', fontsize=12)
        axes[1].set_ylabel('CDF', fontsize=12)
        axes[1].tick_params(labelsize=10)

        fig.suptitle('Distance Distribution', fontsize=16, fontweight='bold')
        for ax in axes:
            ax.grid(True, linestyle='--', linewidth=0.5)

        if path_out:
            plt.savefig(path_out, dpi=300)
            print(f"{path_out=}")
        plt.show()

    @staticmethod
    def draw_heat_map(path_of_dist_df, path_out=None):
        '''- 画df的热力图
        '''
        plt.style.use('seaborn-white')
        plt.imshow(path_of_dist_df, cmap='YlOrRd', aspect='equal')
        plt.colorbar()
        plt.xticks([])
        plt.yticks([])
        plt.xlabel('peers AS')
        plt.ylabel('peers AS')
        plt.show()
        if path_out:
            plt.savefig(path_out, dpi=300)
            print(f"{path_out=}")

class RawMrtDataAnaly():
    pass
    # see bgpToolKit


def multi_collector_plot(event_path:str):
    '''将多采集器的数据整合到一个大图的多个子图中
    '''
    import matplotlib.pyplot as plt
    import os
    event_name= event_path.split('__')[-2]
    dir_name  = os.path.dirname(event_path)
    lis= os.listdir(dir_name )
    lis= [ dir_name+'/'+ s for s in lis if event_name in s ]
    lis.sort()
    lis= lis[:]

    nrows= 9; ncols= 2
    fig, axes= plt.subplots(nrows= nrows, ncols= ncols, figsize= (10,10) )
    
    plt.suptitle( event_name, fontsize=14)

    for i in range(nrows):
        for j in range(ncols):
            title= simple_plot( lis[i*2+j], axes[i][j])
            if i == 0:
                axes[i][j].legend(prop={'size': 6})                      
            if i != nrows-1:
                axes[i][j].set_xticklabels([])
                axes[i][j].set_xlabel('')
            else:
                axes[i][j].set_xlabel('time')
    plt.tight_layout()
    #plt.savefig(event_name+ "采集器对比.jpg", dpi=300)
    
def simple_plot(file_path:str, front_k= -1, has_label= True, subax= None, subplots= False, need_scarler= False):
    '''
    - description: 作图：特征趋势观察
    - args-> file_path {str}: 
    - args-> front_k {*}: 对前k列作图
    - args-> has_label {*}: 特征数据中有无label列
    - args-> subax {*}: 
    - args-> subplots {*}: 每列数据单独一个子图
    - args-> need_scarler {*}: 需要归一化
    - return {*}
    '''    
    lis= file_path.split('__')
    try:
        title= lis[-2]+ '__'+ lis[-1][:-4]
    except:
        pass
    
    df= pd.read_csv(file_path)
    print(df.shape)
    df['date']= df['date'].str.slice(11, 16)
    if need_scarler:
        from sklearn.preprocessing import MinMaxScaler
        scaler = MinMaxScaler()
        df.iloc[:, 2:-1]= scaler.fit_transform(df.iloc[:, 2:-1])
    
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
            #title= title,
            figsize= (10, num_feats),
            subplots= subplots,
            legend= True,
            #logy=True,
            ax= subax,
        )
    #ax.set_title( title,  fontsize= 10)

    if has_label:
        if df['label'].dtype== 'int64':
            rows_label= df['label'][df['label'] != 0].index
        else:
            rows_label= df['label'][df['label'] != 'normal'].index     #  'normal'
        rows_label= rows_label.tolist()
        rows_label.append(-1)
        sat_end_list= []
        ptr1= 0; ptr2=0
        while ptr2< len(rows_label)-1:
            if rows_label[ptr2]+ 1== rows_label[ptr2+1]:
                ptr2+=1
                continue
            else:
                sat_end_list.append( (rows_label[ptr1], rows_label[ptr2]))
                ptr2+=1
                ptr1= ptr2

        if subplots:
            for a in ax:
                for tup in sat_end_list:
                    a.axvspan(tup[0], tup[-1], color='y', alpha= 0.35)
        else:
            for tup in sat_end_list:
                ax.axvspan(tup[0], tup[-1], color='y', alpha= 0.35)

    plt.tight_layout()
    plt.savefig(f'simple_plot__{title}.jpg', dpi=300)
    return




