# BGP特征提取工具 

## 组织结构

### 1. 项目路径

```
fastFET  
    ├── BGPMAGNET/      BGP原始数据下载工具(by倪泽栋)  
    ├── bgpToolKit.py   BGP异常检测中常用的辅助函数
    ├── collectData.py  主模块：数据收集，包括下载、解析 
    ├── drawing.py      特征提取后的数据作图  
    ├── event_list.csv　要采集的事件列表  
    ├── featGraph.py    图特征采集功能集合   
    ├── featTradition.py传统特征采集功能集合  
    ├── featTree.py     特征树，集成所有特征及其pl.Expr  
    ├── FET.py          主模块：特征采集  
    ├── logConfig.json  日志配置  
    ├── MultiProcess.py 多进程工具   
    ├── README.md 
    ├── RIPEStatAPI.py  `https://stat.ripe.net/data/*`常用接口集合 
    └── utils.py        工具集合   
``` 

### 2. cwd 默认路径
```
cwd  
├── Dataset  
│   ├── features/               特征提取最终数据  
│   │　　└── date_event_monitor.csv  
│   ├── MOAS/                   提取过程中属于multi-origin-AS的消息  
│   ├── raw_cmpres              原始数据下载存放路径    
│   │   ├── rrc00/    
│   │   └── routeviews./    
│   └── raw_parsed              原始数据解析存放路径    
│       └── 事件名  
│           └── 采集器名/  
└── log                         日志路径    
    ├── errors.log  
    └── info.log 
```

## 使用方法

### - 提取特征

创建特征提取工具对象`fet`, 添加目标事件信息, 指定特征集合, 开始提取特征：    
```python
from fastFET import FET
ev= FET.EventEditor()
event1= "RU_AS_hijack_twitter,2022/03/28 11:05:00, 2022/03/28 12:06:00, rrc22"
ev.addEvents([event1,])

fet= FET.FET(raw_dir= 'Dataset/', increment=4)
fet.setCustomFeats(["volume"])
feat_path= fet.run()
```
- `addEvents`方法添加此次要采集特征的事件信息列表。格式：事件名；起始时间；结束时间（可缺省）；采集器（可多个，用`,`相隔）。
- `fet= FET.FET()`，特征采集器实例。
- `fet.setCustomFeats([...])`，自定义所采集特征类型。3种方法：全量采集；按类别采集；按单个特征采集 (详见该方法注释)。
- `feats= fet.getAllFeats()`，查看该工具现已集成的特征列表。
- `fet.run()`，运行主函数。参数`only_rib= True`时，实例仅用于对rib表图特征采集。返回特征存放路径。

### - 特征分析
作图分析特征时序变化：
```python
from fastFET.drawing import simple_plot
from glob import glob
ev_name= event1.split(',')[0]
p= glob(f"{feat_path}*{ev_name}*")[0]
simple_plot(p, front_k= 5, subplots=False)
```

### - 其他工具
```python
# 获取IP经纬度和地址
from fastFET.bgpToolKit import CommonTool
dic= CommonTool.ip2coord(['8.8.8.8'])

# 收集RIPE-NCC和RouteViews项目中的peers
from fastFET.bgpToolKit import PeersData
dic_peer= PeersData.get_peers_info()
dic_rrc = PeersData.get_rrc_info()

# 画图：快速查看各采集点消息量的宏观走势
from fastFET.bgpToolKit import MRTfileHandler
MRTfileHandler.draw_collectors_file_size(time_start= '20211004.1200', time_end= '20221004.1200')

# 从所有采集点的rib表获取全局 prefix和peer_AS的共现矩阵
from fastFET.bgpToolKit import COmatrixPfxAndPeer
COmatrixPfxAndPeer.get_pfx2peer_COmatrix_parall()
rank= COmatrixPfxAndPeer.peers_rank_from_COmat()    # 计算全球peerAS的视野排名

# 从一个rib表中选出最佳的peers列表
from fastFET.bgpToolKit import PeerSelector
lis= PeerSelector.select_peer_from_a_rib(f"{rib_path}")

# 特征提取前的数据清洗
from fastFET.bgpToolKit import UpdsMsgPreHandler
df_new= UpdsMsgPreHandler.run_cut_peak()

# 作图：直接针对updates消息的分析工具
from fastFET.bgpToolKit import RawMrtDataAnaly
rda= RawMrtDataAnaly()
```

更多工具实现详见： `./bgpToolKit.py`, `./RIPEStatAPI.py`

## 特征字段说明
- 共139字段，含序列号，日期，136个特征，及标签。

类别 | 字段 | 解释
|---|---|--- 
\- | time_bin | 时间序列号
\- | date | 日期
volume | v_total | 消息总数
\- | v_A | 宣告消息总数
\- | v_W | 撤销消息总数
\- | v_IGP | 属于IGP的消息总数
\- | v_EGP | 属于EGP的消息总数
\- | v_ICMP | 属于IMCOMPLETE的消息总数
\- | v_peer | 不同peer的数量
\- | v_pfx_t_cnt | 不同prefix的数量
\- | v_pfx_t_avg | 不同prefix出现过的平均次数
\- | v_pfx_t_max | 不同prefix出现过的最大次数
\- | v_pfx_A_cnt | 属宣告的不同prefix的数量
\- | v_pfx_A_avg | 属宣告的不同prefix出现过的平均次数
\- | v_pfx_A_max | 属宣告的不同prefix出现过的最大次数
\- | v_pfx_W_cnt | 属撤销的不同prefix的数量
\- | v_pfx_W_avg | 属撤销的不同prefix出现过的平均次数
\- | v_pfx_W_max | 属撤销的不同prefix出现过的最大次数
\- | v_pp_t_cnt | 不同peer-prefix对的数量
\- | v_pp_t_avg | 不同peer-prefix对出现过的平均次数
\- | v_pp_t_max | 不同peer-prefix对出现过的最大次数
\- | v_pp_A_cnt | 属宣告的不同peer-prefix对的数量
\- | v_pp_A_avg | 属宣告的不同peer-prefix对出现过的平均次数
\- | v_pp_A_max | 属宣告的不同peer-prefix对出现过的最大次数
\- | v_pp_W_cnt | 属撤销的不同peer-prefix对的数量
\- | v_pp_W_avg | 属撤销的不同peer-prefix对出现过的平均次数
\- | v_pp_W_max | 属撤销的不同peer-prefix对出现过的最大次数
\- | v_oriAS_t_cnt | 源AS的数量
\- | v_oriAS_t_avg | 源AS的平均出现次数
\- | v_oriAS_t_max | 源AS的最大出现次数
\- | v_oriAS_peer_cnt | 不同peer-originAS对的数量
\- | v_oriAS_peer_avg | 不同peer-originAS对出现过的平均次数
\- | v_oriAS_peer_max | 不同peer-originAS对出现过的最大次数
\- | v_oriAS_pfx_cnt | 不同prefix-originAS对的数量
\- | v_oriAS_pfx_avg | 不同prefix-originAS对出现过的平均次数
\- | v_oriAS_pfx_max | 不同prefix-originAS对出现过的最大次数
\- | v_oriAS_pp_cnt | 不同peer-prefix-originAS对的数量
\- | v_oriAS_pp_avg | 不同peer-prefix-originAS对出现过的平均次数.
\- | v_oriAS_pp_max | 不同peer-prefix-originAS对出现过的最大次数.
path | path_len_max | 最大路径长度
\- | path_len_avg | 平均路径长度
\- | path_unq_len_max | 去重后的最大路径长度
\- | path_unq_len_avg | 去重后的平均路径长度
AS | As_total_cnt | 出现过的AS的数量
\- | As_total_avg | 不同AS出现过的平均次数
\- | As_total_max | 不同AS出现过的最大次数
\- | AS_rare_avg | 所有消息的路径中含有稀有AS的总共数量
\- | AS_rare_sum | 一条消息的路径中含有稀有AS的最大数量
dynamic | is_WA | 属于撤销后宣告的消息数
\- | is_AW | 属于宣告后撤销的消息数
\- | is_WAW | 属于撤销-宣告-撤销的消息数
\- | is_longer_path | 路径变长的消息数
\- | is_shorter_path | 路径变短的消息数
\- | is_longer_unq_path | 去重后路径变长的消息数
\- | is_shorter_unq_path | 去重后路径变短的消息数
\- | is_new | 属于全新宣告的消息数
\- | is_dup_ann | 属于重复宣告的消息数（仅prefix重复）
\- | is_AWnA | 属于宣告-撤销多次-宣告的消息数
\- | is_imp_wd | 属于隐式撤销的消息数（重复宣告，但其他属性变化）
\- | is_WnA | 属于撤销多次-宣告的消息数
\- | is_AWn | 属于宣告-多次撤销的消息数
\- | is_AnW | 属于多次宣告-撤销的消息数
\- | is_WAn | 属于撤销-多次宣告的消息数
\- | is_dup_wd | 属于重复撤销的消息数
\- | is_dup | 属于重复宣告的消息数（完全重复）
\- | is_flap | 属于宣告-撤销-宣告，且属性完全不变的消息数
\- | is_NADA | 属于宣告-撤销-宣告，但属性有变化的消息数
\- | is_imp_wd_spath | 属于路径属性不变的隐式撤销的消息数
\- | is_imp_wd_dpath | 属于路径属性变化的隐式撤销的消息数
\- | type_0 | 针对同一prefix，源AS改变了的消息数(MOAS)
\- | type_1 | 针对同一prefix，path中第2个AS改变了的消息数
\- | type_2 | 针对同一prefix，path中第3个AS改变了的消息数
\- | type_3 | 针对同一prefix，path中第4个AS改变了的消息数
\- | ED_max | 同一peer-prefix下，最大的编辑距离值的消息数
\- | ED_avg | 同一peer-prefix下，平均的编辑距离值的消息数
\- | ED_0 | 同一peer-prefix下，编辑距离为0的消息数
\- | ED_1 ~ ED_10 | 同一peer-prefix下，编辑距离为1~10的消息数
ratio | ratio_firstOrder | 最活跃的宣告前缀/宣告总数（即 `v_pfx_A_max / v_A`）
\- | ratio_ann | 宣告量占更新消息总量之比（即`v_A / v_total`）
\- | ratio_wd | 撤销量占更新消息总量之比（即`v_W / v_total`）
\- | ratio_origin0 | IGP占宣告量之比（即`v_IGP / v_A`）
\- | ratio_origin1 | EGP占宣告量之比（即`v_EGP / v_A`）
\- | ratio_origin2 | IMCOMPLETE占宣告量之比（即`v_ICMP / v_A`）
\- | ratio_dup_ann | 完全重复宣告占宣告量之比（即`is_dup_ann / v_A`）
\- | ratio_flap | 属性完全不变的宣-撤-宣占宣告量之比（即`is_flap / v_A`）
\- | ratio_NADA | 属性有变化的宣-撤-宣占宣告量之比（即`is_NADA / v_A`）
\- | ratio_imp_wd | 隐式撤销占宣告量之比（即`is_imp_wd / v_A`）
\- | ratio_imp_wd2 | 隐式撤销占隐式撤销+撤销之比（即`is_imp_wd / (is_imp_wd+ v_W)`）
\- | ratio_exp_wd | 真正撤销占隐式撤销+撤销之比（即`v_W / (is_imp_wd+ v_W)`）
\- | ratio_imp_wd_dpath | 路径属性不同的隐式撤销占隐式撤销之比（即`is_imp_wd_dpath / is_imp_wd`）
\- | ratio_imp_wd_spath | 路径属性相同的隐式撤销占隐式撤销之比（即`is_imp_wd_spath / is_imp_wd`）
\- | ratio_new | 全新宣告占宣告量之比（即`is_new / v_A`）
\- | ratio_wd_dups | 重复撤销占撤销量之比（即`is_dup_wd / v_W`）
\- | ratio_longer_path | 更长路径宣告占宣告量之比（即`is_longer_path / v_A`）
\- | ratio_shorter_path | 更短路径宣告占宣告量之比（即`is_shorter_path / v_A`）
\- | ratio_longer_path2 | 更长路径宣告占更长/短宣告量之比（即`is_longer_path / (is_longer_path+ is_shorter_path)`）
\- | ratio_shorter_path2 | 更短路径宣告占更长/短宣告量之比（即`is_shorter_path / (is_longer_path+ is_shorter_path)`）
node_level_graph | nd_degree_centrality | 节点平均度中心性
\- | nd_node_clique_number | 节点平均最大集团数
\- | nd_number_of_cliques | 节点平均集团数
\- | nd_closeness_centrality | 节点平均紧密中心性
\- | nd_betweenness_centrality | 节点平均中介中心性
\- | nd_local_efficiency | 节点平均局部效率
\- | nd_harmonic_centrality | 节点平均谐波中心度
\- | nd_eigenvector_centrality | 节点平均特征向量中心度
\- | nd_pagerank | 节点平均重要度排名
\- | nd_clustering | 节点平均聚类中心性
\- | nd_triangles | 节点平均三角形数量 
\- | nd_eccentricity | 节点平均偏心率
\- | nd_average_shortest_pth_length | 节点平均最短路径长度
\- | nd_load_centrality | 节点平均负载中心性
\- | nd_degree | 节点平均度数
\- | nd_square_clustering | 节点平均平方聚类系数
\- | nd_average_neighbor_degree | 节点平均邻居度数
AS_level_graph | gp_nb_of_nodes | 总节点数
\- | gp_nb_of_edges | 总边数
\- | gp_diameter | 最大偏心率
\- | gp_assortativity | 同配性
\- | gp_largest_eigenvalue | 最大特征值
\- | gp_algebraic_connectivity | 代数连通度
\- | gp_effective_graph_resistance | 有效图阻抗
\- | gp_symmetry_ratio | 对称率
\- | gp_natural_connectivity | 自然连通度
\- | gp_node_connectivity | 节点连通度
\- | gp_edge_connectivity | 边连通度
\- | gp_weighted_spectrum_3 | 三方加权频谱
\- | gp_weighted_spectrum_4 | 四方加权频谱
\- | gp_percolation_limit | 渗透极限
\- | gp_nb_spanning_trees | 生成树数量
\- | label	|	异常类型标签


## 特征补充
下述特征留待实现

字段            |  说明
 ----           |  ----
ConcentratRatio |  前三个最活跃的宣告前缀/宣告总数（即 `vol_ann_pfx_max / v_A`）


## 其他
### BGP原始数据处理中注意事项 
1. BGP RAW DATA: 采集时间间隔不统一，如rrc00中20030723.0745之前为15min（且时刻不固定），之后为5min（时刻固定）。
2. MRT文件解析后，path 字段可能存在`{}`形式，如下: 
    - 58057 6939 4635 4788 38044 23736
    - 58057 6939 4635 4788 38044 {23736}
    - 58057 6939 1299 2603 2603 2603 6509 {271,7860,8111,53904}
3. `stat.ripe.net`的API获取的路由，path字段可能存在`[]`形式。
3. `Route-Views`中的MRT文件名格式不严谨，经常出现无规律的时间戳。

### 数据分析中观测到的一些现象
- 劫持震荡：当`is_MOAS`很大，而`vol_oriAS_peer_pfx`或`vol_oriAS_pfx`很小时，说明存在一个prefix反复被多个AS宣告的情况。
- outage类型难溯源