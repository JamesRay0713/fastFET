## bgpmagnet使用说明

安装依赖：pip install requestments.txt

函数调用在download.py中，只需按需求修改其中的参数即可

```python
if __name__=="__main__":
    bgpdbp=downloadByParams(
        worker_num=30,
        urlgetter=bgpGetter(base_params( 
            start_time="2022-04-14-00:00",
            end_time="2022-04-14-23:59",
            bgpcollectors="all",
            data_type=BGP_DATATYPE["ALL"]
        )),
        destination="./0414",
        save_by_collector=0
    )
    bgpdbp.start_on()
```

- start_time/end_time
  - "%Y-%m-%d-%H:%M"格式
  - 月/日需要保证是两位，例如2022年4月1日 —> 2022-04-01
  - 24小时制
- bgpcollectors
  - 需要所有的采集器："all"
  - 需要ripe的所有采集器：“ripe"
  - 需要routeviews的所有采集器：“routeviews”
  - 指定采集器：["rrc00","rrc01"]
- data_type
  - 所有数据类型：BGP_DATATYPE["ALL"]
  - 只需要rib：BGP_DATATYPE["RIBS"]
  - 只需要updates：BGP_DATATYPE["UPDATES"]
- destination
  - 目标文件夹
- save_by_collector
  - 设置后，下载的文件会以采集器名为子目录在目标文件夹下分开存储



### demo

需要采集所有ripe采集点从2022-04-14-00:00到2022-04-14-23:59的updates包,存到文件夹0414下

```python
if __name__=="__main__":
    bgpdbp=downloadByParams(
        worker_num=30,
        urlgetter=bgpGetter(base_params(
            start_time="2022-04-14-00:00",
            end_time="2022-04-14-23:59",
            bgpcollectors="ripe",
            data_type=BGP_DATATYPE["UPDATES"]
        )),
        destination="./0414",
        save_by_collector=0
    )
    bgpdbp.start_on()
```

需要采集rrc01,rrc02,rrc14从2022-04-14-00:00到2022-04-14-23:59的所有类型的包

```python
if __name__=="__main__":
    bgpdbp=downloadByParams(
        worker_num=30,
        urlgetter=bgpGetter(base_params(
            start_time="2022-04-14-00:00",
            end_time="2022-04-14-23:59",
            bgpcollectors=["rrc01","rrc02","rrc14"],
            data_type=BGP_DATATYPE["ALL"]
        )),
        destination="./0414",
        save_by_collector=0
    )
    bgpdbp.start_on()
```



### 附

由于有时会出现HTTP连接超时的情况，我设置了每次HTTP连接的超时时长，因此有可能出现某些包连接超时的情况，这些包的信息被记录在文件夹下errorInfo.txt中。如果需要重新下载这些包，可以调用tool.py下的check_error_info函数，参数为文件名。如果errorInfo.txt为空则没有必要调用该函数。

```python
check_error_info("./0414/errorInfo.txt")
```



## 解析模块

调用parse.py中的parseAll函数，参数为文件夹名，可以解析整个文件夹内的数据

例如：

```python
st=time.time()
parseall("rrc00_0301")
print("-----------")
print(time.time()-st)
```

该程序会把rrc00_0301文件夹下的数据全部解析，并放到rrc00_0301_Parsed文件夹下

此外，解析格式可以通过config/parseMRT.ini来设置，目前支持以下几种格式

- verbose：解析出全字段，包括所有路径属性，格式为TYPE|timestamp|flag|peer_ip|peer_as|prefix|aspath|origin|next_hop|local_pref|med|community|atomic_aggr|merge_aggr
- ts_format
  - 0-时间戳
  - 1-标准格式时间
- AS_PATH_ONLY：只解析出aspath
- PREFIX_AND_ORIGIN：只解析prefix字段和origin字段
- 若全设置为0则为默认模式，字段为：TYPE|d|flag|peer_ip|peer_as|prefix|aspath