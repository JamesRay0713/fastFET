import sys, os
sys.path.append(os.path.dirname(sys.path[0]))
import datetime
import urllib,re
import urllib.request
import urllib.error
import warnings
from bs4 import BeautifulSoup
from fastFET.BGPMAGNET.tools import get_year_month, get_year_month_day
from fastFET.BGPMAGNET.params import HTTPS,PATTERN_STR, BGP_RIPE_URL, RouteViews,BGP_RIPE, RouteViews_URL, BGP_DATATYPE

class base_params:
    __slot__=["start_time","end_time","data_type","bgp_collectors"]
    def __init__(self,
        start_time=None,
        end_time=None,
        data_type=None,
        bgpcollectors=None,
    ):
        # 这里start_time和end_time需要设置为datetime类型
        if start_time==None:
            pass
        elif isinstance(start_time,str):
            self.start_time=datetime.datetime.strptime(start_time,"%Y-%m-%d-%H:%M")
        elif isinstance(start_time,datetime.datetime):
            self.start_time=start_time
        else:
            warnings.warn("Not Correct DataType:start_time",UserWarning)

        if end_time==None:
            pass
        elif isinstance(end_time,str):
            self.end_time=datetime.datetime.strptime(end_time,"%Y-%m-%d-%H:%M")
        elif isinstance(end_time,datetime.datetime):
            self.end_time=end_time
        else:
            warnings.warn("Not Correct DataType:end_time",UserWarning)
        
        if start_time!=None and end_time!=None:
            if start_time>end_time:
                warnings.warn("start_time should be previous before end_time",UserWarning)

        self.data_type=data_type
        
        # 检查bgpcollector
        if bgpcollectors==None:
            pass
        elif isinstance(bgpcollectors,str):
            if bgpcollectors.lower()=="all":
                self.bgp_collectors=list(RouteViews)+list(BGP_RIPE)
            elif bgpcollectors.lower()=="routeviews":
                self.bgp_collectors=list(RouteViews)
            elif bgpcollectors.lower()=="ripe":
                self.bgp_collectors=list(BGP_RIPE)
        elif isinstance(bgpcollectors,list):
            s=set()
            for c in bgpcollectors:
                if c.lower() in BGP_RIPE or c.lower() in RouteViews:
                    s.add(c)
            self.bgp_collectors=list(s)
        else:
            warnings.warn("Not Correct DataType:bgpcollectors",UserWarning)

class urlGetter:
    def __init__(self,params:base_params):
        super(urlGetter).__init__()
        self.params=params
    
    def findElement(self, url, pattern_str):
        #use beautifulsoup to get pattern_str-like element in html
        sources=[]
        bs4_parser = "html.parser"
        try:
            response = urllib.request.urlopen(url)
            html = BeautifulSoup(response.read(), bs4_parser)
            for link in html.findAll('a',text=re.compile(pattern_str)):
                sources.append(link['href'])
            response.close()
        except urllib.error.HTTPError:
            print(url + " dont have such data!") 
        return sources

    def getURL(self):
        pass

class bgpGetter(urlGetter):
    def set_base_url(self,collector):
        base_url=[]
        if collector in RouteViews:
            base_url.append(RouteViews_URL + collector + "/bgpdata")
        elif collector in BGP_RIPE:
            base_url.append(BGP_RIPE_URL + collector + "/")
        return base_url
    
    def set_base_url_by_type(self,collector,selected_time,datatype):
        base_url=[]
        if collector in RouteViews:
            if datatype==BGP_DATATYPE["UPDATES"] or datatype==BGP_DATATYPE["ALL"]:
                base_url.append(RouteViews_URL + collector+ "/bgpdata/" + selected_time + "/UPDATES/")
            if datatype==BGP_DATATYPE["RIBS"] or datatype==BGP_DATATYPE["ALL"]:
                base_url.append(RouteViews_URL + collector+ "/bgpdata/" + selected_time + "/RIBS/")
        elif collector in BGP_RIPE:
            base_url.append(BGP_RIPE_URL + collector + "/" + selected_time + "/")
        return base_url
    
    def set_pattern_str(self,cc,dt):
        pattern_str=""
        if dt==BGP_DATATYPE["UPDATES"]:
            pattern_str=PATTERN_STR['UPDATES']
        elif dt==BGP_DATATYPE["RIBS"]:
            pattern_str=PATTERN_STR['BZ2'] if cc in RouteViews else PATTERN_STR['BVIEW']
        elif dt==BGP_DATATYPE["ALL"]:
            pattern_str=PATTERN_STR['UPDATES']+"|"+(PATTERN_STR['BZ2'] if cc in RouteViews else PATTERN_STR['BVIEW'])
        return pattern_str   

    def getURL(self):
        RibUrls=[] 
        start_time=self.params.start_time
        end_time=self.params.end_time
        #set type
        datatype=self.params.data_type
        #set collectors
        for cc in self.params.bgp_collectors:
            print(cc)
            sources=[]
            base_url=self.set_base_url(cc)
            for url in base_url:
                sources+=self.findElement(url, PATTERN_STR["YEAR_MONTH"])
            selected_times=[]    
            for s in sources:
                t=s.split("/")[0]
                ym=get_year_month(datetime.datetime.strptime(t,"%Y.%m"))
                if get_year_month(start_time) <= ym and get_year_month(end_time) >= ym:
                    selected_times.append(t)
            if len(selected_times)==0:
                print(cc+" dont have such data in your start_time and end_time")
                continue
            for st in selected_times:
                sources=[]
                base_url=self.set_base_url_by_type(cc,st,datatype)
                pattern_str=self.set_pattern_str(cc,datatype)
                for url in base_url:
                    sources = self.findElement(url, pattern_str)
                    for s in sources:
                        if len(s)<20:
                            continue
                        data=s.split(".")
                        tt=datetime.datetime.strptime(data[1]+'.'+data[2],"%Y%m%d.%H%M")
                        if start_time <= tt and end_time >= tt:
                            finalurl=url+s
                            RibUrls.append(finalurl)
        return RibUrls


if __name__ == '__main__':
    b=bgpGetter(base_params(
        start_time="2022-04-13-00:00",
        end_time="2022-04-13-23:59",
        bgpcollectors="all",
        data_type=BGP_DATATYPE["ALL"]
    ))
    print(b.getURL())