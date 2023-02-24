from base import base_params, bgpGetter
from tools import check_error_list
from params import BGP_DATATYPE
from dataGetter import downloadByParams

if __name__=="__main__":
    bgpdbp=downloadByParams( 
        urlgetter=bgpGetter(base_params(
            start_time="2022-05-05-00:00",
            end_time="2022-05-05-00:10",
            bgpcollectors=["rrc00"],
            data_type=BGP_DATATYPE['UPDATES']
        )),
        destination="/data/bgpdata",
        save_by_collector=1
    )

    bgpdbp.start_on()
    check_error_list("/data/bgpdata/errorInfo.txt")
