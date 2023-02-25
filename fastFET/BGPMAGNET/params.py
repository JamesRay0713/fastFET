from fastFET.BGPMAGNET.bgpparser.params import reverse_defaultdict

MAX_WAIT_TIME=200

BGP_RIPE={"rrc00","rrc01","rrc02","rrc03","rrc04","rrc05","rrc06","rrc07","rrc08","rrc09","rrc10","rrc11","rrc12","rrc13","rrc14","rrc15","rrc16","rrc18","rrc19","rrc20","rrc21","rrc22","rrc23","rrc24","rrc25","rrc26"}
BGP_RIPE_URL="http://data.ris.ripe.net/"

RouteViews={"route-views.amsix","route-views.chicago","route-views.chile","route-views.eqix","route-views.flix","route-views.fortaleza","route-views.gixa","route-views.gorex","route-views.isc","route-views.jinx","route-views.kixp","route-views.linx","route-views.napafrica","route-views.nwax","route-views.perth","route-views.phoix","route-views.rio","route-views.saopaulo","route-views.sfmix","route-views.sg","route-views.soxrs","route-views.sydney","route-views.telxatl","route-views.wide","route-views2.saopaulo","route-views3","route-views4","route-views5","route-views6","route-views.peru","route-views.siex","route-views.mwix","route-views.bdix","route-views.bknix","route-views.uaeix","route-views"}
RouteViews_URL="http://archive.routeviews.org/"


HTTPS=1
FTP=2

RIB_FILE_TIEMOUT=1000
UPDATES_FILE_TIMEOUT=200

BGP_DATATYPE=reverse_defaultdict({
    1:'RIBS',
    2:'UPDATES',
    3:'ALL'
})

PATTERN_STR=reverse_defaultdict({
    '^(((?:19|20)\d\d).(0?[1-9]|1[0-2]))':'YEAR_MONTH',
    '((?:19|20)\d\d)/':'YEAR',
    '(0?[1-9]|1[0-2])':'MONTH',
    '(0[1-9]|1\d|2\d)':'DAY',
    '^rpki':'NTT',
    '^updates':'UPDATES',
    '.bz2$':'BZ2',
    '^bview':'BVIEW',
    '.gz$':'GZ'
})

