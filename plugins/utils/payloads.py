from datetime import datetime
import uuid
import copy

from configs.links import COOKIES_URL_SEJU

PTNGR = ["003", "034", "014"]

class Payloads:
    def __init__(self, session):
        self.session = session
        self._base_header = {
            "browserEngine": "Chrome",
            "browserVersion": "127.0.0.0",
            "curConnUrl": None,
            "eventData": None,
            "eventNm": "SRT",
            "lang": "ko-KR",
            "lat": None,
            "latencyPage": None,
            "lon": None,
            "osName": "Windows",
            "osVersion": "10",
            "pathCd": "CBP",
            "prevPage": COOKIES_URL_SEJU,
            "siteId": "hanatour",
            "uid": None,
            "uidDomain": None,
            "uidx": None,
            "userAgent": self.session.get_header(),
            "userDevice": "PC",
            "userIp": None,
        }

    def _get_updated_header(self):
        header = copy.deepcopy(self._base_header)
        header["timestamp"] = datetime.now().strftime("%Y%m%d%H%M%S")
        header["sessionId"] = str(uuid.uuid4())
        return header


    def build_major_product_payload(
            self,
            domain,
            cityCd,
            cityNm,
            strtDepDay,
            endDepDay,
            page=1,
            pageSize=200
        ):
        return {
            'afcnCobrandProdYn': "N",
            "afcnResTrgtDvCd" : "00",
            "appVersion": "",
            "cityCd": cityCd,
            "cityNm": cityNm,
            "depCityCd": "",
            "depCityNm": "전체",
            "domain": domain,
            "endDepDay": endDepDay.strftime("%Y%m%d"),
            "header": self._get_updated_header(),
            "isCobrand": "Y",
            "isCustomCobrand": "N",
            "os": "pc",
            "page": page,
            "pageSize": pageSize,
            "paymentTypeYn": "Y",
            "pkgServiceCd": "FP",
            "ptnCd": "A3595",
            "ptngr": PTNGR,
            "resPathCd": "CBP",
            "sort": "RPRS_SORT1",
            "strtDepDay": strtDepDay.strftime("%Y%m%d"),
            "trvlDayCnt": ""
        }
    

    def build_product_payload(
            self,
            domain,
            rprsProdCds,
            cityCd,
            cityNm,
            strtDepDay,
            endDepDay,
            page=1,
            pageSize=200
        ):
        return {
            'afcnCobrandProdYn': "N",
            "afcnResTrgtDvCd" : "00",
            "appVersion": "",
            "cityCd": cityCd,
            "cityNm": cityNm,
            "depCityCd": "",
            "depCityNm": "전체",
            "domain": domain,
            "endDepDay": endDepDay.strftime("%Y%m%d"),
            "header": self._get_updated_header(),
            "isCobrand": "Y",
            "isCustomCobrand": "N",
            "os": "pc",
            "page": page,
            "pageSize": pageSize,         
            "paymentTypeYn": "Y",
            "pkgServiceCd": "FP",
            "prodTypeCd": "G,I",
            "ptnCd": "A3595",
            "ptngr": PTNGR,
            "resPathCd": "CBP",
            "rprsProdCds": rprsProdCds, 
            "sort": "PROD_SORT5",
            "strtDepDay": strtDepDay.strftime("%Y%m%d"),
            "trvlDayCnt": ""
        }
    

    def build_describe_payload(
        self,
        pkgCd
    ):
        return {
            'coopYn' : "N",
            'inpPathCd' : "CBP",
            'partnerYn' : "N",
            'pkgCd' : pkgCd,
            'ptnCd' : "A3595",
            'resAcceptPtn' : {},
            'smplYn' : "N"  
        }