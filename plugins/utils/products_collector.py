# 시나리오
# s3 나라 리스트 읽고 유의미한 지역만 리스트화
# 병렬적으로 리스트에 있는 나라에 대해 API 요청을 하고 이를 N개에 대해서 맥스 힙큐를 사용하며 비교 저장
# 저장된 힙큐는 딕셔너리에 상품 공용키를 통해 저장
# 저장된 딕셔너리에 대해서 N개의 힙큐 데이터를 받아 각각의 상세 페이지 api를 요청하고 N크기의 리스트에 데이터를 기존의 것과 합쳐 

from utils.s3_util import S3Util
from utils.session import Session
from utils.payloads import Payloads

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from collections import defaultdict
from itertools import count

import heapq
import logging

class ProductsCollector:
    def __init__(
        self,
        cookie : str,
        data_url : list,
        main_url : str,
        landing_url : str
    ):
        self.cookie = cookie
        self.data_url = data_url
        self.main_url = main_url
        self.landing_url = landing_url
        self.data_container = defaultdict(list)
        self.counter = count()
        self.seen_products = defaultdict(set)



    @staticmethod
    def _score(
        adt_amt : str,
        dep_day : str
    ):
        price = int(adt_amt)
        dep = int(dep_day)
        return (-price, -dep)
    


    def get_listing_areas(self, bucket, filename):
        s3 = S3Util(bucket)
        data = s3.read_json(filename)

        list_areas = []

        for item in data["data"]:
            for area in item["areas"]:
                if area["areaType"] == "C":
                    list_areas.append(
                        {
                            "main": item["name"],
                            "sub": "",
                            "city": area["name"],
                            "code": area["code"],
                            "type": area["areaType"],
                        }
                    )

                if area["areas"]:
                    for specific in area["areas"]:
                        if specific["areaType"] == "C":
                            list_areas.append(
                                {
                                    "main": item["name"],
                                    "sub": area["name"],
                                    "city": specific["name"],
                                    "code": specific["code"],
                                    "type": specific["areaType"],
                                }
                            )

        return list_areas
    



    def get_products_data1(
            self, 
            area : dict, 
            N : int, 
            start_dep_day : datetime, 
            end_dep_day : datetime
        ):

        # 쓰레드로 진행되기 때문에 각각 새로운 세션 생성
        session = Session(self.cookie)
        payload = Payloads(session)

        # api 요청
        try:
            # major products 
            major_payload = payload.build_major_product_payload(
                self.main_url,
                area["code"],
                area["city"],
                start_dep_day,
                end_dep_day,
            )

            _products = session.post(
                self.data_url[0], json=major_payload, timeout=10
            )
            _products.raise_for_status()
            logging.info(f"[SUCCESS] POST major : {area['city']}")

            # Extract
            for product in _products.json()['data']['products']:
                # 에어텔, 자유여행, 허니문이면 스킵
                if any(
                    sticker.get("value") in ("에어텔", "자유여행", "허니문")
                    for sticker in product.get("sticker", [])
                ):
                    continue

                rprs_prod_cd = product["rprsProdCd"]
                citycd = area["code"]
                cityNm = area["city"]

                # 상품의 모든 날짜에 대해 1차 상세 데이터 요청 및 적재
                for page_num in range(1, 31): # 30 페이지까지
                    product_payload = payload.build_product_payload(
                        self.main_url,
                        rprs_prod_cd,
                        citycd,
                        cityNm,
                        start_dep_day,
                        end_dep_day,
                        page=page_num,
                    )

                    try:
                        _product_data = session.post(
                            self.data_url[1], json=product_payload, timeout=10
                        )
                        _product_data.raise_for_status()

                        product_data = _product_data.json()["data"]["products"]

                        # product 데이터가 비워져있다면 탐색 종료
                        if not product_data:
                            break

                        for product2 in product_data:
                            # 잔여석이 2자리 미만이라면 스킵
                            if product2["remaSeatCnt"] <= 1:
                                continue

                            # 2일 이하 일정 제외
                            if int(product2["trvlDayCnt"]) <= 2:
                                continue

                            # 데이터 정의
                            sale_prod_cd = product2["saleProdCd"]
                            sale_prod_nm = product2["saleProdNm"]
                            dep_air_nm = product2["depAirNm"]
                            main = area["main"]
                            sub = area["sub"]
                            city = area["city"]
                            trvl_day = f"{product2['trvlNgtCnt']}박{product2['trvlDayCnt']}일"
                            hashtag = " ".join(product2.get("hashtag", []))
                            adt_amt = product2["adtAmt"]
                            dep_day = product2["depDay"]
                            nrml_amt = product2["nrmlAmt"] if product2["nrmlAmt"] else ""
                            discount_text = (
                                product2["discountText"]
                                if product2["discountText"]
                                else ""
                            )
                            label = ",".join(
                                f"{z['code']}_{z['value']}"
                                for z in product2.get("label", [])
                            )
                            city_info = ",".join(
                                f"{z['countryName']}_{z['cityName']}"
                                for z in product2['logger']['click'].get('cityInfo', [])
                            )

                            # 날짜 제거한 base 상품코드 (고유 상품키)
                            base_key = sale_prod_cd[:6] + sale_prod_cd[12:]

                            # 데이터화 -> dictionary
                            dic = {
                                "baseKey": base_key,
                                "saleProdCd": sale_prod_cd,
                                "saleProdNm": sale_prod_nm,
                                "depAirNm": dep_air_nm,
                                "main": main,
                                "sub": sub,
                                "city": city,
                                "trvlDay": trvl_day,
                                "hashtag": hashtag,
                                "nrmlAmt": nrml_amt,
                                "adtAmt": adt_amt,
                                "discountText": discount_text,
                                "label": label,
                                "depDay": dep_day,
                                "cityInfo":city_info
                            }

                            # 점수 확인
                            score = self._score(adt_amt, dep_day)

                            # max-heapq (가격과 날짜는 거꾸로 들어가 0번이 가장 큰값이 들어갈 예정)
                            max_heapq = self.data_container[base_key]
                            seen_set = self.seen_products[base_key]

                            if sale_prod_cd in seen_set:
                                continue
                            
                            # 들어갈 값을 확인하고 N개에 어울리는 데이터인지 검증
                            entry = (score, next(self.counter), sale_prod_cd, dic)

                            if len(max_heapq) < N:
                                heapq.heappush(max_heapq, entry)
                                seen_set.add(sale_prod_cd)

                            else:
                                if score > max_heapq[0][0]:
                                    removed_entry = heapq.heapreplace(max_heapq, entry)
                                    removed_code = removed_entry[2]
                                    seen_set.discard(removed_code)
                                    seen_set.add(sale_prod_cd)
                    
                    except Exception as e:
                        logging.error(f'[ERROR] POST Failed : {rprs_prod_cd}\n\tError Message : {e}')

        except Exception as e:
            logging.error(f"[ERROR] POST FAILED (major) : {area['city']}\n\tError Message : {e}")

        finally:
            session.close()


    def get_products_data2(
        self,
        session : Session,
        record : dict,
        num : int
    ):
        try:
            sale_prod_cd = record["saleProdCd"]
            payload = Payloads(session)
            describe_payload = payload.build_describe_payload(sale_prod_cd)

            resp = session.post(self.data_url[2], json=describe_payload, timeout=10)
            resp.raise_for_status()
            data = resp.json()["data"]

            brndNm = data["brndNm"]
            if brndNm == "현지투어플러스":
                return None
            
            prodSbttNm = data["prodSbttNm"]
            promNms = data.get("promNms", "")
            depCityNm = data["depCityNm"]
            cityBasInfoList = " ".join(x["cityNm"] for x in data["cityBasInfoList"])
            rppdCntntInfoList = " ".join(
                y["rprsProdCntntUrlAdrs"] for y in data["rppdCntntInfoList"]
            )
            landing_url = (
                self.landing_url + sale_prod_cd + "&prePage=major-products"
            )

            extra = {
                "prodSbttNm": prodSbttNm,
                "promNms": promNms,
                "brndNm": brndNm,
                "depCityNm": depCityNm,
                "cityBasInfoList": cityBasInfoList,
                "rppdCntntInfoList": rppdCntntInfoList,
                "landing_url": landing_url,
            }

            return {**record, **extra}

        except Exception as e:
            logging.error(f"[ERROR] describe failed: {record.get('saleProdCd')}[{num}]\n\tError: {e}")
            return None
        


    def upload_to_s3(
        self,
        num : int,
        data : list,
        base_prefix :str,
        s3 : S3Util
    ):
        key = f'{base_prefix}dataset{num+1}.parquet'
        s3.upload_parquet(data, key)
        logging.info((f"[SUCCESS] Uploaded dataset{num+1} into S3"))


    def run_pipeline(
        self,
        bronze_bucket : str,
        silver_bucket : str,
        filename1 : str,
        filename2 : str,
        date : datetime,
        date_range : int,
        N : int,
        number_worker : int = 4
    ):
        # area list 생성 (s3 데이터 사용)
        area_list = self.get_listing_areas(bronze_bucket, filename1)

        # 날짜 범위 설정 실행일 + 3주(21일) 부터 ~ range까지
        start_dep_day = date + timedelta(days=21)
        end_dep_day = start_dep_day + timedelta(days=date_range)

        # 모든 area 에 대해서 멀티 쓰레드
        with ThreadPoolExecutor(max_workers=number_worker) as executor:
            for area in area_list:
                executor.submit(
                    self.get_products_data1,
                    area,
                    N,
                    start_dep_day,
                    end_dep_day
                )

        # 중복체크 초기화 (메모리 해제) 
        self.seen_products.clear()

        # 모든 상품에 대해서 디테일 페이지 정보를 얻고 각 정보를 리스트 안에 저장한 뒤 데이터프레임 변환
        list_set = defaultdict(list) # 파티션을 N개 만큼 나누어 저장용

        shared_session = Session(self.cookie) 
        with ThreadPoolExecutor(max_workers=N) as executor:
            future_to_num = {}
            for code, heap_list in self.data_container.items():
                for num, (_, _, _, record) in enumerate(heap_list):
                    f = executor.submit(self.get_products_data2, shared_session, record, num)
                    future_to_num[f] = num

            for future in as_completed(future_to_num):
                n = future_to_num[future]
                res = future.result()
                if res: # None이 아닐 때만 저장
                    list_set[n].append(res)
        shared_session.close()

        # 각 데이터를 S3에 저장
        yymmdd = date.strftime("%Y%m%d")
        hhmm = date.strftime("%H%M")
        base_prefix = f'{filename2}/{yymmdd}/{hhmm}/'
        s3 = S3Util(silver_bucket)

        with ThreadPoolExecutor(max_workers=N) as upload_executor:
            for num, data in list_set.items():
                if not data: continue # 데이터가 비어있을 경우 스킵
                
                upload_executor.submit(
                    self.upload_to_s3,
                    num,
                    data,
                    base_prefix,
                    s3
                )
