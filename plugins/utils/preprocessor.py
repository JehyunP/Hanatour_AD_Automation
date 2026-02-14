import re
import pandas as pd

from collections import defaultdict
from configs.seju_logic import *
from configs.mapping import *

def remove_bracket_words(text):
    if pd.isna(text):
        return text
    text = re.sub(r'\[[^\]]*\]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def replace_parentheses(text):
    if pd.isna(text):
        return text
    text = re.sub(r'([^(]+)\(([^)]+)\)', r'\1 \2', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def keep_words(text):
    if pd.isna(text):
        return text
    text = re.sub(r'[^가-힣A-Za-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def separate_nations(text):
    dic = defaultdict(list)
    if not text or pd.isna(text): return dic

    for word in text.split(','):
        word = word.strip()
        if '_' not in word:
            continue
        nation, city = word.split('_', 1)
        if nation == '대한민국' or city == '대한민국':
            continue
        if city not in dic[nation]:
            dic[nation].append(city)
    return dic


def preprocess_nations(text):
    if pd.isna(text) or not text.strip(): 
        return '', '', ''
    
    dic = separate_nations(text)
    if not dic:
        return '', '', ''
    
    mono = len(dic) == 1

    categories = []
    nations = []
    cities = []

    seen = set()

    for nation in dic.keys():
        if nation in MAP_NATIONS:
            list_nation = MAP_NATIONS[nation].split()

            if len(list_nation) == 1:
                word = list_nation[0]
                if word not in seen:
                    nations.append(word)
                    seen.add(word)
            else:
                cat = list_nation[0]
                if cat not in seen:
                    categories.append(cat)
                    seen.add(cat)
                
                for extra in list_nation[1:]:
                    if extra not in seen:
                        nations.append(extra)
                        seen.add(extra)
        else:
            if nation not in seen:
                nations.append(nation)
                seen.add(nation)

    if mono:
        for city_list in dic.values():
            for city in city_list:
                city = city.strip()
                mapped_city = MAP_CITY.get(city)

                if mapped_city and mapped_city not in seen:
                    cities.append(mapped_city)
                    seen.add(mapped_city)

                if city in MAP_USING_CITY and city not in seen:
                    cities.append(city)
                    seen.add(city)
    return (
        ' '.join(categories),
        ' '.join(nations),
        ' '.join(cities)
    )



def preprocess_title(text, n):
    """
    1. 한자 / 영어 -> 한국어 변환
    2. [단어] 삭제
    3. 특수문자 ' ' 변환
    4. 뛰어쓰기 기준 단어 자르고 리스트화 
    5. 단어를 n개 기준해서 가져오는거 (현재 문장 길이 + 1 + 새로운 단어 길이 < n) 
        -> 현재 문장에 단어가 이미 존재하면 x
    6. ' '.join
    """
    if pd.isna(text):
        return text
    
    sorted_keys = sorted(MAP_TRANSLATE.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if key.isascii():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            text = pattern.sub(MAP_TRANSLATE[key], text)
        else:
            text = text.replace(key, MAP_TRANSLATE[key])

    text = remove_bracket_words(text)
    text = replace_parentheses(text)
    text = keep_words(text)

    line = []
    current_len = 0
    for word in text.split():
        if any(bad in word for bad in NOT_ACCEPTED_WORDS):
            continue
        if word in line:
            continue
        add_len = len(word) if current_len == 0 else 1 + len(word)
        if current_len + add_len > n:
            break
        line.append(word)
        current_len += add_len
    return ' '.join(line)

    
def preprocess_package(brndNm):
    if brndNm == '크루즈':
        return '크루즈'
    elif brndNm == '골프':
        return '골프'
    elif brndNm == '레포츠':
        return '레포츠'
    elif brndNm == '트레킹':
        return '트레킹'
    elif brndNm == '프리미엄':
        return '프리미엄'
    else:
        return '패키지'


def preprocess_package_trv(brndNm):
    if brndNm == '크루즈':
        return '크루즈 여행'
    elif brndNm == '골프':
        return '골프 여행'
    elif brndNm == '레포츠':
        return '레포츠 여행'
    elif brndNm == '트레킹':
        return '트레킹 여행'
    elif brndNm == '프리미엄':
        return '프리미엄 여행'
    else:
        return '패키지 여행'


def preprocess_package_service(brndNm):
    if brndNm == '크루즈':
        return '크루즈 여행서비스'
    elif brndNm == '골프':
        return '골프 여행서비스'
    elif brndNm == '레포츠':
        return '레포츠 여행서비스'
    elif brndNm == '트레킹':
        return '트레킹 여행서비스'
    elif brndNm == '프리미엄':
        return '프리미엄 여행서비스'
    else:
        return '패키지 여행서비스'


def preprocess_promotions(promNms):
    if pd.isna(promNms) or not promNms:
        return ""
    sentences = []
    for word in promNms.split(','):
        word = word.strip()
        if '/' in word:
            for sub_word in word.split('/'):
                sub_word = sub_word.strip()
                if sub_word in MAP_PROMOTION:
                    sentences.append(MAP_PROMOTION[sub_word])
        elif word in MAP_PROMOTION:
            sentences.append(MAP_PROMOTION[word])
    return ' '.join(sentences)


def preprocess_month(depDay):
    if pd.isna(depDay):
        return depDay
    depDay = str(depDay)[4:6]
    month = ''
    if depDay in MAP_MONTH:
        month = MAP_MONTH[depDay]
    return month


def preprocess_depart_city(name):
    if pd.isna(name):
        return name
    return MAP_DEPART[name] if name in MAP_DEPART else ''


def preprocess_hashtag(text, start, end):
    if pd.isna(text):
        return ""
        
    raw_tags = [w.strip() for w in str(text).split('#') if w.strip()]

    cleaned = []
    seen = set()

    for tag in raw_tags:
        tag = keep_words(tag)
        if not tag:
            continue

        if any(bad in tag for bad in NOT_ACCEPTED_WORDS):
            continue

        if tag in seen:
            continue
    
        seen.add(tag)
        cleaned.append(tag)

    return ' '.join(cleaned[start:end])


def preprocess_airline(text):
    if text in ['대한항공', '아시아나항공']:
        return f'{text} 직항'
    return ''


def sort_title_universal(title, sep=''):
    text = title.lower()
    text = re.sub(r'(\d+)\s*(박|일|월|회|성급|km|성)', r' \1\2 ', text)
    
    unique_words = set(text)
    sorted_words = sorted(list(unique_words))
    
    return sep.join(sorted_words)