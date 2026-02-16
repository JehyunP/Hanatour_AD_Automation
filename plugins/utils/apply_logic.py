import pandas as pd

from utils.preprocessor import sort_title_universal

class LogicProducer:

    def __init__(self, prefix, logic, base_url, id):        
        self.prefix = prefix
        self.logic = logic
        self.base_url = base_url
        self.id = id
    

    def apply_logic(self, s3):
        dfs = s3.read_recent_folder(self.prefix)

        merged_dfs = []

        for num, df in enumerate(dfs.values()):
            applied_logic_df = self.logic.create_dataFrame(df, num, self.base_url, self.id)
            merged_dfs.append(applied_logic_df)

        return pd.concat(merged_dfs, axis=0, ignore_index=True)
    

    def apply_mapping(self, df, mapping_df):
        copied = df.copy()
        map_df = mapping_df.drop_duplicates(subset=['code'], keep='last')

        # 임시
        map_df['new_code'] = 'seju-' + map_df['code'].astype(str) + '-0001'

        id_map = map_df.set_index('new_code')['id'].to_dict()
        title_map = map_df.set_index('new_code')['title'].to_dict()
        img_map = map_df.set_index('new_code')['imageURL'].to_dict()

        copied['isMapped'] = copied['id'].isin(map_df['new_code'])

        mask = copied['isMapped']
        base_id = copied.loc[mask, 'id']

        copied.loc[mask, 'id'] = base_id.map(id_map).values
        copied.loc[mask, 'title'] = base_id.map(title_map).values
        copied.loc[mask, 'image_link'] = base_id.map(img_map).values

        return copied

    
    def apply_drop_duplicates(self, df):
        # 제목 80자 이상은 제거
        df = df[df['title'].str.len() <= 80]

        # 중복 랜딩페이지 제거
        df = df.drop_duplicates(subset=['link'], keep='first')

        # isMapped = True / id 기준 정렬
        df_sorted = df.sort_values(by=['isMapped', 'id'], ascending=[False, True])

        # 제목 중복 제거
        df_sorted['sorted_title'] = df_sorted['title'].apply(sort_title_universal)
        df_sorted = df_sorted.drop_duplicates(subset=['sorted_title'], keep='first')
        df_sorted = df_sorted.drop(columns=['sorted_title'])

        # isMapped 제거
        df_sorted = df_sorted.drop(columns=['isMapped'])

        return df_sorted

