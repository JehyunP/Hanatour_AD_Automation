import pandas as pd
import logging
from utils.preprocessor import sort_title_universal

class LogicProducer:

    def __init__(self, prefix, logic, base_url, id, preferred):        
        self.prefix = prefix
        self.logic = logic
        self.base_url = base_url
        self.id = id
    

    def apply_logic(self, s3, logic_type):
        dfs = s3.read_recent_folder(self.prefix)
        df_list = list(dfs.values())

        logic_priority_files  = s3.read_recent_folder('logic_priority')

        if not logic_priority_files :
            logging.error("[logic_priority] No logic priority files found in S3 prefix 'logic_priority'")
            return pd.DataFrame()

        logic_priority_df = next(iter(logic_priority_files.values()))

        if logic_type == 'seju':
            logic_col = 'sejuLogic'
        elif logic_type == 'dream':
            logic_col = 'dreamLogic'
        else:
            raise ValueError(f"Unknown logic_type: {logic_type}")

        priority_map = {}

        for _, row in logic_priority_df.iterrows():
            base_key = row['baseKey']
            logic_line = row[logic_col]

            if pd.isna(base_key):
                continue

            logic_list = [int(x) for x in logic_line.split(',') if x.strip()]

            priority_map[base_key] = logic_list

        merged_dfs = []
        
        for idx, df in enumerate(df_list):
            try:
                applied_logic_df = self.logic.create_dataFrame(df, priority_map, idx, self.base_url, self.id)
                merged_dfs.append(applied_logic_df)
            except IndexError:
                logging.info(f"[{self.prefix}] There are no more logic to be applied (logic_idx : {idx})")
                break
            except Exception:
                logging.exception(f"[{self.prefix}] Unexpected error catched (logic_idx: {idx})")
                continue

        if not merged_dfs:
            logging.error(f"[{self.prefix}] No DataFrames were produced; merged_dfs is empty.")
            return pd.DataFrame()

        return pd.concat(merged_dfs, axis=0, ignore_index=True)


    
    def apply_drop_duplicates(self, df):
        # 제목 80자 이상은 제거
        df = df[df['title'].str.len() <= 80]

        # 중복 랜딩페이지 제거
        df = df.drop_duplicates(subset=['link'], keep='first')

        # id 기준 정렬
        df_sorted = df.sort_values(by=['id'], ascending=[True])

        # 제목 중복 제거
        df_sorted['sorted_title'] = df_sorted['title'].apply(sort_title_universal)
        df_sorted = df_sorted.drop_duplicates(subset=['sorted_title'], keep='first')
        df_sorted = df_sorted.drop(columns=['sorted_title'])

        return df_sorted

