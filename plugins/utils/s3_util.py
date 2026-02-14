import os 
import boto3
import logging

import pandas as pd
import io
import json

class S3Util:
    def __init__(self, bucket):
        self.s3 = boto3.client(
            's3',
            region_name = 'ap-northeast-2'
        )
        self.bucket = bucket

    def upload_json(self, data, key):
        body = json.dumps(
            data
            , ensure_ascii=False
            , indent=2
        ).encode('utf-8')

        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json; charset=utf-8"
            )
            logging.info(f'[SUCCESS] Upload to S3: {self.bucket}/{key}')

        except Exception as e:
            logging.error(f'[ERROR] S3 Upload Failed: {str(e)}')
            raise


    def read_json(self, key):
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=key
            )

            body = response["Body"].read().decode("utf-8")
            data = json.loads(body)
            logging.info(f'[SUCCESS] Read {key}')
            return data
        
        except Exception as e:
            logging.error(f'[ERROR] Read Fail\n\t{e}')
            return None
        
    
    def upload_parquet(self, data, key):
        try:
            if isinstance(data, pd.DataFrame):
                df = data
            else:
                df = pd.DataFrame(data)

            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False, engine='pyarrow')
            buffer.seek(0)

            self.s3.upload_fileobj(buffer, self.bucket, key)
            logging.info(f'[SUCCESS] Uploaded Parquet to S3: {key}')

        except Exception as e:
            logging.error(f'[ERROR] S3 Upload Fail (Key: {key})\n\t{e}')
            raise e
        
    
    def read_recent_folder(self, base_prefix):
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=base_prefix)
        if 'Contents' in response:
            all_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]

            if all_keys:
                folder_paths = {os.path.dirname(k) for k in all_keys}
                latest_folder = sorted(list(folder_paths), reverse=True)[0]
                logging.info(f'[SUCCESS] Find latest folder : {latest_folder}')

                target_keys = [k for k in all_keys if k.startswith(latest_folder)]

                df_dic = {}
                for key in target_keys:
                    filename = os.path.basename(key).replace('.parquet', '')
                    logging.info(f'[LOADING] {filename} from {key}')

                    obj = self.s3.get_object(Bucket=self.bucket, Key=key)
                    df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

                    df_dic[filename] = df

                logging.info(f'[SUCCESS] Loaded all file from S3')
                logging.info(f'\tLoaded datasets: {list(df_dic.keys())}')
                return df_dic
            
            else:
                logging.error(f'[ERROR] No .parquet file in the recent folder')
                raise

        else:
            logging.error('[ERROR] BUCKET is Empty')
            raise


    def read_xlsx(self, key) ->pd.DataFrame:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            file_content = obj['Body'].read()
            df = pd.read_excel(io.BytesIO(file_content), engine="openpyxl")
            logging.info(f"[SUCCESS] Read xlsx from S3: {self.bucket}/{key}")
            return df
        except self.s3.exceptions.NoSuchKey:
            logging.error(f"[ERROR] No such key: {self.bucket}/{key}")
            raise
        except Exception as e:
            logging.error(f"[ERROR] Read xlsx failed: {self.bucket}/{key}\n\t{e}")
            raise

        