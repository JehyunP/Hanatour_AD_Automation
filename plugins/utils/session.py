import requests
import logging

BASE_USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    )


class Session():

    def __init__(self, cookie):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": BASE_USER_AGENT
        })

        try:
            resp = self.session.get(cookie, timeout=10)
            resp.raise_for_status()
            logging.info(f'[CREATED] Session Created with {cookie}')
        except Exception as e:
            logging.warning(f'[WARNING] Initial session request failed: {e}')


    def get_response(self, url):
        response = None
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            logging.info(f'[SUCCESS] URL : {url}')

            return response.json()
        
        except Exception as e:
            logging.error("[ERROR] Request failed")
            if response is not None:
                logging.error(f"Status Code: {response.status_code}")
                logging.error(f"Content-Type: {response.headers.get('Content-Type')}")
            logging.error(f"Exception: {e}")
            return None
        
    def get_header(self):
        return BASE_USER_AGENT
    
    def post(self, url, **kwargs):
        return self.session.post(url, **kwargs)

    def close(self):
        self.session.close()
        logging.info("[CLOSED] Session closed")