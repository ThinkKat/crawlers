import os
import sqlite3
import logging
from pathlib import Path

from dotenv import load_dotenv
from datetime import datetime

from playwright.sync_api import sync_playwright
import requests
import redis

class Fetcher:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        self.logger = logging.getLogger("fetcher")
        
        load_dotenv()
        self.conn = sqlite3.connect(os.getenv("CRAWLERS_DB_URL"))
        self.r = redis.Redis(host = os.getenv("REDIS_HOST"), port = os.getenv("REDIS_PORT"), db = 0)
        
        self.p = None
        self.browser = None
    
    def fetch(self, 
              url_id: str, 
              url: str, 
              load_meta: dict | None = {"mode": "static"}, 
              save_meta: dict | None = {"save_path": "assets"}):
        
        """ Fetch url and save html
        """
        requested_at = datetime.now()
        cur = self.conn.cursor()
        
        if load_meta["mode"] == "static":
            try:
                response = requests.get(url, timeout=5)
            except Exception:
                self.logger.error(f"Failed to request urls {url}")
                # status 업데이트
                # TODO: 바로 failed로 바꾸는게 아니라 retry 이후 count
                cur.execute(
                    """
                    UPDATE url_info SET status="failed" WHERE id=?
                    """, 
                    (url_id, )
                )
                # Close cursor
                cur.close()
                # Commit
                self.conn.commit()
                
                # 종료
                return
            
            # crawl history
            responsed_at = datetime.now()
            responsed_date = responsed_at.strftime("%Y-%m-%d")
            latency = response.elapsed.total_seconds()
            http_response_status = response.status_code
            crawl_status = response.ok * 1
            next_crawled_at = None
            
            html = response.text
            
        elif load_meta["mode"] == "browser":
            if not self.p and not self.browser:
                self.p = sync_playwright().start()
                self.browser = self.p.chromium.launch(headless=True)
            page = self.browser.new_page()
            response = page.goto(url)
            
            # crawl history
            responsed_at = datetime.now()
            responsed_date = responsed_at.strftime("%Y-%m-%d")
            timing = page.evaluate("() => performance.timing")
            latency = (timing["responseEnd"] - timing["requestStart"])
            http_response_status = response.status
            
            # TODO: requests와 crawl_status를 맞춰야 함. crawl_status가 200대만 true.
            crawl_status = response.ok * 1
            next_crawled_at = None
            
            html = page.content()
            
            page.close()
            # browser.close()
        else:
            raise Exception("Not impletmented another mode : browser")
        
        # Save html
        save_path = f"{save_meta['save_path']}/{responsed_date}"
        file_name = f"{url_id}-{responsed_at.timestamp()}.html"
        if not Path(f"{save_path}").exists():
            Path(f"{save_path}").mkdir(parents=True, exist_ok=True)
        Path(f"{save_path}/{file_name}").write_text(html)
        self.logger.info(f"Success file: {save_path}/{file_name}")
        
        # Save crawl history
        cur.execute(
            """
            INSERT INTO url_fetch_history 
            (url_id, requested_at, responsed_at, http_response_status, latency, next_crawled_at, crawl_status, html_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
            """,
            (url_id, 
             requested_at.strftime("%Y-%m-%d %H:%M:%S"), 
             responsed_at.strftime("%Y-%m-%d %H:%M:%S"), 
             http_response_status, 
             latency, 
             next_crawled_at, 
             crawl_status, 
             f"{save_path}/{file_name}")
        )
        
        # status 업데이트
        cur.execute(
            """
            UPDATE url_info SET status="fetched" WHERE id=?
            """, 
            (url_id, )
        )
        
        # HTML path push
        self.r.rpush(os.getenv("REDIS_FETCHED_QUEUE"), f"{url_id}|{url}|{save_path}/{file_name}")
        
        # Close cursor
        cur.close()
        
        # Commit
        self.conn.commit()
    
if __name__ == "__main__":
    load_dotenv()
    
    import time
    import json
    with open(os.getenv("LOAD_META_FILE"), "r") as f:
        load_meta_file = json.loads(f.read())
        
    with open(os.getenv("SAVE_META_FILE"), "r") as f:
        save_meta_file = json.loads(f.read())
        
    r = redis.Redis(host = os.getenv("REDIS_HOST"), port = os.getenv("REDIS_PORT"), db = 0)
    # Fetcher
    fetcher = Fetcher()
    
    try_count = 0
    
    while True:
        item = r.lpop(os.getenv("REDIS_FRONTIER_QUEUE"))
        
        if item:
            try_count = 0
            example_url_info = item.decode().split("|")
        else:
            print("No item in frontier queue")
            time.sleep(int(os.getenv("INTERVAL", 5)))
            
            try_count += 1
            if try_count > 10: 
                print("Exceed the try counts")
                exit(1)
            continue
        
        # URl information     
        url_id = example_url_info[0]
        url = example_url_info[1]
        
        # --------------------------------------
        import re
        for key in load_meta_file.keys():
            block = re.match(key, url)
            if block:
                load_meta = load_meta_file[key]
                print(load_meta)
                break # 찾으면 바로 break
            
        for key in save_meta_file.keys():
            block = re.match(key, url)
            if block:
                save_meta = save_meta_file[key]
                print(save_meta)
                break # 찾으면 바로 break
        # --------------------------------------
        
        try:
            fetcher.fetch(url_id, url, load_meta=load_meta, save_meta=save_meta)
        except KeyboardInterrupt:
            exit(1)
        except:
            import traceback
            traceback.print_exc()
        