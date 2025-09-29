import os
import logging
import sqlite3
import uuid
from urllib.parse import urljoin 

from bs4 import BeautifulSoup
from dotenv import load_dotenv

class Parser:
    def __init__(self):
        load_dotenv()
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        self.logger = logging.getLogger("parser")
        self.conn = sqlite3.connect(os.getenv("CRAWLERS_DB_URL"))
        
    def extract_links(self, html:str, base_url:str, parse_meta: dict = {"skip": False}) -> list[str]:
        soup = BeautifulSoup(html, "lxml")
        
        if "skip" in parse_meta and parse_meta["skip"]: # 링크 추출 하지 않음.
            return []
        else:
            if "selector" in parse_meta and parse_meta["selector"]: # Selector filtering
                if "class" in parse_meta["selector"] and parse_meta["selector"]["class"]:
                    elems = soup.find_all(class_ = parse_meta["selector"]["class"])
                    
                    atags = []
                    for elem in elems:
                        atags.extend(elem.find_all("a"))
                    
                    hrefs = set([a.attrs["href"] for a in atags if a and "href" in a.attrs])
            else:
                # 없으면 전부 수집
                atags = soup.find_all("a")
                hrefs = set([a.attrs["href"] for a in atags if "href" in a.attrs])
                    
        # Url 수집
        urls = [] 
        for href in hrefs:
            urls.append(urljoin(base_url, href).lower())
                
        return urls
        
    def load_urls_to_db(self, base_url_id: str, urls: list[str]):
        url_info = [(str(uuid.uuid1()), url, ) for url in urls]
        
        cur = self.conn.cursor()
        
        # url 중복 제거 로직
        # 중복 제거 로직이 단순히 url만으로는 안될 것 같다. canonical urls가 있어서 이 urls을 처리하는 방법이 필요함.
        # e.g. https://example.com == https://www.example.com == https://WWW.EXAMPLE.COM
        cur.executemany(
            """
                INSERT INTO url_info (id, url) VALUES (?, ?) ON CONFLICT (url) DO NOTHING;
            """,
            url_info
        )
        
        # parsing을 마친 url을 parsed로 변경
        cur.execute(
            """
                UPDATE url_info SET status="parsed" WHERE id=?
            """, 
            (base_url_id, )
        )     
        cur.close()
        self.conn.commit()
        
        self.logger.info(f"Insert the {len(url_info)} of urls into db")
            
    
if __name__ == "__main__":
    load_dotenv()
    
    import time
    import json
    
    import redis
    r = redis.Redis(host = os.getenv("REDIS_HOST"), port = os.getenv("REDIS_PORT"), db = 0)
    
    with open(os.getenv("PARSE_META_FILE"), "r") as f:
        parse_meta_file = json.loads(f.read())
    parser = Parser()
    
    try_count = 0
    while True:
        item = r.lpop(os.getenv("REDIS_FETCHED_QUEUE"))
        
        if item:
            try_count = 0
            example_url_save_path = item.decode().split("|")
        else:
            print("No item in fetched queue")
            time.sleep(int(os.getenv("INTERVAL", 5)))
            
            try_count += 1
            if try_count > 10: 
                print("Exceed the try counts")
                exit(1)
            continue
        
        url_id = example_url_save_path[0]
        url = example_url_save_path[1]
        html_path = example_url_save_path[2]
        
        # --------------------------------------
        import re
        for key in parse_meta_file.keys():
            block = re.match(key, url)
            if block:
                parse_meta = parse_meta_file[key]
                print(parse_meta)
                break # 찾으면 바로 break
        # --------------------------------------
            
        with open(html_path, "r") as f:
            html = f.read()
        try:
            urls = parser.extract_links(html, url, parse_meta)
            parser.load_urls_to_db(url_id, urls)
            print(f"Push extracted-url {url}")
        except KeyboardInterrupt:
            exit(1)
        except:
            import traceback
            traceback.print_exc()
            r.rpush(os.getenv("REDIS_FETCHED_QUEUE"), item)
            print(f"Re-push failed item {item}")
        
        