import os
import sqlite3
import logging

from dotenv import load_dotenv
import redis

class Frontier:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        self.logger = logging.getLogger("frontier")
        
        load_dotenv()
        self.conn = sqlite3.connect(os.getenv("CRAWLERS_DB_URL"))
        self.r = redis.Redis(host = os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"))
    
    def load_urls_to_redis(self, maximum : int = 100):
        """ Load urls
        """
        
        # DB에서 pending 상태의 url을 가져옴. 
        cur = self.conn.cursor()
        res = cur.execute("SELECT id, url, status FROM url_info WHERE status = ? LIMIT ?", ("pending", maximum))
        
        urls = res.fetchall()
        urls_id = []
        # Redis 적재
        for url in urls:
            self.r.rpush(os.getenv("REDIS_FRONTIER_QUEUE"), "|".join(url))
            urls_id.append(url[0])
        
        res = cur.execute("UPDATE url_info SET status = 'queued' WHERE id IN (%s)" %
                           ','.join('?'*len(urls_id)), urls_id)
        
        cur.close()
        self.conn.commit()
        self.logger.info(f"Push the {len(urls)} of urls to redis queue")
        
    def update_urls_status(self):
        pass
    
    
if __name__ == "__main__":
    import time
    load_dotenv()
    frontier = Frontier()
    while True:
        frontier.load_urls_to_redis()
        time.sleep(int(os.getenv("INTERVAL", 5)))
        