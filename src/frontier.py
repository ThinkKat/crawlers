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
        
    def get_urls_from_db(self, maximum : int = 100) -> list[tuple]:
        """ Get urls_info from db
        """
        
        # DB에서 pending 상태의 url을 가져옴. 
        cur = self.conn.cursor()
        res = cur.execute("SELECT id, url FROM url_info WHERE status = ? LIMIT ?", ("pending", maximum))
        urls_info = res.fetchall()
        urls_id = [url_info[0] for url_info in urls_info]
        
        # Queueing으로 transition
        cur.execute("UPDATE url_info SET status = 'queueing' WHERE id IN (%s)" % ','.join('?'*len(urls_id)), urls_id)
        cur.close()
        
        # Commit
        self.conn.commit()
        return urls_info
    
    def load_urls_to_redis(self, urls_info: list[str]):
        """ Load urls
        """
        cur = self.conn.cursor()
        urls_id = []
        # Redis 적재
        for url in urls_info:
            self.r.rpush(os.getenv("REDIS_FRONTIER_QUEUE"), "|".join(url))
            urls_id.append(url[0])
        
        # Queued로 transition
        cur.execute("UPDATE url_info SET status = 'queued' WHERE id IN (%s)" % ','.join('?'*len(urls_id)), urls_id)
        
        cur.close()
        self.conn.commit()
        self.logger.info(f"Push the {len(urls_info)} of urls to redis queue")
        
    def update_urls_status(self):
        pass
    
    
if __name__ == "__main__":
    import time
    load_dotenv()
    frontier = Frontier()
    while True:
        urls_info = frontier.get_urls_from_db(maximum=100)
        
        frontier.load_urls_to_redis()
        time.sleep(int(os.getenv("INTERVAL", 5)))
        