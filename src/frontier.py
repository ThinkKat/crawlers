import os
import sqlite3
import logging
import random
import time
from urllib.parse import urlparse

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
    
    
    def reset_urls_status(self):
        # Reset url status
        # TODO: Reset the domain queues
        cur = self.conn.cursor()
        cur.execute("UPDATE url_info SET status = 'pending' WHERE status = 'queueing'")
        cur.close()
        # Commit
        self.conn.commit()
        
    
    def load_urls_to_redis(self, urls_info: list[tuple]):
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
        

    # Add the politeness
    def load_urls_to_domain_queues(self, urls_info: list[tuple]):
        """ Load urls into domain queues
        """
        
        cnt_loaded_urls = 0
        
        # redis pipeline
        pipe = self.r.pipeline(transaction=True)
        rr_new_domains = set([])
        
        for url_id, url in urls_info:
            p = urlparse(url)
            domain = f"{p.netloc}".lower()
            
            dkey = f"{os.getenv("REDIS_DOMAIN_QUEUE_PREFIX")}{domain}"
            pipe.rpush(dkey, f"{url_id}|{url}")
            cnt_loaded_urls += 1
            
            if self.r.hexists(os.getenv("REDIS_SEEN_DOMAIN"), domain) is False:
                rr_new_domains.add(domain)
                
        pipe.execute()
        if rr_new_domains:
            pipe = self.r.pipeline(transaction=True)
            for d in rr_new_domains:
                pipe.hset(os.getenv("REDIS_SEEN_DOMAIN"), d, 1)
                pipe.rpush(os.getenv("REDIS_RR_LIST"), d)
                nkey = f"{os.getenv("REDIS_DOMAIN_NEXT_KEY")}{d}"
                pipe.setnx(nkey, time.time())
            pipe.execute()
            
        self.logger.info(f"도메인 큐 적재 완료: {cnt_loaded_urls} urls, 도메인 추가 {len(rr_new_domains)}개")
        
    def schedule_round_robin(self, max_moves_per_tick: int = 100) -> list[tuple]:
        """
        라운드로빈으로 도메인을 돌며, now >= next_eligible_at 이고
        도메인 큐에 URL이 있으면 한 개를 전역 ready queue로 이동.
        한 틱에서 최대 max_moves_per_tick개까지 이동.
        """
        moved = 0
        
        steps = 0
        now = time.time()
        
        urls_info = []
        while moved < max_moves_per_tick and steps < max(max_moves_per_tick, 1000):
            domain = self.r.rpoplpush(os.getenv("REDIS_RR_LIST"), os.getenv("REDIS_RR_LIST"))
            if domain is None: # 도메인이 없는 경우
                break
            domain = domain.decode()
            steps += 1
            
            nkey = f"{os.getenv("REDIS_DOMAIN_NEXT_KEY")}{domain}"
            next_at = float(self.r.get(nkey))
            
            dkey = f"{os.getenv("REDIS_DOMAIN_QUEUE_PREFIX")}{domain}"
            # 다음 시각이 안 됐거나 큐가 비었으면 스킵
            if now < next_at or self.r.llen(dkey) == 0:
                continue
            
            # 한 건 이동
            url = self.r.lpop(dkey)
            
            if not url:
                # 경쟁조건으로 pop 실패 시 skip
                continue
            
            urls_info.append(tuple(url.decode().split("|")))

            # delay + jitter 적용 후 next 갱신
            delay = random.uniform(1, 5)
            jitter = random.random() * 0.5
            self.r.set(nkey, now + delay + jitter)

            moved += 1
            
        if moved:
            self.logger.info(f"스케줄러: 각 도메인 큐에서 총 {moved}건의 url 꺼냄.")
            
        return urls_info
    
    
if __name__ == "__main__":
    import time
    load_dotenv()
    frontier = Frontier()
    
    try:
        while True:
            # DB에서 url 가져오기
            urls_info = frontier.get_urls_from_db(maximum=100)
            
            # 도메인별 큐 만들기
            frontier.load_urls_to_domain_queues(urls_info)
            
            # 라운드로빈으로 돌면서 적절한 도메인큐에서 url 가져오기
            urls_info_per_domain = frontier.schedule_round_robin()
            
            # 최종 frontier 큐에 적재
            frontier.load_urls_to_redis(urls_info_per_domain)
            time.sleep(int(os.getenv("INTERVAL", 5)))
            
    except KeyboardInterrupt:
        # frontier.reset_urls_status()
        # print("Reset the queueing urls to pending")
        print("Keyboard Interrupt")
        
        
        