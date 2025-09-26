

if __name__ == "__main__":
    """
    1. frontier 시작해서 url을 redis에 적재.
    2. fetcher가 frontier가 적재한 데이터를 가져와서 fetch후 저장.
    3. parser가 저장된 html을 parsing해서 url-status에 저장.
    4. frontier가 주기적으로 db에서 url을 가져와서 redis에 적재.
    """
    pass