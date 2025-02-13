import requests

import logging 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)



url = "http://0.0.0.0:9090/recommendations?user_id={user_id}&k=10"

def resp(f):
    def wrapper(*args, **kwargs):
        response = f(*args, **kwargs) 
        print("Статус код:", response.status_code)
        print("Ответ:", response.text)
        return response  
    return wrapper

@resp
def get_resp(url):
    return requests.post(url)  

user_id = 2

get_resp(url.format(user_id=user_id))  

logger.info("для пользователя без персональных рекомендаций")

user_id = 4

get_resp(url.format(user_id=user_id))  

logger.info("для пользователя с персональными рекомендациями, но без онлайн-истории")


get_resp(url.format(user_id=user_id))  

logger.info("для пользователя с персональными рекомендациями и онлайн-историей")