import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager
from Recommendation_Offline import RecommendationsOffline
from Iteraction_Processing import IteractionProcessing
from Recommendation_Online import RecommendationOnline
logger = logging.getLogger("uvicorn.error")

# глобальные переменные
rec_offline = None  
rec_online = None 
us_iteraction = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting")

    global rec_offline, us_iteraction,rec_online
    us_iteraction = IteractionProcessing("data/us_actions.csv")
    rec_offline = RecommendationsOffline()
    rec_offline.load("personal",path="als_recommendations.parquet")
    rec_offline.load("default",path="top_100_popular.parquet")
    rec_online = RecommendationOnline("data/us_actions.csv")
    yield
    logger.info("Stopping")
    
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/put")
async def us_iteraction(user_id:int,track_id:int):
    """
    зыписывает пользовательское взаимодействие в файл
    """
    us_iteraction.store(user_id,track_id)
    logger.info('succeddfully stored')


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 10):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    
    # 2 списка с рекомендациями, RecOnline может быть none
    RecOffline = rec_offline.get(user_id = user_id, k=k)
    RecOnline = rec_online.get_rec(user_id=user_id)
    
    if RecOnline.get(user_id) is None:
        rec = RecOffline
    else:
        if RecOnline[user_id][0] == RecOnline[user_id][-1]:
            rec = RecOffline
        else:
            rec = RecOnline[user_id][1:3] + RecOffline[3:3+k]
    

    return {"recs": rec}