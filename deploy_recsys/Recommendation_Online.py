import pandas as pd

import logging 
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class RecommendationOnline:
    """
    класс для обработки онлан рекомендаций
    """
    def __init__(self,iteractions_store_path:str):

        try:
            # использую csv, чтобы глазами мог файл чекать, на проде - parquet
            self.data = pd.read_csv(iteractions_store_path)
            self.sim_tracks = pd.read_parquet("sim_tracks.parquet")
            logger.info("files was load successful")
        except Exception as e:
            logger.error(f"file not found, additional information: {e}")
            sys.exit(1)
    
    def get_sim(self,user_id):
        """
        возвращает словарь
        ключ - ключ
        значения - похожие треки
        {track_id:[]}
        """
        valid_data= self.data.loc[self.data["user_id"]==user_id]

        if valid_data["user_id"].tolist()==[]:
            logger.info("No users actions")
            return {-1:None}

        if valid_data.shape[0] == 1:
            logger.info("1 user actions")
            sim = (self.sim_tracks["track_id_2"].loc[self.sim_tracks["track_id_1"] == valid_data["track_id"].tolist()[0]]).tolist()
            if sim == []:
                sim = None
            
            return {valid_data["track_id"].tolist()[0]:sim}

        logger.info("more then 2 users actions")
        valid_data = valid_data.tail(2) # два новых прослушанных трека

        sim_for_current_tracks = {}
        for track in valid_data["track_id"].tolist():
            #в словрь может и null добивить, если на этот трек нет рекомендаций
            sim = (self.sim_tracks["track_id_2"].loc[self.sim_tracks["track_id_1"] == track]).tolist()
            if sim == []:
                sim = None
            sim_for_current_tracks[track] = sim

        return sim_for_current_tracks

    def get_rec(self,user_id:int,k:int=5):
        """
        пустой список если на пользователя нет данных
        иначе
        беру похожие треки на 3 последних прослушанных и пересекаю
        нет пересечения - просто беру самые популярные из похожих
        """
        rec = self.get_sim(user_id=user_id)

        if list(rec.values())==[None]:
            return {-1:None}
        
        if len(rec.keys()) == 1 or None in rec.values():
            return {user_id:list(rec.values())}
        
        #если для 2 последних треков нашли похожие, возвращаю пересечение 
        #если пересечения нет, то самые популярные треки из похожих 
        track_1, track_2 = rec.values()


        intersection = list (set(track_1) & set (track_2))
        if intersection == []:
            logger.info("no intersection")
            return {user_id:track_1[:len(track_1)//k] +track_2[:abs(k - len(track_1)//k)]}
        
        return {user_id:intersection[:k]}


if __name__ =="__main__":
    rec_online = RecommendationOnline("data/us_actions.csv")
    rec = rec_online.get_rec(1)
    print(rec)


