import pandas as pd


class InteractionProcessing:

    def __init__(self,iteraction_store_path:str):
        """задает путь к файлу хранения пользовательских взаимодействий"""
        self.file_path = iteraction_store_path
    
    def make_valid_format(self,user_id:int,track_id:int)->None:
        "из user_id и track_id создаю pd.Dataframe"
        self.us_iteraction= pd.DataFrame({"user_id":[user_id],"track_id":[track_id]})

    def store(self,user_id,track_id) -> None:
        """
        сохраняет последний прослушанный трек пользователся, в указанный файл
        при его отсутсвии, создает новый
        """
        import os

        self.make_valid_format(user_id,track_id)

        if not os.path.exists(self.file_path):
            # использую csv, чтобы глазами мог файл чекать, на проде - parquet
            self.us_iteraction.to_csv(self.file_path, mode='w', index=False)
        else:
            self.us_iteraction.to_csv(self.file_path, mode='a', index=False, header=False)

if __name__ == "__main__":
    us_iteraction = InteractionProcessing("data/us_actions.csv")
    us_iteraction.store(1,2)

        