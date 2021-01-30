import json


class Conf:
    def load_data(self, file_path: str = "conf.json"):
        with open(file_path, "r") as file:
            _data = json.loads(file.read())

        self.DB_Host = _data["dbHost"]
        self.DB_NAME = _data["dbName"]
        self.DB_USER = _data["dbUser"]
        self.DB_PWD = _data["dbPwd"]
        self.PIXIV_USER = _data["pixivUser"]
        self.PIXIV_PWD = _data["pixivPwd"]


CONF = Conf()
