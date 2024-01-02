from enum import Enum
import json

with open("config/app_config.json") as app_config_json:
    app_config = json.load(app_config_json)

class key(Enum):
    ENV                 = app_config["enviorment"]
    FILE_FORMAT_ORC     = "ORC"
    FILE_FORMAT_CSV     = "CSV"
    FILE_FORMAT_PARQUET = "PARQUET"
    PRODUCTION          = "PROD"
    DEVELOPMENT         = "DEV"
    STAGING             = "STAGE"
    MASTER_YARN         = "yarn"
    MASTER_LOCAL        = "local"


