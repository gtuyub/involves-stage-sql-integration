from prefect.blocks.core import Block
from pydantic.v1 import SecretStr
from typing import Optional

class IntegracionInvolves(Block):

    environment : str
    domain : str
    app_user : Optional[SecretStr]  
    app_password : Optional[SecretStr]
    username : Optional[SecretStr]
    password : Optional[SecretStr]
    server : str
    database : str