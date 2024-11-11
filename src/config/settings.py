from dataclasses import dataclass
from typing import Optional
from pathlib import Path
import os
from dotenv import load_dotenv
from config.config_block import IntegracionInvolves

@dataclass
class APIConfig:

    environment : str
    domain : str
    app_user : str
    app_password : str

@dataclass
class DatabaseConfig:

    username : str
    password : str
    server : str
    database : str


@dataclass
class Config:

    api : APIConfig
    db : DatabaseConfig

    @classmethod
    def load_from_env(cls,env_path: Optional[Path] = None, override : bool = False) -> 'Config':

        if env_path:
            load_dotenv(env_path,override=override)
        
        else:
            load_dotenv(override=override)
        
        api_config = APIConfig(

            environment = int(os.getenv('ENVIRONMENT')),
            domain = os.getenv('DOMAIN'),
            app_user = os.getenv('APP_USER'),
            app_password = os.getenv('APP_PASSWORD'),

        )

        db_config = DatabaseConfig(

            username = os.getenv('SQL_USER'),
            password = os.getenv('SQL_PASSWORD'),
            server = os.getenv('SERVER'),
            database = os.getenv('DATABASE')
        )

        return cls(api=api_config, db=db_config)
    
    @classmethod
    def load_from_block(cls, block_name : str, env_path: Optional[Path] = None) -> 'Config':

        try:
            block = IntegracionInvolves.load(f'{block_name}')

        except Exception:
            cls.create_block_from_env(block_name,env_path)
            block = IntegracionInvolves.load(f'{block_name}')

        api_config = APIConfig(

            environment = int(block.environment),
            domain = block.domain,
            app_user = block.app_user.get_secret_value(),
            app_password = block.app_password.get_secret_value()

        )

        db_config = DatabaseConfig(

            username = block.username.get_secret_value(),
            password = block.password.get_secret_value(),
            server = block.server,
            database = block.database
        )

        return cls(api=api_config, db=db_config)
    
    @classmethod
    def create_block_from_env(cls, block_name : str, env_path : Optional[Path] = None, overwrite_block : bool = False, override_env_vars : bool = False):

        if env_path:
            load_dotenv(env_path,override=override_env_vars)
        
        else:
            load_dotenv(override=override_env_vars)

        block = IntegracionInvolves(

            environment = int(os.getenv('ENVIRONMENT')),
            domain = os.getenv('DOMAIN'),
            app_user = os.getenv('APP_USER'),
            app_password = os.getenv('APP_PASSWORD'),
            username = os.getenv('SQL_USER'),
            password = os.getenv('SQL_PASSWORD'),
            server = os.getenv('SERVER'),
            database = os.getenv('DATABASE')
        )
        valid_block_name = block_name.lower().replace('_','-')
        block.save(valid_block_name,overwrite=overwrite_block)



    

    

