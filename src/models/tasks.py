from sqlalchemy import Engine, create_engine
from .exceptions import SQLEngineError
from .base import Base
import logging
import importlib 
import inspect

logger = logging.getLogger(__name__)

def create_db_engine(server : str, database : str, username : str, password : str) -> Engine:

    connection_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_engine(connection_url)
    try:
        connection = engine.connect()
        connection.close()
        logger.info(f'SQLAlchemy connection with context server : "{server}" database : "{database}" tested successfully.')
        return engine
    except Exception as e:
        raise SQLEngineError(f'Cannot create database engine with context:\n server : {server} \n database : {database}\n Error : {e}')
    


def get_models_to_sync(env : int):
    """devuelve una lista con los modelos a actualizar de acuerdo al numero de ambiente en involves."""


    models_module = importlib.import_module('.orm_model',package='models')

    models = [
            cls for _,cls in inspect.getmembers(models_module,inspect.isclass) 
            if issubclass(cls,Base) and cls is not Base
            ]

    logger.info(f'Tables retrieved for update :\n {[model.__tablename__ for model in models]}')

    if env == 5:

        models_to_sync = [c for c in models if c.__name__ != 'EmployeeAbsence']

    else:
        models_to_sync = models

    return models_to_sync