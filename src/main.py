from prefect import task, flow, get_run_logger
from prefect.artifacts import create_table_artifact
from sqlalchemy.orm import Session
import logging
from typing import Type, Optional
from models.base import Base
from models.exceptions import SyncError
from sqlalchemy.orm import sessionmaker
from api_client.involves_api_client import InvolvesAPIClient
from models.tasks import create_db_engine, get_models_to_sync
from config.settings import Config

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@task(task_run_name = 'sincronizar-tabla-{model.__tablename__}')
def sync_table(api_client : InvolvesAPIClient, model : Type[Base], db : Session) -> None:

    logger = get_run_logger()

    table_name = model.__tablename__

    logger.info(f'iniciando proceso de sincronizacion tabla : {table_name}')
    data = model.get_records_to_sync(api_client,db)
    logger.info(f'{len(data)} registros obtenidos tabla : {table_name}.')

    classified_data = model.classify_records(data,db)

    new_records = classified_data['to_insert']
    modified_records = classified_data['to_update']

    try:

        if new_records or modified_records:

            if new_records:
                logger.info(f'{len(new_records)} registros nuevos encontrados para insertar en la tabla {table_name}')
                model.insert_records(new_records,db)
                create_table_artifact(new_records,'registros-nuevos')
                logger.info('registros insertados exitosamente.')
            if modified_records:
                logger.info(f'{len(modified_records)} registros modificados encontrados para actualizar en la tabla {table_name}')
                model.update_records(modified_records,db)
                create_table_artifact(modified_records, 'registros-actualizados')
                logger.info('registros actualizados exitosamente.')


            db.commit()

        else:
            logger.info(f'No hay registros nuevos para insertar o modificar en la tabla {table_name}')

    except Exception as e:
        db.rollback()
        logger.error(f'no se pudo actualizar la tabla : {table_name} debido al siguiente error :\n {e}')
        raise SyncError from e 


@flow(name='sincronizar_datos_involves')
def main(config_block : Optional[str] = None):

    logger = get_run_logger()

    try:

        config = Config.load_from_block(config_block) if config_block else Config.load_from_env()
        engine = create_db_engine(config.db.server, config.db.database, config.db.username, config.db.password)
        Session = sessionmaker(engine)
        api_client = InvolvesAPIClient(config.api.environment, config.api.domain, config.api.app_user, config.api.app_password)
    
    except Exception as e:
    
        logger.critical(f'No se pudo ejecutar el flujo debido a un error critico: \n {e}')
        raise

    models = get_models_to_sync(config.api.environment)

    for tbl in models:
        with Session() as db:
            sync_table(api_client,tbl,db)



if __name__ == "__main__" :

    main()