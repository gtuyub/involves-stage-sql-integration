from sqlalchemy.orm import DeclarativeBaseNoMeta, Mapped, mapped_column, Session
from sqlalchemy import func, insert, update
from sqlalchemy.types import Integer, BigInteger
from typing import List,Dict,Any, Union
from abc import abstractmethod, ABC
from api_client.involves_api_client import InvolvesAPIClient
from .exceptions import InsertOperationError, UpdateOperationError


class Base(DeclarativeBaseNoMeta, ABC):

    __abstract__ = True

    id : Mapped[int] = mapped_column(Integer,primary_key=True,autoincrement=False)
    updated_at_millis : Mapped[int] = mapped_column(BigInteger)
    
    
    @classmethod
    def insert_records(cls, records : List[Dict[str,str]], db : Session) -> None:

        if records:
            try:
                db.execute(
                    insert(cls).execution_options(render_nulls=True),
                    records
                    )
            except Exception as e:
                raise InsertOperationError(f'Ocurrio un error al intentar realizar la operacion de insercion en la tabla {cls.__tablename__}: \n {e}')


    @classmethod
    def update_records(cls, records : List[Dict[str,str]], db : Session) -> None:

        if records:

            try:
                db.execute(
                    update(cls),
                    records
                )
            except Exception as e:
                raise UpdateOperationError(f'Ocurrio un error al intentar realizar la operacion de actualizacion en la tabla {cls.__tablename__}: \n {e}')
            
    @classmethod        
    def classify_records(cls, records: List[Dict[str, str]], db: Session, batch_size: int = 1000) -> Dict[str, List[Dict[str, str]]]:
        new_records = []
        modified_records = []

        primary_key = 'id'
        ids = [rec[primary_key] for rec in records]

        existing_records_ids = set()
        
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            existing_records = db.query(cls.id).filter(cls.id.in_(batch_ids)).all()
            existing_records_ids.update({r.id for r in existing_records})

        for rec in records:
            if rec[primary_key] in existing_records_ids:
                modified_records.append(rec)
            else:
                new_records.append(rec)

        return {
            'to_insert': new_records,
            'to_update': modified_records
        }
            
            
    @classmethod
    @abstractmethod
    def get_last_sync_time(cls, db : Session)-> Union[str,int]:

        last_sync = db.query(func.max(cls.updated_at_millis)).scalar()
        
        return last_sync if last_sync else 0
    

    @classmethod
    @abstractmethod
    def get_records_to_sync(cls, api_client : InvolvesAPIClient, db: Session) -> List[Dict[str,Any]]:
        pass     

            


