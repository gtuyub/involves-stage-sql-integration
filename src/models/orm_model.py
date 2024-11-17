from typing import Any, Dict, List, Union
from .base import Base
from sqlalchemy.orm import Session
from sqlalchemy import func
import sqlalchemy.types as types
from sqlalchemy import Column
from sqlalchemy.types import Integer,String,Boolean, Date,DateTime, Float, BigInteger
from involves_api.client import InvolvesAPIClient
from datetime import datetime, timedelta


class CustomString(types.TypeDecorator):
    """Custom string type decorator which maps empty strings to NULL"""

    impl = types.String
    cache_ok = True

    def process_bind_param(self,value,dialect):
        if value == '' or value == ' ':
            return None
        return value
    
    def process_result_value(self, value, dialect):
        return value
    
    def copy(self, **kw):
        return CustomString(self.impl.length)



class Visit(Base):
    __tablename__ =  "visit"

    employee_id = Column(Integer)
    point_of_sale_id = Column(Integer)
    visit_date = Column(Date)
    visit_type = Column(String)
    visit_status = Column(String)
    manual_entry_date = Column(DateTime)
    manual_exit_date = Column(DateTime)
    gps_entry_date = Column(DateTime)
    gps_exit_date = Column(DateTime)
    visit_duration_manual = Column(Integer)
    visit_duration_gps = Column(Integer)
    is_deleted = Column(Boolean)
    updated_at_millis = Column(Integer)

    @classmethod
    def get_last_sync_time(cls, db: Session):
        return super().get_last_sync_time(db)
        
    @classmethod    
    def get_records_to_sync(cls, api_client : InvolvesAPIClient, db: Session) -> List[Dict[str, Any]]:
        return api_client.get_updated_visits(start_millis=cls.get_last_sync_time(db))


class PointOfSale(Base):
    __tablename__ =  "point_of_sale"

    point_of_sale_base_id = Column(Integer)
    point_of_sale_name = Column(String)
    chain = Column(String)
    chain_group = Column(String)
    channel = Column(String)
    point_of_sale_code = Column(String)
    region = Column(String)
    macro_region = Column(String)
    point_of_sale_type = Column(String)
    point_of_sale_profile = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    zip_code = Column(String)
    is_enabled = Column(Boolean)
    is_deleted = Column(Boolean)
    updated_at_millis = Column(BigInteger)

    @classmethod
    def get_last_sync_time(cls, db: Session) -> Union[str,int]:
        return super().get_last_sync_time(db)
    
    @classmethod
    def get_records_to_sync(cls,api_client : InvolvesAPIClient, db: Session) -> List[Dict[str, Any]]:
        return api_client.get_updated_points_of_sale(start_millis=cls.get_last_sync_time(db))

class Employee(Base):
    __tablename__ = "employee"

    employee_name = Column(String)
    employee_code = Column(String)
    is_field_team = Column(String)
    user_group = Column(String)
    leader_name = Column(String)
    is_enabled = Column(String)
    updated_at_millis = Column(BigInteger)

    @classmethod
    def get_last_sync_time(cls, db: Session) -> Union[str,int]:
        return super().get_last_sync_time(db)
    
    @classmethod
    def get_records_to_sync(cls, api_client : InvolvesAPIClient, db: Session) -> List[Dict[str, Any]]:
        return api_client.get_updated_employees(millis=cls.get_last_sync_time(db))


class Product(Base):
    __tablename__ = "product"

    product_name = Column(String)
    bar_code = Column(String)
    product_line = Column(String)
    is_active = Column(Boolean)
    is_deleted = Column(Boolean)
    updated_at_millis = Column(BigInteger)

    @classmethod
    def get_last_sync_time(cls, db: Session) -> Union[str,int]:
        return super().get_last_sync_time(db)
    
    @classmethod
    def get_records_to_sync(cls, api_client: InvolvesAPIClient, db: Session) -> List[Dict[str, Any]]:
        return api_client.get_updated_products(start_millis=cls.get_last_sync_time(db))


class Form(Base):
    __tablename__ = "form"

    form_name = Column(String)
    is_active = Column(Boolean)
    is_deleted = Column(Boolean)
    form_purpose = Column(String)
    requires_check_in = Column(Boolean)
    requires_point_of_sale = Column(Boolean)
    updated_at_millis = Column(BigInteger)

    @classmethod
    def get_last_sync_time(cls, db: Session) -> Union[str,int]:
        return super().get_last_sync_time(db)
    
    @classmethod
    def get_records_to_sync(cls, api_client: InvolvesAPIClient, db: Session) -> List[Dict[str, Any]]:
        return api_client.get_updated_forms(millis = cls.get_last_sync_time(db))


class FormField(Base):
    __tablename__ = "form_field"

    form_id = Column(Integer)
    field_name = Column(String)
    field_description = Column(String)
    field_order = Column(Integer)
    is_deleted = Column(Boolean)
    is_required = Column(Boolean)   

    @classmethod
    def get_last_sync_time(cls, db: Session) -> Union[str,int]:
        return super().get_last_sync_time(db)
    
    @classmethod
    def get_records_to_sync(cls, api_client: InvolvesAPIClient, db: Session) -> List[Dict[str, Any]]:
        return api_client.get_updated_form_fields(millis = cls.get_last_sync_time(db))



class FormResponse(Base):
    __tablename__ = "form_response"

    item_id = Column(Integer)
    replied_at = Column(DateTime)
    response_status = Column(String)
    time_spent = Column(BigInteger)
    form_id = Column(Integer)
    form_field_id = Column(Integer)
    employee_id = Column(Integer)
    point_of_sale_id = Column(Integer)
    product_id = Column(Integer)
    response_value = Column(CustomString)
    is_deleted = Column(Boolean)
    updated_at_millis = Column(BigInteger)

    @classmethod
    def get_last_sync_time(cls, db: Session) -> Union[str,int]:
        return super().get_last_sync_time(db)
    
    @classmethod
    def get_records_to_sync(cls, api_client: InvolvesAPIClient, db: Session) -> List[Dict[str, Any]]:
        return api_client.get_updated_form_responses(start_millis = cls.get_last_sync_time(db))


class EmployeeAbsence(Base):
    __tablename__ = "employee_absence"

    employee_id = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    absence_reason = Column(String)
    absence_note = Column(String)

    @classmethod
    def get_last_sync_time(cls, db: Session) -> Union[str,int]:

        millis = db.query(func.max(cls.updated_at_millis)).scalar()  
        if millis:      
            current_date = datetime.fromtimestamp(millis/1000)
            sync_date = current_date - timedelta(days=30)
            return sync_date.strftime('%Y-%m-%d')

    
    @classmethod
    def get_records_to_sync(cls, api_client: InvolvesAPIClient, db: Session) -> List[Dict[str, Any]]:
        return api_client.get_employee_absences(start_date=cls.get_last_sync_time(db))

