import requests
from requests.auth import HTTPBasicAuth
import time
from typing import Optional, List, Dict, Any
import logging

class InvolvesAPIClient(requests.Session):

    def __init__(self,environment,domain,username,password):

        super().__init__()

        self.environment = environment
        self.username = username
        self.password = password
        self.base_url = f"https://{domain}.involves.com/webservices/api"
        self.auth = HTTPBasicAuth(self.username,self.password)

        self.headers.update({
            'X-AGILE-CLIENT' : 'EXTERNAL_APP',
            'Accept-Version' : '2020-02-26'
        })

    def get_updated_visits(self, millis : int) -> List[Dict[str,Any]]:

        records = []

        while True:

            request_url = f'{self.base_url}/v1/{self.environment}/visit/sync/timestamp/{millis}?count=true'

            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth)
            response.raise_for_status() 

            items = response.json().get('items')
            millis = response.json().get('timestampLastItem')

            if items:
                for d in items:
                    row = {

                        'id' : d.get('id'),
                        'employee_id' : d.get('employee').get('id') if isinstance(d.get('employee'),dict) else None,
                        'point_of_sale_id' : d.get('pointOfSale').get('id') if isinstance(d.get('pointOfSale'),dict) else None,
                        'visit_date' : d.get('visitDate'),
                        'visit_type' : d.get('type'),
                        'visit_status' : d.get('status'),
                        'manual_entry_date' : d.get('entryDateManualCheckin'),
                        'manual_exit_date' :  d.get('exitDateManualCheckin'),
                        'gps_entry_date' : d.get('entryDateGPSCheckin'),
                        'gps_exit_date' : d.get('exitDateGPSCheckin'),
                        'visit_duration_manual' : d.get('visitDurationCheckinManual'),
                        'visit_duration_gps' : d.get('visitDurationCheckinGPS'),
                        'updated_at_millis' : d.get('updatedAtMillis'),
                        'is_deleted' : d.get('deleted')  
                        }
                    
                    records.append(row)

            if not millis:
                break

        return records
    
    def get_updated_points_of_sale(self, millis : int) -> List[Dict[str,Any]]:

        records = []

        while True:

            request_url = f'{self.base_url}/v1/{self.environment}/pointofsale/sync/timestamp/{millis}?count=true'

            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth)
            response.raise_for_status()

            items = response.json().get('items')
            millis = response.json().get('timestampLastItem')
            update_timestamp = round(time.time()*1000)

            if items:
                for d in items:
                    row = {

                        'id' : d.get('id'),
                        'point_of_sale_base_id' : d.get('pointOfSaleBaseId'),
                        'point_of_sale_name' : d.get('name'),
                        'chain' : d.get('chain').get('name') if isinstance(d.get('chain'),dict) else None,
                        'chain_group' : d.get('chain').get('chainGroup').get('name') if isinstance(d.get('chain'),dict) and isinstance(d.get('chain').get('chainGroup'),dict) else None,
                        'channel' : d.get('pointOfSaleChannel').get('name') if isinstance(d.get('pointOfSaleChannel'),dict) else None,
                        'point_of_sale_code' : d.get('code'),
                        'point_of_sale_region' : d.get('region').get('name') if isinstance(d.get('region'),dict) else None,
                        'macro_region' : d.get('region').get('macroRegion').get('name') if isinstance(d.get('region'),dict) and isinstance(d.get('region').get('macroRegion'),dict) else None,
                        'point_of_sale_type' : d.get('pointOfSaleType').get('name') if isinstance(d.get('pointOfSaleType'),dict) else None, 
                        'point_of_sale_profile' : d.get('pointOfSaleProfile').get('name') if isinstance(d.get('pointOfSaleProfile'),dict) else None,
                        'latitude' : d.get('address').get('latitude') if isinstance(d.get('address'),dict) else None,
                        'longitude' : d.get('address').get('longitude') if isinstance(d.get('address'),dict) else None,
                        'zip_code' : d.get('address').get('zipCode') if isinstance(d.get('address'),dict) else None,
                        'is_enabled' : d.get('enabled'),
                        'is_deleted' : d.get('deleted'),
                        'updated_at_millis' : update_timestamp,


                    }

                    records.append(row)
            
            if not millis:
                break
        
        return records
    
    def get_updated_employees(self,millis : int) ->List[Dict[str,Any]]:

        records = []
        page = 1

        while True:         

            request_url = f'{self.base_url}/v1/{self.environment}/employeeenvironment?updatedAtMillis={millis}&page={page}&size=200'

            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth)
            response.raise_for_status()

            items = response.json().get('items')
            total_pages = response.json().get('totalPages')

            if items:
                for d in items:
                    row = {

                        'id' : d.get('id'),
                        'employee_name' : d.get('name'),
                        'employee_code' : d.get('nationalIdCard2'),
                        'is_field_team' : d.get('fieldTeam'),
                        'user_group' : d.get('userGroup').get('name') if isinstance(d.get('userGroup'),dict) else None,
                        'leader_name' : d.get('employeeEnvironmentLeader').get('name') if isinstance(d.get('employeeEnvironmentLeader'),dict) else None,
                        'is_enabled' : d.get('enabled'),
                        'updated_at_millis' : d.get('userUpdatedAtMillis')
                        }
                    
                    records.append(row)

            if page >= total_pages:
                break

            page +=1    

        return records

    def get_updated_products(self, millis : int) -> List[Dict[str,Any]]:

        records = []

        while True:

            request_url = f'{self.base_url}/v1/{self.environment}/sku/sync/timestamp/{millis}?count=true'

            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth)
            response.raise_for_status()

            items = response.json().get('items')
            millis = response.json().get('timestampLastItem')

            if items:
                for d in items:
                    row = {

                        'id' : d.get('id'),
                        'product_name' : d.get('name'),
                        'bar_code' : d.get('barCode'),
                        'product_line' : d.get('productLine').get('name') if isinstance(d.get('productLine'),dict) else None,
                        'is_active' : d.get('active'),
                        'is_deleted' : d.get('deleted'),
                        'updated_at_millis' : d.get('updatedAtMillis')
                    }

                    records.append(row)
            
            if not millis:
                break
        
        return records
    
    def get_updated_forms(self, millis : int) -> Dict[str,List[Dict[str,Any]]]:

        records = []

        while True:

            request_url = f'{self.base_url}/v1/{self.environment}/form/sync/timestamp/{millis}?count=true'

            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth)
            response.raise_for_status()

            forms = response.json().get('items')
            millis = response.json().get('timestampLastItem')

            if forms:
                for d in forms:

                    row = {
                        'id' : d.get('id'),
                        'form_name' : d.get('name'),
                        'is_active' : d.get('active'),
                        'is_deleted' : d.get('deleted'),
                        'form_purpose' : d.get('formPurpose'),
                        'requires_check_in' : d.get('checkinRequired'),
                        'requires_point_of_sale' : d.get('pointOfSaleRequired'),
                        'updated_at_millis' : d.get('updatedAtMillis')

                    }

                    records.append(row)               

            if not millis:
                break

        return records
    
    def get_updated_form_fields(self, millis : int) -> List[Dict[str,Any]]:

        records = []

        while True:

            request_url = f'{self.base_url}/v1/{self.environment}/form/sync/timestamp/{millis}?count=true'

            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth)
            response.raise_for_status()

            forms = response.json().get('items')
            millis = response.json().get('timestampLastItem')

            if forms:
                for d in forms:
                    for field in d.get('formFields'):

                        field_row = {
                            'id' : field.get('id'),
                            'form_id' : d.get('id'),
                            'field_name' : field.get('information').get('label') if isinstance(field.get('information'),dict) else None,
                            'field_description' : field.get('information').get('alternativeLabel') if isinstance(field.get('information'),dict) else None,
                            'field_order' : field.get('order'),
                            'is_deleted' : field.get('deleted'),
                            'is_required' : field.get('required'),
                            'updated_at_millis' : d.get('updatedAtMillis')
                        }

                        records.append(field_row)


            if not millis:
                break

        return records


    def get_updated_form_responses(self, millis : int)-> List[Dict[str,Any]]:

        records = []

        while True:

            request_url = f'{self.base_url}/v1/{self.environment}/survey/sync/timestamp/{millis}?count=true'

            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth)
            response.raise_for_status()

            items = response.json().get('items')
            millis = response.json().get('timestampLastItem')

            if items:
                for d in items:
                    for answer in d.get('surveyData'):

                        row = {

                        'id' : answer.get('id'),
                        'item_id' : d.get('id'),
                        'replied_at' : d.get('repliedAt'),
                        'response_status' : d.get('status'),
                        'time_spent' : d.get('timeSpent'),
                        'form_id' : d.get('form').get('id') if isinstance(d.get('form'),dict) else None,
                        'form_field_id' : answer.get('formField').get('id') if isinstance(answer.get('formField'),dict) else None,
                        'employee_id' : d.get('assignedTo').get('id') if isinstance(d.get('assignedTo'),dict) else None,
                        'point_of_sale_id' : d.get('pointOfSale').get('id') if isinstance(d.get('pointOfSale'),dict) else None,
                        'product_id' : answer.get('sku').get('id') if isinstance(answer.get('sku'),dict) else None,
                        'response_value' : answer.get('value'),
                        'is_deleted' : d.get('deleted'),
                        'updated_at_millis' : d.get('updatedAtMillis')
                            
                        }

                        records.append(row)
            
            if not millis:
                break
        
        return records
    
    def get_employee_absences(self, start_date : Optional[str] = None) -> List[Dict[str,Any]]:

        records = []
        page = 1

        while True:         

            request_url = f'{self.base_url}/v1/{self.environment}/employeeabsence?page={page}&size=200'

            if start_date:
                request_url += f'&startDate={start_date}'

            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth)
            response.raise_for_status()

            items = response.json().get('items')
            total_pages = response.json().get('totalPages')
            update_timestamp = round(time.time()*1000)

            if items:
                for d in items:
                    row = {

                        'id' : d.get('id'),
                        'employee_id' : d.get('employeeEnvironmentSuspended').get('id') if isinstance(d.get('employeeEnvironmentSuspended'),dict) else None,
                        'start_date' : d.get('absenceStartDate'),
                        'end_date' : d.get('absenceEndDate'),
                        'absence_reason' : d.get('reasonNote'),
                        'absence_note' : d.get('absenceNote'),
                        'updated_at_millis' : update_timestamp
                        }
                    
                    records.append(row)

            if page >= total_pages:
                break

            page +=1

        return records
    