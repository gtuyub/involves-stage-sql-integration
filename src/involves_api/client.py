import requests
from requests.auth import HTTPBasicAuth
import time
import logging
from typing import Optional, List, Dict, Any, Callable, Union, TypeVar
T = TypeVar('T')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class InvolvesAPIClient(requests.Session):
    """A client for interacting with the Involves Stage API."""

    def __init__(self,environment,domain,username,password):
        """Initializes the API client with basic authentication."""
        super().__init__()

        self.environment = environment
        self.username = username
        self.password = password
        self.domain = domain
        self.base_url = f"https://{self.domain}.involves.com/webservices/api"
        self.auth = HTTPBasicAuth(self.username,self.password)

        self.headers.update({
            'X-AGILE-CLIENT' : 'EXTERNAL_APP',
            'Accept-Version' : '2020-02-26'
        })

        logger.info(f'initialized involves_api_client at: \n env : {self.environment}. \n domain : {self.domain}.')


    def _paginated_request_with_timestamp(self, url : str, start_millis : Optional[int] = None, end_millis : Optional[int] = None, params : Dict[str,Any] = None, fetch_func : Callable[[Dict[str,Any]], Union[T,List[T]]] = None) -> List[T]:
        """
        Get records modified or created on a specific interval in milliseconds from the provided API URL.

        Parameters:
            url (str): The base URL for the API endpoint.
            start_millis (Optional[int]): The starting timestamp in milliseconds to use as a parameter. If not provided defaults to 0.
            end_millis (Optional[int]): The ending timestamp in milliseconds to use as a parameter. If not provided the method returns all records modified after start_millis.
            params (Dict[str, Any], optional): Additional parameters to include in the request.
            fetch_func (Callable[[Dict[str, Any]], Union[T, List[T]]], optional): A function to transform each dict from the API response data into the desired format.

        Returns:
            List[T]: A list of records created or modified after start_millis and before end_millis.
        """
        records = []

        if not fetch_func:
            fetch_func = lambda x : x

        default_params = {
            'size' : 100
        }

        if params:
            default_params.update(params)

        millis = start_millis
        
        while True:

            request_url = f'{url}{millis if millis else 0}'
            response = super().request(method='GET',url=request_url,headers=self.headers,auth=self.auth, params=default_params)
            logger.info(f'GET request at URL : \n {request_url}. \n status_code = {response.status_code}')

            response.raise_for_status()

            response_data : Dict = response.json()
            
            items = response_data.get('items')
            millis = response_data.get('timestampLastItem')
            logger.info(f'timestamp of next request : {millis}')

            if items:
                logger.info(f'request response includes {len(items)} items.')
                for item in items:
                    row = fetch_func(item)

                    if isinstance(row,list):
                        records.extend(row)
                    else:
                        records.append(row)

            if not millis or (end_millis is not None and millis >= end_millis):
                logger.info(f'timestampLastItem not found in response or end millis reached, paginated request finished with a total of {len(records)} items.')
                break

        return records
    
    def _paginated_request_with_page(self, url : str, params : Dict[str,Any] = None, fetch_func : Callable[[Dict[str,Any]], Union[T,List[T]]] = None) -> List[T]:
        """
        Get records from the provided API URL with pagination.

        Parameters:
            url (str): The base URL for the API endpoint.
            params (Dict[str, Any], optional): Additional parameters to include in the request.
            fetch_func (Callable[[Dict[str, Any]], Union[T, List[T]]], optional): A function to transform each dict from the API response data into the desired format.

        Returns:
            List[T]: A list of records obtained from the URL.
        """

        records = []
        page = 1
        if not fetch_func:
            fetch_func = lambda x : x
        
        default_params = {
            'size' : 200,
            'page' : page
        }

        if params:
            default_params.update(params)

        while True:         

            response = super().request(method='GET',url=url,headers=self.headers,auth=self.auth, params=default_params)
            logger.info(f'GET request at URL : \n {url}. \n status_code = {response.status_code}')

            response.raise_for_status()

            response_data : Dict = response.json()

            if 'items' in response_data:

                items = response_data.get('items')

            else:

                items = response_data

            logger.info(f'request response includes {len(items)} items.')


            if items:
                for item in items:
                    row = fetch_func(item)

                    if isinstance(row,list):
                        records.extend(row)
                    else:
                        records.append(row)

            
            total_pages = response_data.get('totalPages') if isinstance(response_data,dict) else 1
            logger.info(f'page progress : {page}/{total_pages}')
                

            if page >= total_pages or total_pages is None:
                logger.info(f'Paginated request finished with a total of {len(records)} items.')
                break

            page +=1  
            default_params.update({'page':page}) 

        return records


    def get_updated_visits(self, start_millis : Optional[int] = None, end_millis : Optional[int] = None ) -> List[Dict[str,Any]]:
        """
        Get visits updated after start_millis and before end_millis 

        Parameters:
            start_millis (Optional[int]): The starting timestamp in milliseconds to use as a parameter. If not provided returns all records.
            end_millis (Optional[int]): The ending timestamp in milliseconds to use as a parameter. If not provided the method returns all records modified after start_millis.

        Returns:
            List[T]: A list of dictionaries representing visits.
        """

        request_url = f'{self.base_url}/v1/{self.environment}/visit/sync/timestamp/'
        return self._paginated_request_with_timestamp(
                url=request_url,
                start_millis = start_millis,
                end_millis = end_millis,             
                fetch_func= lambda x : {
                        'id' : x.get('id'),
                        'employee_id' : x.get('employee',{}).get('id') if isinstance(x.get('employee',{}),dict) else None,
                        'point_of_sale_id' : x.get('pointOfSale',{}).get('id') if isinstance(x.get('pointOfSale',{}),dict) else None,
                        'visit_date' : x.get('visitDate'),
                        'visit_type' : x.get('type'),
                        'visit_status' : x.get('status'),
                        'manual_entry_date' : x.get('entryDateManualCheckin'),
                        'manual_exit_date' :  x.get('exitDateManualCheckin'),
                        'gps_entry_date' : x.get('entryDateGPSCheckin'),
                        'gps_exit_date' : x.get('exitDateGPSCheckin'),
                        'visit_duration_manual' : x.get('visitDurationCheckinManual'),
                        'visit_duration_gps' : x.get('visitDurationCheckinGPS'),
                        'updated_at_millis' : x.get('updatedAtMillis'),
                        'is_deleted' : x.get('deleted')  
            }
            )
    
    def get_updated_points_of_sale(self, start_millis : Optional[int] = None, end_millis : Optional[int] = None) -> List[Dict[str,Any]]:
        """
        Get points of sale updated after start_millis and before end_millis 

        Parameters:
            start_millis (Optional[int]): The starting timestamp in milliseconds to use as a parameter. If not provided returns all records.
            end_millis (Optional[int]): The ending timestamp in milliseconds to use as a parameter. If not provided the method returns all records modified after start_millis.

        Returns:
            List[T]: A list of dictionaries representing points of sale.
        """
        request_url = f'{self.base_url}/v1/{self.environment}/pointofsale/sync/timestamp/'
        update_timestamp = round(time.time()*1000)
        return self._paginated_request_with_timestamp(
                url=request_url,
                start_millis = start_millis,
                end_millis = end_millis,
                fetch_func = lambda x :  {

                        'id' : x.get('id'),
                        'point_of_sale_base_id' : x.get('pointOfSaleBaseId'),
                        'point_of_sale_name' : x.get('name'),
                        'chain' : x.get('chain',{}).get('name') if isinstance(x.get('chain',{}),dict) else None,
                        'chain_group' : x.get('chain',{}).get('chainGroup',{}).get('name') if isinstance(x.get('chain',{}),dict) else None,
                        'channel' : x.get('pointOfSaleChannel',{}).get('name') if isinstance(x.get('pointOfSaleChannel',{}),dict) else None,
                        'point_of_sale_code' : x.get('code'),
                        'point_of_sale_region' : x.get('region',{}).get('name') if isinstance(x.get('region',{}),dict) else None,
                        'macro_region' : x.get('region',{}).get('macroRegion',{}).get('name') if isinstance(x.get('region',{}),dict) else None,
                        'point_of_sale_type' : x.get('pointOfSaleType',{}).get('name') if isinstance(x.get('pointOfSaleType',{}),dict) else None, 
                        'point_of_sale_profile' : x.get('pointOfSaleProfile',{}).get('name') if isinstance(x.get('pointOfSaleProfile'),dict) else None,
                        'latitude' : x.get('address',{}).get('latitude') if isinstance(x.get('address',{}),dict) else None,
                        'longitude' : x.get('address',{}).get('longitude') if isinstance(x.get('address',{}),dict) else None,
                        'zip_code' : x.get('address',{}).get('zipCode') if isinstance(x.get('address',{}),dict) else None,
                        'is_enabled' : x.get('enabled'),
                        'is_deleted' : x.get('deleted'),
                        'updated_at_millis' : update_timestamp
                    }
                    )

    
    def get_updated_employees(self,millis : Optional[int] = None) ->List[Dict[str,Any]]:
        """
        Get employees updated after millis.

        Parameters:
            millis (Optional[int]): The starting timestamp in milliseconds to use as a parameter. If not provided returns all records.

        Returns:
            List[T]: A list of dictionaries representing employees.
        """
        request_url = f'{self.base_url}/v1/{self.environment}/employeeenvironment/'
        params = {'updatedAtMillis' : millis} if millis else None

        return self._paginated_request_with_page(
                url=request_url,
                params = params,
                fetch_func = lambda x : {

                        'id' : x.get('id'),
                        'employee_name' : x.get('name'),
                        'employee_code' : x.get('nationalIdCard2'),
                        'is_field_team' : x.get('fieldTeam'),
                        'user_group' : x.get('userGroup',{}).get('name') if isinstance(x.get('userGroup',{}),dict) else None,
                        'leader_name' : x.get('employeeEnvironmentLeader',{}).get('name') if isinstance(x.get('employeeEnvironmentLeader',{}),dict) else None,
                        'is_enabled' : x.get('enabled'),
                        'updated_at_millis' : x.get('userUpdatedAtMillis')
                        }
                    )

    def get_updated_products(self, start_millis : Optional[int] = None, end_millis : Optional[int] = None) -> List[Dict[str,Any]]:
        """
        Get products updated after start_millis and before end_millis 

        Parameters:
            start_millis (Optional[int]): The starting timestamp in milliseconds to use as a parameter. If not provided returns all records.
            end_millis (Optional[int]): The ending timestamp in milliseconds to use as a parameter. If not provided the method returns all records modified after start_millis.

        Returns:
            List[T]: A list of dictionaries representing products.
        """

        request_url = f'{self.base_url}/v1/{self.environment}/sku/sync/timestamp/'
        return self._paginated_request_with_timestamp(
                url=request_url,
                start_millis = start_millis,
                end_millis = end_millis,
                fetch_func= lambda x : {

                        'id' : x.get('id'),
                        'product_name' : x.get('name'),
                        'bar_code' : x.get('barCode'),
                        'product_line' : x.get('productLine',{}).get('name') if isinstance(x.get('productLine',{}),dict) else None,
                        'is_active' : x.get('active'),
                        'is_deleted' : x.get('deleted'),
                        'updated_at_millis' : x.get('updatedAtMillis')
                    }
            )

    
    def get_updated_forms(self, millis : Optional[int] = None)  -> Dict[str,List[Dict[str,Any]]]:
        """
        Get forms updated after millis.

        Parameters:
            millis (Optional[int]): The starting timestamp in milliseconds to use as a parameter. If not provided returns all records.

        Returns:
            List[T]: A list of dictionaries representing forms.
        """

        request_url = f'{self.base_url}/v1/{self.environment}/form/sync/timestamp/'
        return self._paginated_request_with_timestamp(
                url=request_url,
                start_millis=millis,
                fetch_func= lambda x : {
                        'id' : x.get('id'),
                        'form_name' : x.get('name'),
                        'is_active' : x.get('active'),
                        'is_deleted' : x.get('deleted'),
                        'form_purpose' : x.get('formPurpose'),
                        'requires_check_in' : x.get('checkinRequired'),
                        'requires_point_of_sale' : x.get('pointOfSaleRequired'),
                        'updated_at_millis' : x.get('updatedAtMillis')

                    }
            )
    
    def get_updated_form_fields(self, millis: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get form fields updated after millis.

        Parameters:
            millis (Optional[int]): The starting timestamp in milliseconds to use as a parameter. If not provided returns all records.

        Returns:
            List[T]: A list of dictionaries representing form fields.
        """

        request_url = f'{self.base_url}/v1/{self.environment}/form/sync/timestamp/'
        def fetch_func(form_data: Dict[str, Any]) -> List[Dict[str, Any]]:
            field_records = []
            for field in form_data.get('formFields', []):
                field_row = {
                    'id': field.get('id'),
                    'form_id': form_data.get('id'),
                    'field_name': field.get('information', {}).get('label') if isinstance(field.get('information', {}),dict) else None,
                    'field_description': field.get('information', {}).get('alternativeLabel') if isinstance(field.get('information', {}),dict) else None,
                    'field_order': field.get('order'),
                    'is_deleted': field.get('deleted'),
                    'is_required': field.get('required'),
                    'updated_at_millis': form_data.get('updatedAtMillis')
                }
                field_records.append(field_row)
            return field_records
    
        return self._paginated_request_with_timestamp(
            url=request_url,
            start_millis=millis,
            fetch_func=fetch_func
        )
    
    def get_updated_form_responses(self, start_millis: Optional[int] = None, end_millis: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get form responses updated after start_millis and before end_millis.

        Parameters:
            start_millis (Optional[int]): The starting timestamp in milliseconds to use as a parameter. If not provided returns all records.
            end_millis (Optional[int]): The ending timestamp in milliseconds to use as a parameter. If not provided returns all records.


        Returns:
            List[T]: A list of dictionaries representing form responses.
        """

        request_url = f'{self.base_url}/v1/{self.environment}/survey/sync/timestamp/'
        
        def fetch_func(survey_data: Dict[str, Any]) -> List[Dict[str, Any]]:
            response_records = []
            for answer in survey_data.get('surveyData'):
                row = {
                    'id': answer.get('id'),
                    'survey_id': survey_data.get('id'),
                    'replied_at': survey_data.get('repliedAt'),
                    'time_spent': survey_data.get('timeSpent'),
                    'form_id': survey_data.get('form', {}).get('id') if isinstance(survey_data.get('form', {}),dict) else None,
                    'form_field_id': answer.get('formField', {}).get('id') if isinstance(answer.get('formField', {}),dict) else None,
                    'employee_id': survey_data.get('assignedTo', {}).get('id') if isinstance(survey_data.get('assignedTo', {}),dict) else None,
                    'point_of_sale_id': survey_data.get('pointOfSale', {}).get('id') if isinstance(survey_data.get('pointOfSale', {}),dict) else None,
                    'product_id': answer.get('sku', {}).get('id') if isinstance(answer.get('sku'),dict) else None,
                    'response_value': answer.get('value'),
                    'is_deleted': survey_data.get('deleted'),
                    'updated_at_millis': survey_data.get('updatedAtMillis')
                }
                response_records.append(row)
            return response_records

        return self._paginated_request_with_timestamp(
            url=request_url,
            start_millis=start_millis,
            end_millis=end_millis,
            fetch_func=fetch_func
        )
    
    def get_employee_absences(self, start_date : Optional[str] = None) -> List[Dict[str,Any]]:
        """
        Get employee absences valid from start_date.

        Parameters:
            start_date (Optional[str]): The starting date as string in format 'YYYY-mm-dd'.

        Returns:
            List[T]: A list of dictionaries representing absences.
        """

        request_url = f'{self.base_url}/v1/{self.environment}/employeeabsence/'
        update_timestamp = round(time.time()*1000)
        if start_date:
            params = {'startDate' : start_date}

        return self._paginated_request_with_page(
                url=request_url,
                params = params,
                fetch_func = lambda x : {
                        'id' : x.get('id'),
                        'employee_id' : x.get('employeeEnvironmentSuspended',{}).get('id') if isinstance(x.get('employeeEnvironmentSuspended',{}),dict) else None,
                        'start_date' : x.get('absenceStartDate'),
                        'end_date' : x.get('absenceEndDate'),
                        'absence_reason' : x.get('reasonNote'),
                        'absence_note' : x.get('absenceNote'),
                        'updated_at_millis' : update_timestamp
                        }
                    )
    
    def get_all_regions(self) -> List[Dict[str,Any]]:
        """
        Get a list of all regions defined on the specific environment.

        Returns:
        List[T]: A list of dictionaries representing regions.
        """
        request_url = f'{self.base_url}/v3/environments/{self.environment}/regionals/'

        return self._paginated_request_with_page(
                url=request_url,
                fetch_func = lambda x : {

                        'id' : x.get('id'),
                        'regional_name' : x.get('name'),
                        'macroregional_id' : x.get('macroregional',{}).get('id') if isinstance(x.get('macroregional'),dict) else None
                }
            )
    

    def get_all_macroregions(self) -> List[Dict[str,Any]]:
        """
        Get a list of all macroregions defined on the specific environment.

        Returns:
        List[T]: A list of dicionaries representing macroregions.
        """

        request_url = f'{self.base_url}/v1/{self.environment}/macroregion/find'

        return self._paginated_request_with_page(
            url=request_url
        )
    



