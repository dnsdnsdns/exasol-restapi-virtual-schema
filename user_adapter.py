import json
import logging.handlers
from plain_text_tcp_handler import PlainTextTcpHandler

class AdapterCallHandler:
    API_URL = 'https://randomuser.me/api/'

    def __init__(self, request):
        self.request_json_object: dict = json.loads(request)

        self.api_key: str = self.request_json_object['schemaMetadataInfo']['properties']['API_KEY']
        self.log_listener: str = self.request_json_object['schemaMetadataInfo']['properties']['LOG_LISTENER']
        self.log_listener_port = int(self.request_json_object['schemaMetadataInfo']['properties']['LOG_LISTENER_PORT'])
        self.log_level: str = self.request_json_object['schemaMetadataInfo']['properties']['LOG_LEVEL']

        self.logger = PlainTextTcpHandler.initialize_logger(self.log_listener, self.log_listener_port, self.log_level)

    def controll_request_processing(self) -> str:
        """Takes the parsed JSON request and decides based on the request type how to handle the request.
        :returns a JSON string that will be interpreted by the database."""
        request_type: str = self.request_json_object["type"]
        if request_type == "createVirtualSchema":
            return self.__handle_create_virtual_schema()
        elif request_type == "dropVirtualSchema":
            return json.dumps({"type": "dropVirtualSchema"})
        elif request_type == "refresh":
            return json.dumps({"type": "refresh"})
        elif request_type == "setProperties":
            return json.dumps({"type": "setProperties"})
        elif request_type == "getCapabilities":
            return json.dumps({"type": "getCapabilities",
                               "capabilities": ["FILTER_EXPRESSIONS", "LIMIT", "LITERAL_STRING", "LITERAL_DOUBLE",
                                                "LITERAL_EXACTNUMERIC", "FN_PRED_OR", "FN_PRED_AND",
                                                "FN_PRED_EQUAL", "FN_PRED_IN_CONSTLIST"]})
        elif request_type == "pushdown":
            return self.__handle_pushdown()
        else:
            raise ValueError('F-VS-OWFS-1 Unsupported adapter callback')

    def __handle_create_virtual_schema(self) -> str:
        result = {"type": "createVirtualSchema",
                  "schemaMetadata": {"tables": []}
                  }

        user: dict = self.__get_user_table_json()

        result["schemaMetadata"]["tables"].append(user)
        return json.dumps(result)

    def __get_user_table_json(self) -> dict:
        return {"name": "USER_TABLE",
                "columns":
                    [
                        {"name": "GENDER",
                         "dataType": {"type": "VARCHAR", "size": 20}},
                        {"name": "NAME_TITLE",
                         "dataType": {"type": "VARCHAR", "size": 20}},
                        {"name": "NAME_FIRST",
                         "dataType": {"type": "VARCHAR", "size": 20}},
                        {"name": "NAME_LAST",
                         "dataType": {"type": "VARCHAR", "size": 20}},
                        {"name": "LOCATION_STREET_NUMBER",
                         "dataType": {"type": "DECIMAL", "precision": 9, "scale": 0}},
                        {"name": "LOCATION_STREET_NAME",
                         "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "LOCATION_CITY",
                         "dataType": {"type": "VARCHAR", "size": 100}},
                        {"name": "LOCATION_STATE",
                         "dataType": {"type": "VARCHAR", "size": 100}},
                        {"name": "LOCATION_COUNTRY",
                         "dataType": {"type": "VARCHAR", "size": 50}},
                        {"name": "LOCATION_POSTCODE",
                         "dataType": {"type": "VARCHAR", "size": 100}},
                        {"name": "LOCATION_COORDINATES_LATITUDE",
                         "dataType": {"type": "VARCHAR", "size": 100}},
                        {"name": "LOCATION_COORDINATES_LONGITUDE",
                         "dataType": {"type": "VARCHAR", "size": 100}},
                        {"name": "TIMEZONE_OFFSET",
                         "dataType": {"type": "VARCHAR", "size": 20}},
                        {"name": "TIMEZONE_DESCRIPTION",
                         "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "EMAIL",
                         "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "LOGIN_UUID",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "LOGIN_USERNAME",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "LOGIN_PASSWORD",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "LOGIN_SALT",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "LOGIN_MD5",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "LOGIN_SHA1",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "LOGIN_SHA256",
                          "dataType": {"type": "VARCHAR", "size": 200}},                          
                        {"name": "DOB_DATE",
                         "dataType": {"type": "TIMESTAMP"}},
                        {"name": "DOB_AGE",
                         "dataType": {"type": "DECIMAL", "precision": 9, "scale": 0}},
                        {"name": "REGISTERED_DATE",
                         "dataType": {"type": "TIMESTAMP"}},
                        {"name": "REGISTERED_AGE",
                         "dataType": {"type": "DECIMAL", "precision": 9, "scale": 0}},
                        {"name": "PHONE",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "CELL",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "ID_NAME",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "ID_VALUE",
                          "dataType": {"type": "VARCHAR", "size": 200}},
                        {"name": "NAT",
                         "dataType": {"type": "VARCHAR", "size": 20}}
                    ]}

    def __handle_pushdown(self) -> str:
        self.logger.info('>>>>PUSHDOWN<<<<')
        self.logger.info(f'{json.dumps(self.request_json_object)}\n\n\n')

        sql: str = self.__build_sql()
        result: dict = {
            "type": "pushdown",
            "sql": sql
        }

        self.logger.info('>>>>ADAPTER SQL<<<<<')
        self.logger.info(f'{json.dumps(sql)}\n\n\n>')
        return json.dumps(result)

    def __build_sql(self):
        api_method: str = self.__parse_api_method_from_name(self.request_json_object['pushdownRequest']['from']['name'])
        filters = self.parse_filters(self.request_json_object['pushdownRequest']['filter'])

        log_ip: str = self.logger.handlers[0].host
        log_port: int = self.logger.handlers[0].port
        log_level: int = self.logger.level

        self.logger.info(f'\n\n\nAPI FILTERS {filters}')
        if api_method == 'user_table':
            return self.__generate_user_sql(api_method, json.dumps(filters), log_ip, log_port, log_level)

    def __parse_api_method_from_name(self, name) -> str:
        if name == 'USER_TABLE':
            return 'user_table'

    def parse_filters(self, filters):
        buffer = []

        self.logger.info('>>>>>FILTER<<<<<')
        self.logger.info(f'{filters}')

        # -- If true filter is 'IN_CONSTLIST'
        if filters.get('arguments'):
            for argument in filters.get('arguments'):
                e: dict = {'left': {'name': filters.get('expression').get('name')},
                           'right': {'value': argument.get('value')}, 'type': 'predicate_equal'}
                buffer.append(self.parse_filters(e))
        # -- Is filters a single filter or a list of filters?
        elif filters.get('expressions'):
            for f in filters.get('expressions'):
                buffer.append(self.parse_filters(f))
        # -- Leaf element has to be of type 'predicate_equal' because this is the only predicate on leaf level that is supported
        else:
            try:
                return self.__handle_predicate_equal(filters)
            except (ValueError, KeyError) as err:
                self.logger.warning(err.message)
                return None
        return buffer

    def __handle_predicate_equal(self, filter_json) -> str:
        """Check if expressions are reversed"""

        if filter_json.get('right').get('value'):
            filter_value: str = filter_json['right']['value']
            filter_name: str = filter_json['left']['name']
        else:
            filter_value: str = filter_json['left']['value']
            filter_name: str = filter_json['right']['name']

        self.logger.info(f'Filter name: {filter_name} || Filter value: {filter_value}')

        api_parameter_key_mapping: dict = {'GENDER': 'gender=', 'NAT': 'nat='}

        if filter_name in ('GENDER', 'NAT'):
            try:
                float(filter_value)
                raise TypeError()
            except ValueError:
                return f"{api_parameter_key_mapping[filter_name]}{filter_value}"
            except TypeError as e:
                e.message = f'E-VS-OWFS-2 {filter_name} column filter does not accept numbers. Found <{filter_value}>.'
                raise
        else:
            raise KeyError(
                f'E-VS-OWFS-1 Filtering not supported on column {filter_name} in PREDICATE_EQUAL expression.')

    def __generate_user_sql(self, api_method, filters, log_ip, log_port, log_level) -> str:
        sql: str = f'SELECT restapi_vs_scripts.api_handler(\'{self.API_URL}\', \
                                                        \'{api_method}\', \
                                                        \'{filters}\', \
                                                        \'{self.api_key}\', \
                                                        \'{log_ip}\', \
                                                        \'{log_port}\', \
                                                        \'{log_level}\') \
                                                        EMITS (gender VARCHAR(20), \
                                                                name_title VARCHAR(20), \
                                                                name_first VARCHAR(20), \
                                                                name_last VARCHAR(20), \
                                                                location_street_number DECIMAL(9,0), \
                                                                location_street_name VARCHAR(200), \
                                                                location_city VARCHAR(100), \
                                                                location_state VARCHAR(100), \
                                                                location_country VARCHAR(50), \
                                                                location_postcode VARCHAR(100), \
                                                                location_coordinates_latitude VARCHAR(100), \
                                                                location_coordinates_longitude VARCHAR(100), \
                                                                timezone_offset VARCHAR(20), \
                                                                timezone_description VARCHAR(200), \
                                                                email VARCHAR(200), \
                                                                login_uuid VARCHAR(200), \
                                                                login_username VARCHAR(200), \
                                                                login_password VARCHAR(200), \
                                                                login_salt VARCHAR(200), \
                                                                login_md5 VARCHAR(200), \
                                                                login_sha1 VARCHAR(200), \
                                                                login_sha256 VARCHAR(200), \
                                                                dob_date TIMESTAMP, \
                                                                dob_age DECIMAL(9,0), \
                                                                registered_date TIMESTAMP, \
                                                                registered_age DECIMAL(9,0), \
                                                                phone VARCHAR(200), \
                                                                cell VARCHAR(200), \
                                                                id_name VARCHAR(200), \
                                                                id_value VARCHAR(200), \
                                                                nat VARCHAR(20))'
        return sql
