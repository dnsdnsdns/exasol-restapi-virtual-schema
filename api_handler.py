import logging.handlers
import json
import datetime
import re
import requests
from plain_text_tcp_handler import PlainTextTcpHandler

class ApiHandler:
    def __init__(self, ctx):
        self.ctx = ctx
        self.api_host: str = ctx.api_host
        self.api_method: str = ctx.api_method
        self.api_key: str = ctx.api_key

        try:
            self.parameter_expressions = json.loads(ctx.api_parameters)
        except json.decoder.JSONDecodeError:
            self.parameter_expressions = ctx.api_parameters

        self.logger = PlainTextTcpHandler.initialize_logger(ctx.logger_ip, int(ctx.logger_port), int(ctx.logger_level))

    def api_calls(self) -> None:
        """Takes the API parameter expression(s) the UDF was called with and unpacks them if they are a list. After
        unpacking the values the class proceeds with calling the API with the respective parameters and emitting the
        results."""
        self.logger.info('\n', self.parameter_expressions, '\n')
        if type(self.parameter_expressions) == list:
            self.__unpack_parameter_expression_list()
        else:
            self.__request_api_and_emit(self.parameter_expressions)

    def __unpack_parameter_expression_list(self) -> None:
        param_str = ''
        for expression in self.parameter_expressions:
            # -- Handle Null values sent from the adapter
            # if (type(expression) == list and any(not element for element in expression)) or not expression:
            #     continue
            # elif type(expression) == list and (all(element.startswith('nat') for element in expression)):
            #     param_str += self.__unpack_const_list_expression(expression)
            # elif type(expression) == list and (all(element.startswith('gender') for element in expression)):
            #     param_str += self.__unpack_const_list_expression(expression)
            
            param_str += expression + '&'
        self.__request_api_and_emit(param_str)

    # def __unpack_const_list_expression(self, expression: list) -> None:
    #     for literal in expression:
    #         self.__request_api_and_emit(literal)

    def __request_api_and_emit(self, param: str) -> None:
        self.logger.info(f'REQUESTNG API WITH: {param}')

        try:
            response: requests.Response = self.__api_request(param)
            json_response_object: dict = json.loads(response.text)
        except requests.Timeout as e:
            e.message = f'E-VW-OWFS-8 API request with parameter <{param}> timed out.'

        if response and response.status_code == 200:
            if self.api_method == 'user_table':
                self.__emit_user(json_response_object)
        else:
            self.logger.error('')

    def __api_request(self, param: str) -> requests.Response:
        request: str = f"{self.api_host}?{param}"
        self.logger.info(f'REQUEST STRING: {request}\n\n\n')
        return requests.get(request)

    def __emit_user(self, json_dict: dict) -> None:
        results_group = json_dict.get('results') if json_dict.get('results') else {}
        results_converted = self.__cast_datatypes(results_group)

        self.ctx.emit(results_converted[0].get('gender'),
                    results_converted[0].get('name').get('title'),
                    results_converted[0].get('name').get('first'),
                    results_converted[0].get('name').get('last'),
                    results_converted[0].get('location').get('street').get('number'),
                    results_converted[0].get('location').get('street').get('name'),
                    results_converted[0].get('location').get('city'),
                    results_converted[0].get('location').get('state'),
                    results_converted[0].get('location').get('country'),
                    results_converted[0].get('location').get('postcode'),
                    results_group[0].get('location').get('coordinates').get('latitude'),
                    results_group[0].get('location').get('coordinates').get('longitude'),
                    results_group[0].get('location').get('timezone').get('offset'),
                    results_group[0].get('location').get('timezone').get('description'),
                    results_group[0].get('email'),
                    results_group[0].get('login').get('uuid'),
                    results_group[0].get('login').get('username'),
                    results_group[0].get('login').get('password'),
                    results_group[0].get('login').get('salt'),
                    results_group[0].get('login').get('md5'),
                    results_group[0].get('login').get('sha1'),
                    results_group[0].get('login').get('sha256'),
                    results_group[0].get('dob').get('date'),
                    results_group[0].get('dob').get('age'),
                    results_group[0].get('registered').get('date'),
                    results_group[0].get('registered').get('age'),
                    results_group[0].get('phone'),
                    results_group[0].get('cell'),
                    results_group[0].get('id').get('name'),
                    results_group[0].get('id').get('value'),
                    results_group[0].get('nat'))

    def __cast_datatypes(self, results_json: dict) -> dict:
        # postcode in api response can be int or string, if int cast to string
        if type(results_json[0].get('location').get('postcode')) == int:
            results_json[0]['location']['postcode'] = self.__cast_postcode(results_json[0].get('location').get('postcode'))
        # dob_date and registred_date in api response are of type string, cast to timestamp/datetime
        results_json[0]['dob']['date'] = self.__cast_to_timestamp(results_json[0].get('dob').get('date'))
        results_json[0]['registered']['date'] = self.__cast_to_timestamp(results_json[0].get('registered').get('date'))
        return results_json

    def __cast_postcode(self, postcode: int) -> str:
        return str(postcode)

    def __cast_to_timestamp(self, timestamp_str: str) -> datetime.datetime:
        return datetime.datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')



