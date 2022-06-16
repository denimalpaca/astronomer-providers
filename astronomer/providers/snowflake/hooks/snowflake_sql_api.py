from abc import ABC

import aiohttp
import requests
import uuid
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from astronomer.providers.snowflake.hooks.sql_api_generate_jwt import JWTGenerator


class SnowflakeSQLAPIHookAsync(SnowflakeHook, ABC):
    """
    A client to interact with Snowflake.

    This hook requires the snowflake_conn_id connection. The snowflake host, login,
    and, password field must be setup in the connection. Other inputs can be defined
    in the connection or hook instantiation. If used with the S3ToSnowflakeOperator
    add 'aws_access_key_id' and 'aws_secret_access_key' to extra field in the connection.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param account: snowflake account name
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param warehouse: name of snowflake warehouse
    :param database: name of snowflake database
    :param region: name of snowflake region
    :param role: name of snowflake role
    :param schema: name of snowflake schema
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    """

    def run(self, sql: str, statement_count: int, query_tag: str = ''):
        conn_config = self._get_conn_params()

        req_id = uuid.uuid4()
        url = "https://{0}.snowflakecomputing.com/api/v2/statements".format(conn_config['account'])
        params = {"requestId": req_id, "async": True, "pageSize": 10}
        headers = self.get_headers()
        data = {
            "statement": sql,
            "timeout": 60,
            "resultSetMetaData": {
                "format": "json"
             },
            "database": conn_config['database'],
            "schema": conn_config['schema'],
            "warehouse": conn_config['warehouse'],
            "role": conn_config['role'],
            "bindings": {},
            "parameters": {
                 "multi_statement_count": statement_count,
                 "query_tag": query_tag,
            }
        }
        response = requests.post(url, data=data, headers=headers, params=params)
        if response and response.status_code == 200:
            response = response.json()
            if statement_count > 1:
                self.query_ids = response['statementHandles']
            else:
                self.query_ids.append(response['statementHandle'])
            return self.query_ids

    def get_headers(self):
        conn_config = self._get_conn_params()

        # Get the JWT token from the connection details and the private key
        token = JWTGenerator(conn_config['account'], conn_config['user'], conn_config['private_key']).get_token()

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
            "Accept": "application/json",
            "User-Agent": "snowflakeSQLAPI/1.0",
            "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
        }
        return headers

    async def get_query_status(self, query_id):
        conn_config = self._get_conn_params()
        req_id = uuid.uuid4()
        url = "https://{0}.snowflakecomputing.com/api/v2/statements/{1}".format(conn_config['account'], query_id)
        params = {"requestId": req_id, "page": 2, "pageSize": 10}
        self.log.info("Retrieving status for query id %s", {query_id})
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params, headers=self.get_headers()) as response:
                resp = await response.json()
                return resp

