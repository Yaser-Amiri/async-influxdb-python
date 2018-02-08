# -*- coding: utf-8 -*-
"""Python asynchronous client for InfluxDB."""

import random
import json
import asyncio
import aiohttp

from .line_protocol import make_lines, quote_ident, quote_literal
from .resultset import ResultSet
from .exceptions import InfluxDBClientError
from .exceptions import InfluxDBServerError


class InfluxDBClient(object):

    def __init__(self, host='localhost', port=8086, username='root',
                 password='root', database=None, timeout=None, retries=3):
        """Construct a new InfluxDBClient object."""
        self.__host = host
        self.__port = int(port)
        self._username = username
        self._password = password
        self._database = database
        self._timeout = timeout
        self._retries = retries
        self._scheme = "http"

        self.__baseurl = "{0}://{1}:{2}".format(
            self._scheme, self._host, self._port)

        self._headers = {
            'Content-Type': 'application/json',
            'Accept': 'text/plain'
        }

        self._session = aiohttp.ClientSession()

    @property
    def _baseurl(self):
        return self.__baseurl

    @property
    def _host(self):
        return self.__host

    @property
    def _port(self):
        return self.__port

    def switch_database(self, database):
        self._database = database

    def switch_user(self, username, password):
        self._username = username
        self._password = password

    async def request(self, url, method='GET', params=None, data=None,
                      expected_response_code=200, headers=None):

        req_url = "{0}/{1}".format(self._baseurl, url)

        if headers is None:
            headers = self._headers

        if params is None:
            params = {}

        if isinstance(data, (dict, list)):
            data = json.dumps(data)

        # Try to send the request more than once by default (see #103)
        retry = True
        _try = 0
        while retry:
            try:
                response = await self._session.request(
                    method=method,
                    url=req_url,
                    auth=aiohttp.BasicAuth(self._username, self._password),
                    params=params,
                    data=data,
                    headers=headers,
                    timeout=self._timeout
                )
                break
            except (aiohttp.client_exceptions.ClientConnectorError,
                    aiohttp.client_exceptions.ClientOSError,
                    aiohttp.client_exceptions.ServerTimeoutError):
                _try += 1
                if self._retries != 0:
                    retry = _try < self._retries
                if method == "POST":
                    await asyncio.sleep((2 ** _try) * random.random() / 100.0)
                if not retry:
                    if url == 'ping':
                        raise InfluxDBClientError("can't connect to server.")
                    raise
        # if there's not an error, there must have been a successful response

        if 500 <= response.status < 600:
            raise InfluxDBServerError(await response.text())
        elif response.status == expected_response_code:
            return response
        else:
            raise InfluxDBClientError(await response.text(), response.status)

    async def ping(self):
        try:
            response = await self.request(
                url="ping",
                method='GET',
                expected_response_code=204
            )
            response.close()
            return True, response.headers['X-Influxdb-Version']
        except InfluxDBClientError:
            return False, None

    async def write(self, data, params=None, expected_response_code=204,
                    protocol='json'):

        headers = self._headers
        headers['Content-Type'] = 'application/octet-stream'

        if params:
            precision = params.get('precision')
        else:
            precision = None

        if protocol == 'json':
            data = make_lines(data, precision).encode('utf-8')
        elif protocol == 'line':
            if isinstance(data, str):
                data = [data]
            data = ('\n'.join(data) + '\n').encode('utf-8')

        response = await self.request(
            url="write",
            method='POST',
            params=params,
            data=data,
            expected_response_code=expected_response_code,
            headers=headers
        )
        response.close()
        return True

    async def _write_points(self, points, time_precision, database,
                            retention_policy, tags, protocol='json'):
        if time_precision not in ['n', 'u', 'ms', 's', 'm', 'h', None]:
            raise ValueError(
                "Invalid time precision is given. "
                "(use 'n', 'u', 'ms', 's', 'm' or 'h')")

        if protocol == 'json':
            data = {
                'points': points
            }

            if tags is not None:
                data['tags'] = tags
        else:
            data = points

        params = {
            'db': database or self._database
        }

        if time_precision is not None:
            params['precision'] = time_precision

        if retention_policy is not None:
            params['rp'] = retention_policy

        else:
            await self.write(
                data=data,
                params=params,
                expected_response_code=204,
                protocol=protocol
            )

        return True

    async def write_points(self, points, time_precision=None, database=None,
                           retention_policy=None, tags=None, batch_size=None,
                           protocol='json'):
        if batch_size and batch_size > 0:
            for batch in self._batches(points, batch_size):
                await self._write_points(points=batch,
                                         time_precision=time_precision,
                                         database=database,
                                         retention_policy=retention_policy,
                                         tags=tags, protocol=protocol)
            return True

        return await self._write_points(points=points,
                                        time_precision=time_precision,
                                        database=database,
                                        retention_policy=retention_policy,
                                        tags=tags, protocol=protocol)

    @staticmethod
    async def _read_chunked_response(response, raise_errors=True):
        result_set = {}
        async for line in response.content:
            if isinstance(line, bytes):
                line = line.decode('utf-8')
            data = json.loads(line)
            for result in data.get('results', []):
                for _key in result:
                    if isinstance(result[_key], list):
                        result_set.setdefault(
                            _key, []).extend(result[_key])
        response.close()
        return ResultSet(result_set, raise_errors=raise_errors)

    @staticmethod
    def _batches(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    async def query(self, query, params=None, epoch=None,
                    expected_response_code=200, database=None,
                    raise_errors=True, chunked=False, chunk_size=0):

        if params is None:
            params = {}

        params['q'] = query
        params['db'] = database or self._database

        if epoch is not None:
            params['epoch'] = epoch

        if chunked:
            params['chunked'] = 'true'
            if chunk_size > 0:
                params['chunk_size'] = chunk_size

        response = await self.request(
            url="query",
            method='GET',
            params=params,
            data=None,
            expected_response_code=expected_response_code
        )

        if chunked:
            return await self._read_chunked_response(response)

        data = await response.json()
        response.close()
        results = [
            ResultSet(result, raise_errors=raise_errors)
            for result in data.get('results', [])
        ]

        if len(results) == 1:
            return results[0]

        return results

    async def get_list_database(self):
        """Get the list of databases in InfluxDB.

        :returns: all databases in InfluxDB
        :rtype: list of dictionaries

        :Example:

        ::

            >> dbs = client.get_list_database()
            >> dbs
            [{u'name': u'db1'}, {u'name': u'db2'}, {u'name': u'db3'}]
        """
        rs = await self.query("SHOW DATABASES")
        return list(rs.get_points())

    async def create_database(self, dbname):
        """Create a new database in InfluxDB.

        :param dbname: the name of the database to create
        :type dbname: str
        """
        await self.query("CREATE DATABASE {0}".format(quote_ident(dbname)))

    async def drop_database(self, dbname):
        """Drop a database from InfluxDB.

        :param dbname: the name of the database to drop
        :type dbname: str
        """
        await self.query("DROP DATABASE {0}".format(quote_ident(dbname)))

    async def get_list_measurements(self):
        rs = await self.query("SHOW MEASUREMENTS")
        return list(rs.get_points())

    async def drop_measurement(self, measurement):
        await self.query(
            "DROP MEASUREMENT {0}".format(quote_ident(measurement)))

    async def create_retention_policy(
            self, name, duration, replication, database=None, default=False):
        query_string = \
            "CREATE RETENTION POLICY {0} ON {1} " \
            "DURATION {2} REPLICATION {3}".format(
                quote_ident(name), quote_ident(database or self._database),
                duration, replication)

        if default is True:
            query_string += " DEFAULT"

        await self.query(query_string)

    async def alter_retention_policy(self, name, database=None, duration=None,
                                     replication=None, default=None):
        query_string = (
            "ALTER RETENTION POLICY {0} ON {1}"
        ).format(quote_ident(name), quote_ident(database or self._database))
        if duration:
            query_string += " DURATION {0}".format(duration)
        if replication:
            query_string += " REPLICATION {0}".format(replication)
        if default is True:
            query_string += " DEFAULT"

        await self.query(query_string)

    async def drop_retention_policy(self, name, database=None):
        query_string = (
            "DROP RETENTION POLICY {0} ON {1}"
        ).format(quote_ident(name), quote_ident(database or self._database))
        await self.query(query_string)

    async def get_list_retention_policies(self, database=None):

        if not (database or self._database):
            raise InfluxDBClientError(
                "get_list_retention_policies() requires a database as a "
                "parameter or the client to be using a database")

        rsp = await self.query(
            "SHOW RETENTION POLICIES ON {0}".format(
                quote_ident(database or self._database))
        )
        return list(rsp.get_points())

    async def get_list_users(self):
        rs = await self.query("SHOW USERS")
        return list(rs.get_points())

    async def create_user(self, username, password, admin=False):
        text = "CREATE USER {0} WITH PASSWORD {1}".format(
            quote_ident(username), quote_literal(password))
        if admin:
            text += ' WITH ALL PRIVILEGES'
        await self.query(text)

    async def drop_user(self, username):
        text = "DROP USER {0}".format(quote_ident(username))
        await self.query(text)

    async def set_user_password(self, username, password):
        text = "SET PASSWORD FOR {0} = {1}".format(
            quote_ident(username), quote_literal(password))
        await self.query(text)

    async def delete_series(self, database=None, measurement=None, tags=None):
        database = database or self._database
        query_str = 'DROP SERIES'
        if measurement:
            query_str += ' FROM {0}'.format(quote_ident(measurement))

        if tags:
            tag_eq_list = ["{0}={1}".format(quote_ident(k), quote_literal(v))
                           for k, v in tags.items()]
            query_str += ' WHERE ' + ' AND '.join(tag_eq_list)
        await self.query(query_str, database=database)

    async def grant_admin_privileges(self, username):
        text = "GRANT ALL PRIVILEGES TO {0}".format(quote_ident(username))
        await self.query(text)

    async def revoke_admin_privileges(self, username):
        text = "REVOKE ALL PRIVILEGES FROM {0}".format(quote_ident(username))
        await self.query(text)

    async def grant_privilege(self, privilege, database, username):
        text = "GRANT {0} ON {1} TO {2}".format(privilege,
                                                quote_ident(database),
                                                quote_ident(username))
        await self.query(text)

    async def revoke_privilege(self, privilege, database, username):
        text = "REVOKE {0} ON {1} FROM {2}".format(privilege,
                                                   quote_ident(database),
                                                   quote_ident(username))
        await self.query(text)

    async def get_list_privileges(self, username):
        text = "SHOW GRANTS FOR {0}".format(quote_ident(username))
        rs = await self.query(text)
        return list(rs.get_points())

    def send_packet(self, packet, protocol='json'):
        if protocol == 'json':
            data = make_lines(packet).encode('utf-8')
        elif protocol == 'line':
            data = ('\n'.join(packet) + '\n').encode('utf-8')
        self.udp_socket.sendto(data, (self._host, self._udp_port))

    def close(self):
        """Close http session."""
        if isinstance(self._session, aiohttp.ClientSession):
            self._session.close()
