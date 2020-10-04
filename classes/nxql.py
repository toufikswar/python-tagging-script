#!/usr/bin/python
# Copyright (C) 2017 Nexthink SA, Switzerland

# Library import
import concurrent.futures
import http.client
import logging
from multiprocessing.pool import ThreadPool
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
from urllib.parse import urlparse
import base64
import ssl
import socket
import os
import requests
import sys




class Nxql(object):
    """Summary of class Nxql.

    This class represent an nxql object and a list of functions that can be
    applied to it.
    An nxql object will represent a query with some attributes like the
    different parameters that are essentials to correctly run the query.

    Class Attributes:
        username: the username of the user that will run the query
        password: password of the user that will run the query

    Object Attributes:
        query: the nxql query itself (as created in the nxql editor)
        engine: the list of Engine on which the nxql query will run
        r_format: The return format of the query (xml, csv, json)
        hr: human readable variable (true or false)
        logger: this is to log INFO/WARNING/ERROR/DEBUG messages
        urls: list of prepared url that will be fetched by urllib2 library

    """

    # Same for all NXQL instance (we admit all Engines are connected to same Portal)
    username = ''
    password = ''

    @classmethod
    def verify_credentials(cls):
        """Function to verify credentials

        Function that will validate that the username and password aren't
        empty values.

        """

        if not cls.username:
            raise ValueError("Username is empty.")
        if not cls.password:
            raise ValueError("Password is empty.")
        return True

    def __init__(self, websession, logger):
        """Construct the nxql object

        Construct an nxql object with
        - default 'xml' format
        - hr set to 'false'
        - default query 'select id from device'
        - empty list of Engine
        - empty list of urls.
        - opener set to None (to define with define_opener function)


        """

        self._query = '(select id (from device))'
        self.r_format = 'xml'
        self.hr = 'false'
        self.engine = []
        self.urls = []
        self.opener = None
        self.logger = logger
        self._websession = websession

    @property
    def query(self):
        return self._query

    # Assign a new query to the object if query is not empty
    @query.setter
    def query(self, q):
        if not q:
            self.logger.warn("Query cannot be empty")
        else:
            self._query = q

    @property
    def engine(self):
        return self._engine

    # Assign a new engine list to the object if engine is a list
    @engine.setter
    def engine(self, e):
        if not isinstance(e, list):
            self.logger.warn("Need to provide a list element")
        else:
            self._engine = e

    @property
    def r_format(self):
        return self._r_format

    # Assign a new format to the object if format is supported
    @r_format.setter
    def r_format(self, f):
        if f not in ['xml', 'csv', 'json']:
            self.logger.warn("Format should be xml, csv or json. Keeping default 'xml' format")
        else:
            self._r_format = f

    @property
    def hr(self):
        return self._hr

    # Assign a new hr value if it's either true or false
    @hr.setter
    def hr(self, h):
        if h not in ['true', 'false']:
            self.logger.warn("Human readable variable can be either true or false. Keeping default 'false' value")
        else:
            self._hr = h

    def add_engine(self, e):
        """Append Engine to the list of Engine

        Function to add new Engine to the Engine list. It appends an Engine only
        if the value is not empty.

        Args:
            e: FQDN or IP of an Engine Appliance

        """

        if not e:
            self.logger.warn("Cannot add empty Engine")
        else:
            self.engine.append(e)

    def clean_category_query(self, category_name, object_type):
        """Modify the query to clean a specific category

        Update the value query of the object with a query to clean a category
        on a specific object. The query is not executed here, only prepared.

        Args:
            category_name: Name of the category
            object_type: Type of the object on which we have the category

        """

        template = "(update (set #\"{0}\" nil) (from {1}))"
        query = template.format(category_name, object_type)
        self.logger.debug("Clean Query: "+ query)
        self._query = query
        return query

    def start_update_query(self, keyword, category_name, object_type):
        """Start the creation of an update query for a specific tag

        Prepare the format of an update query for a keyword in a category on an
        object.

        Args:
            keyword: one of the keyword of a category
            category_name: Name of the category
            object_type: Type of the object on which we have the category

        """

        template = '(update (set #"{0}" (enum "{1}")) (from {2}'
        query = template.format(category_name, keyword, object_type)
        self.logger.debug("Update Query: " + query)
        self._query = query
        return query

    def add_condition(self, condition_field, value, object_type, base_query=None):
        """Add a condition to an update query

        Add a new condition to an update query that was started with
        start_update.

        Args:
            condition_field: field on which we apply the condition (id, hash, name)
            value: value of the field
            object_type: Type of the object

        """

        if condition_field not in ['id', 'hash', 'name']:
            self.logger.warn("Invalid condition field. Condition not added.")
        else:
            if condition_field == 'id':
                template = '(where {0} (eq id (identifier {1})))'
            if condition_field == 'hash':
                template = '(where {0} (eq hash (md5 {1})))'
            if condition_field == 'name' and object_type == 'binary':
                template = '(where {0} (eq executable_name (pattern {1})))'
            if condition_field == 'name' and object_type != 'binary':
                template = '(where {0} (eq name (pattern "{1}")))'
            query = template.format(object_type, value)
            if base_query:
                new_query = base_query + query
                return new_query
            else:
                self._query += query
                return self._query

    def finish_update_query(self, base_query=None):
        """Finish the construction of the update query

        Add the missing parenthesis to the update query.

        """
        if base_query:
            new_query = base_query + "))"
            self.logger.debug("Full Update Query: " + new_query)
            return new_query
        else:
            self._query += "))"
            self.logger.debug("Full Update Query: " + self.query)
            return self._query

    def build_validating_opener(self, ca_certs):
        class VerifiedHTTPSConnection(http.client.HTTPSConnection):
            def connect(self):
                # overrides the version in httplib so that we do certificate verification
                sock = socket.create_connection((self.host, self.port), self.timeout)
                if self._tunnel_host:
                    self.sock = sock
                    self._tunnel()

                # wrap the socket using verification with the root ca provided
                self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, cert_reqs=ssl.CERT_REQUIRED,
                                            ca_certs=ca_certs)

        # wraps https connections with ssl certificate verification
        class VerifiedHTTPSHandler(urllib.request.HTTPSHandler):
            def __init__(self, connection_class=VerifiedHTTPSConnection):
                self.specialized_conn_class = connection_class
                urllib.request.HTTPSHandler.__init__(self)

            def https_open(self, req):
                return self.do_open(self.specialized_conn_class, req)

        https_handler = VerifiedHTTPSHandler()
        url_opener = urllib.request.build_opener(https_handler)

        return url_opener

    def prepare_url(self):
        """Prepare the full urls to run the query later on

        Reset the urls list in case it isn't empty and make sure there is some
        Engine in the list. Then encode the different parameters to the url and
        add them the the urls list.

        """

        self.logger.debug("Verifying credentials for URL preparation ...")
        if Nxql.verify_credentials():
            self.logger.debug("Credentials verified successfully")
            if self.urls:
                self.urls = []
                self.logger.debug("Emptied URL list")
            if not self.engine:
                raise ValueError("Engine list is empty.")

            self.logger.debug("Preparing URL parameters ...")
            parameters = urllib.parse.urlencode({'query': self.query, 'format': self.r_format, 'hr': self.hr}, True)
            self.logger.debug("Preparing Authentication Header ...")
            username_password_string = '%s:%s' % (Nxql.username, Nxql.password)
            username_password_bytes = username_password_string.encode("utf-8")
            auth_string = base64.b64encode(username_password_bytes)

            for engine in self.engine:
                self.logger.debug("Creating URL for Engine: " + engine)
                url = 'https://' + engine + ':1671/2/query'
                request = urllib.request.Request(url, parameters)
                request.add_header("Authorization", "Basic %s" % auth_string)
                self.logger.debug("Request Header: " + str(request.headers))
                self.urls.append(request)

    def prepare_url_2(self):
        """Another version of prepare_url() : Prepare the full urls to run the query later on

        - Verify credentials
        - Prepare a list of URLs for each engine to be queried
        - if no engines found, exits the program

        """
        try :
            """ self.logger.debug("Verifying credentials for URL preparation ...")
            if Nxql.verify_credentials():
                self.logger.debug("Credentials verified successfully")
                if self.urls:
                    self.urls = []
                    self.logger.debug("Emptied URL list")
                if not self.engine:
                    raise ValueError("Engine list is empty.") """

            # Empty the URLs list first
            self.urls = []

            # Populate the  URL list based on each available Engine
            for engine in self.engine:
                # We create a URL with each engine's hostname
                url = 'https://' + engine + ':1671/2/query'
                self.logger.debug("Built URL : " + url)
                # We add all engines' URL in a list
                self.urls.append(url)
        
        except ValueError as value_ex:
            self.logger.error("No Engines available - cannot proceed, closing program")
            sys.exit(1)

    def fetch_url_2(self, url):
        """Another version of fetch_url() that uses the Requests library

        Function that runs a get request adding:
        - the query
        - specifing the format
        - authentication
        - verify : do not use SSL

        Args:
            an url

        Return:
            an http response object

        """
        try:
            session = self._websession.create_session()
            response = session.get(url, params={'query': self.query, 'format': self.r_format, 'hr': self.hr},
                                stream=False, verify=False)
            
            response.raise_for_status()

        except requests.exceptions.ConnectionError as err:
            self.logger.error(f"An error has occured : " + str(err))
            raise SystemExit(err)
           
        else:
            return response

    def run_request_2(self):
        """Function to run requests in parallel

        Function that uses a ThreadPool to run several get requests in parallel on serveral engines.
        It will store the result made of (hostname, response text, response code) in a list

        Return:
            list of (hostname, response text, response code)

        """
        result_list = []
        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(self.fetch_url_2, url): url for url in self.urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    response = future.result()
                except Exception as exc:
                    self.logger.error('{} generated an exception: {}'.format(url, exc))
                else:
                    self.logger.debug('{} returned {}'.format(url, response))
                    result_list.append((urlparse(response.url).hostname,response.text, response.status_code))

        return result_list

    def fetch_url(self, url):
        """Function to actually run a query for a specific url

        Function that uses urlopen to launch a query to the given url.

        Args:
            url: a fully prepared url (header + url + parameters)

        Return:
            url: the url
            response: the response object
            e: the error in case there is one or None otherwise

        """

        try:
            # addr = urlparse(url.get_full_url()).hostname
            # port = '1671'
            # cert = ssl.get_server_certificate((addr, port), ssl_version=ssl.PROTOCOL_SSLv23)
            # template = 'Certificate for {0} :\n{1}'
            # message = template.format(addr, cert)
            # self.logger.debug(message)

            response = self.opener.open(url)
            return url, response, None
        except Exception as e:
            return url, None, e

    def run_request(self):
        """Function for multithreading the fetch_url

        Function that will associate thread to fetch_url for each Url (20 in
        parallel maximum).

        Return:
            results: tuple composed of (url, response, error)

        """

        pool = ThreadPool(10)
        results = pool.imap_unordered(self.fetch_url, self.urls)
        pool.close()
        pool.join()
        result_list = []
        for url, response, error in results:
            result_list.append((urlparse(url.get_full_url()).hostname, response, error))
            if error is None:
                template = "Query on {0} executed in {1}. Return code: {2}"
                message = template.format(urlparse(response.geturl()).hostname,
                                          response.info().getheader('NX_EXEC_TIME'), str(response.getcode()))
                self.logger.info(message)

            elif type(error).__name__ == 'HTTPError':
                template = "Failed to fetch {0}. Error Code: {1}. Details: {2}"
                message = template.format(urlparse(url.get_full_url()).hostname, error.code, error.reason)
                self.logger.error(message)

            elif type(error).__name__ == 'URLError':
                template = "Failed to fetch {0}. Error: {1}. Details: {2}"
                message = template.format(urlparse(url.get_full_url()).hostname, type(error).__name__, error.reason)
                self.logger.error(message)
                
            else:
                template = "Failed to fetch {0}. Error: {1}. Details: {2}"
                message = template.format(urlparse(url.get_full_url()).hostname, type(error).__name__, error.args)
                self.logger.error(message)

        return result_list
    
    def prepare_for_engine_object_updates(self, id_query, id_column, tags):
        """Prepare the full urls to run the query later on

        - Preserve the condition building parameters
        - Prepare a list of URLs for each engine to be queried
        - if no engines found, exits the program

        """
        try :

            # Put the criteria here to be used by the threaded calls
            self._id_query = id_query
            self._id_column = id_column
            self._tags = tags

            # Empty the URLs list first
            self.urls = []

            # Populate the  URL list based on each available Engine
            for engine in self.engine:
                # We create a URL with each engine's hostname
                url = 'https://' + engine + ':1671/2/query'
                self.logger.debug("Built URL : " + url)
                # We add all engines' URL in a list
                self.urls.append(url)
        
        except ValueError as value_ex:
            self.logger.error("No Engines available - cannot proceed, closing program")
            sys.exit(1)

    def process_engine_object(self, url):
        """Get the related engine objects
        Uses the request library to Another version of fetch_url() that uses the Requests library

        Function that runs a get request adding:
        - the query
        - specifing the format
        - authentication
        - verify : do not use SSL

        Args:
            an url

        Return:
            an http response object

        """
        num_updates = 0
        num_failures = 0
        updated_ids = []
        try:
            # First get the list of object identifiers from the current Engine
            template = 'Requesting list of objects from Engine with URL "{}".'
            message = template.format(url)
            self.logger.debug(message)
            get_session = self._websession.create_session()
            response = get_session.get(url, params={'query': self._id_query, 'format': 'json', 'hr': self.hr},
                                stream=False, verify=False)
            response.raise_for_status()
            # Continue by iterating through the response text if we have results
            if response.status_code == 200 and response:
                hostname = urlparse(response.url).hostname
                json_results = response.json()
                # Construct list of id_column values from list of objects in this engine
                id_list = [obj[self._id_column].upper() for obj in json_results]
                template = 'Engine "{}" returned {} ids' # .\n{}'
                message = template.format(hostname, len(id_list)) #, id_list)
                self.logger.debug(message)

                # For each tag row, see if the id column exists in this engine
                for tag in self._tags:
                    if tag["Object ID"].upper() in id_list:
                        # Found a match, so updated it.
                        self.logger.debug('Found "{}" in Engine "{}".  About to update.'.format(tag["Object ID"], hostname))
                        upd_query = self.start_update_query(tag["Keyword"], tag["Category"], tag["Object Type"])
                        upd_query = self.add_condition(self._id_column, tag["Object ID"], tag["Object Type"], base_query=upd_query)
                        upd_query = self.finish_update_query(base_query=upd_query)
                        # Attempt the update
                        update_session = self._websession.create_session()
                        update_response = update_session.get(url, params={'query': upd_query},
                                            stream=False, verify=False)
                        # Process the result
                        if update_response.status_code != 200:
                            self.logger.error('process_engine_object({}): Unexpected response ({}) from update query: {}'.format(
                                url, update_response.status_code, upd_query))
                            num_failures += 1
                        else:
                            self.logger.debug('Successfully updated "{}" in Engine "{}"'.format(tag["Object ID"], hostname))
                            num_updates += 1
                            updated_ids.append(tag["Object ID"])

            elif response.status_code != 200:
                self.logger.error('process_engine_object({}): Unexpected response from id query: {}'.format(url, response.status_code))
            else:
                self.logger.error('process_engine_object({}): No results returned from id query.'.format(url))

        except requests.exceptions.ConnectionError as err:
            self.logger.error('process_engine_object({}): A ConnectionError exception occurred: {!r}'.format(url, err))
            raise SystemExit(err)
        else:
            return {"url": url, "num_updates": num_updates, "num_failures": num_failures, "updated_ids": updated_ids}

    def process_engine_objects(self):
        """Function to run requests in parallel

        Function that uses a ThreadPool to run several get requests in parallel on serveral engines.
        It will store the result made of (hostname, response text, response code) in a list

        Return:
            list of (hostname, response text, response code)

        """
        result_list = []
        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(self.process_engine_object, url): url for url in self.urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    response = future.result()
                except Exception as exc:
                    self.logger.error('{} generated an exception: {}'.format(url, exc))
                else:
                    self.logger.debug('{} returned {}'.format(url, response))
                    result_list.append(response)

        return result_list

