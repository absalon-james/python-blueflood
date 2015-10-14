import copy
import json
import logging
import pprint
import requests
import utils
from urlparse import urljoin


class Datapoint(dict):
    """
    Models a datapoint to be ingested into blueflood.

    """
    logger = logging.getLogger('blueflood.client.Datapoint')

    def __init__(self, name, value, collection_time=None,
                 ttl_seconds=None, unit=None):
        """
        Inits the datapoint
        @param name - String name of the metric
        @param value - Value of the metric
        @param collection_time - Time of collection
        @param ttl_seconds - Number of seconds for datapoint to live
        @param unit - String unit of the metric
        """
        self['metricValue'] = value
        self['metricName'] = name

        if collection_time is None:
            self.logger.debug("No collection time provided. Generating now")
            collection_time = utils.time_in_ms()
        self['collectionTime'] = collection_time

        # Set ttl
        if not ttl_seconds:
            ttl_seconds = 60 * 60 * 24 * 180
        ttl_seconds = max(ttl_seconds, 0)
        self['ttlInSeconds'] = ttl_seconds

        # Set units
        if unit:
            self['unit'] = unit
        self.logger.debug("Created datapoint:\n%s" % pprint.pformat(self))


class Blueflood(object):
    """
    Blueflood client.

    """
    logger = logging.getLogger('blueflood.client.Blueflood')

    base_headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    # Options available for selection on some GET requests
    selectables = [
        'average',
        'min',
        'max',
        'numPoints',
        'variance'
    ]

    def __init__(self, auth_url=None, apikey=None,
                 username=None, region='IAD',
                 ingest_url=None, read_url=None):
        """
        Inits the client.

        @param auth_url - String url for authentication
        @param apikey - String api key for authentication
        @param username - String username for authentication
        @param region - String region name

        """
        self.auth_url = auth_url
        self._read_url = read_url
        self._ingest_url = ingest_url
        self.apikey = apikey
        self.username = username
        self._token = None
        self.read_service = None
        self.ingest_service = None
        self.region = region
        self.get_token()

    def selects(self, **kwargs):
        """
        Generates the parameter for select queries on certain GET
        requests.

        @param **kwargs - Dictionary containing selectables.
        @return - String - comma separated list of selectables

        """
        return ','.join([s for s in self.selectables
                         if s in kwargs and kwargs.get(s)])

    def read_params(self, start, stop, points=None, resolution=None):
        """
        Sets up a dictionary with basic read parameters for certain
        GET requests.

        @param start - Float time in seconds
        @param stop - Float time in seconds
        @param points - Integer number of points
        @return - Dictionary

        """
        params = {
            'from': start,
            'to': stop,
        }
        if resolution:
            params['resolution'] = resolution
        elif points:
            params['points'] = points
        return params

    def invalidate_token(self):
        """
        Unsets the token.

        """
        self.logger.debug("Invalidating token")
        self._token = None

    def get_token(self):
        """
        Returns the current token if exists. Gets a new one otherwise.
        Also updates the service catalog.

        @return string

        """
        # Return token if we have it
        if self._token is not None:
            return self._token

        # We want json
        headers = copy.copy(self.base_headers)

        # Credential payload
        data = {
            'auth': {
                'RAX-KSKEY:apiKeyCredentials': {
                    'username': self.username,
                    'apiKey': self.apikey
                }
            }
        }
        resp = requests.post(
            urljoin(self.auth_url, 'tokens'),
            data=json.dumps(data),
            headers=headers
        )
        resp.raise_for_status()
        resp_json = resp.json()
        self._token = resp_json['access']['token']['id']
        self.update_catalog(resp_json['access']['serviceCatalog'])
        return self._token

    def update_catalog(self, service_catalog):
        """
        Sets the read and ingest service

        @param service_catalog - List of dicts from 'serviceCatalog'

        """
        ingest_name = 'cloudMetricsIngest'
        read_name = 'cloudMetrics'
        for s in service_catalog:
            if s['name'] == ingest_name:
                self.ingest_service = s
            elif s['name'] == read_name:
                self.read_service = s

    def read_url(self, region='IAD'):
        """
        Returns the url for reading metrics

        @param region - String region name
        @return String|None

        """
        if self._read_url is not None:
            return self._read_url
        if self.read_service is not None:
            return self.url_for_region(self.read_service, region)
        raise Exception("No read service found")

    def ingest_url(self, region="IAD"):
        """
        Returns the url for ingesting metrics

        @param region - String name of the region
        @return String|None

        """
        if self._ingest_url is not None:
            return self._ingest_url
        if self.ingest_service is not None:
            return self.url_for_region(self.ingest_service, region)
        raise Exception("No ingest service found")

    def url_for_region(self, service, region):
        """
        Returns a url from a service for a region

        @param service - Dictionary with endpoints, name, type
        @param region - String region name
        @return String

        """
        for e in service.get('endpoints', []):
            if region == e.get('region'):
                return e['publicURL']

    def request(self, url, method='get', data=None, headers=None, params=None):
        """
        Base request method.
        Get a token if it doesn't exist
        Make a request.
        Check for 401.
        Reauth one time if needed.
        Return object if one is provided.

        @param url - String url
        @param method - String should be one of (get, post, put, delete)
        @param data - Object to be jumped into json
        @param headers - Dictionary of headers
        @param params - Dictionary of query string parameters
        @return - JSON object

        """
        func = getattr(requests, method)
        _headers = copy.copy(self.base_headers)
        _headers.update({
            'X-Auth-Token': self.get_token()
        })

        kwargs = {'headers': _headers}

        if params is not None:
            kwargs['params'] = params
        if headers is not None:
            kwargs['headers'].update(headers)
        if data is not None:
            kwargs['data'] = json.dumps(data)

        self.logger.debug("Request method: %s" % method)
        self.logger.debug("Request url: %s" % url)
        self.logger.debug("Params:\n%s" % pprint.pformat(params))
        self.logger.debug("Headers:\n%s" % pprint.pformat(headers))
        self.logger.debug("Data: \n%s" % pprint.pformat(data))

        resp = func(url, **kwargs)
        if resp.status_code == 401:
            self.invalidate_token()
            kwargs['headers']['X-Auth-Token'] = self.get_token()
            resp = func(url, **kwargs)

        resp.raise_for_status()
        try:
            resp_json = resp.json()
            self.logger.debug("Response:\n%s" % pprint.pformat(resp_json))
            return resp_json
        except ValueError:
            pass

    def find_metrics(self, query='*'):
        """
        Returns a list of metric names.

        @param query - String metric name name query.
        @return - List

        """
        params = {'query': query}
        url = "%s/%s" % (self.read_url(), 'metrics/search')
        return self.request(url, method='get', params=params)

    def ingest(self, data):
        """
        Expects a list of dictionaries representing metric points.
        @param data - List of point dictionaries.

        """
        if not isinstance(data, list):
            data = [data]

        url = '%s/ingest' % self.ingest_url()
        return self.request(url, method='post', data=data)

    def get_metrics(self, start, stop, metrics,
                    points=None, resolution=None, **kwargs):
        """
        Returns multiple metrics
        @param start - Integer time in seconds
        @param stop - Integer time in seconds
        @param metrics - String list of metric names
        @param points - Integer number of points
        @param resolution - One of FULL|MIN5|MIN20|MIN60|MIN240|MIN1440
        @param kwargs - Remaining keyword arguments should be selectables.
        @return - Dictionary
        """
        url = '%s/views' % self.read_url()
        params = self.read_params(start, stop,
                                  points=points, resolution=resolution)
        selects = self.selects(**kwargs) if kwargs else None
        if selects:
            params['select'] = selects
        return self.request(url, method='post', params=params, data=metrics)
