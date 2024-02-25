"""
FRED API documentation: http://api.stlouisfed.org/docs/fred/

Core functionality for interacting with the FRED API.
"""

import os
import requests
from datetime import datetime, timedelta
from time import sleep
import xml.etree.ElementTree as ET

try:
    from itertools import ifilter as filter
except ImportError:
    pass

try:
    import simplejson as json
except ImportError:
    import json


class Fred(object):
    """An easy-to-use Python wrapper over the St. Louis FRED API."""

    FIRST_REQUEST_TIMESTAMP = None
    REQUEST_COUNT = 0

    def __init__(self, api_key='', xml_output=False):
        if 'FRED_API_KEY' in os.environ:
            self.api_key = os.environ['FRED_API_KEY']
        else:
            self.api_key = api_key
        self.xml = xml_output
        self.endpoint = 'https://api.stlouisfed.org/fred/'

    def _create_path(self, *args):
        """Create the URL path with the Fred endpoint and given arguments."""
        args = filter(None, args)
        path = self.endpoint + '/'.join(args)
        return path

    def get(self, *args, throttle, **kwargs):
        """Perform a GET request againt the Fred API endpoint."""
        location = args[0]
        params = self._get_keywords(location, kwargs)
        url = self._create_path(*args)

        # Store time of first request persistently for throttling
        if Fred.FIRST_REQUEST_TIMESTAMP is None:
            Fred.FIRST_REQUEST_TIMESTAMP = datetime.now()

        if Fred.REQUEST_COUNT > 1:
            if throttle:
                self.__throttle()

        error_flag = True
        request_retries = 0
        sleep_secs = 300
        max_retries = 3
        while error_flag and (request_retries <= max_retries):
            request = requests.get(url, params=params)
            content = request.content
            self._request = request
            Fred.REQUEST_COUNT += 1
            output = self._output(content)
            error_flag, error_code = self._handle_errors(output)

            if error_flag:
                request_retries += 1
                print(f"Request failed with code {error_code}. Retrying...")
                sleep(sleep_secs)

        return output

    def _handle_errors(self, output):
        error_flag = False
        error_code = None
        if self.xml:
            root = ET.fromstring(output)
            if root.tag == 'error':
                if (root.get('code') == '429') or (root.get('code') == '500'):
                    error_flag = True
                    error_code = root.get('code')
        else:
            if "error_code" in output:
                if (output['error_code'] == 429) or (output['error_code'] == 500):
                    error_flag = True
                    error_code = output['error_code']
        return error_flag, error_code

    def __throttle(self):
        rq_pace_limit = 90/60  # Rate limit (90 requests per minute) in rq/seconds
        time_delta_secs = (datetime.now() - Fred.FIRST_REQUEST_TIMESTAMP)
        request_pace = Fred.REQUEST_COUNT / time_delta_secs.total_seconds()

        if request_pace >= rq_pace_limit:
            print("Throttling...")
            # Calculate delay
            delay = timedelta(
                seconds=((Fred.REQUEST_COUNT + 1)/rq_pace_limit)
            ) - time_delta_secs
            delay = delay.total_seconds()  # convert to seconds
        else:
            delay = 0

        sleep(delay)

    def _get_keywords(self, location, keywords):
        """Format GET request's parameters from keywords."""
        if 'xml' in keywords:
            keywords.pop('xml')
            self.xml = True
        else:
            keywords['file_type'] = 'json'
        if 'id' in keywords:
            if location != 'series':
                location = location.rstrip('s')
            key = '%s_id' % location
            value = keywords.pop('id')
            keywords[key] = value
        if 'start' in keywords:
            time = keywords.pop('start')
            keywords['realtime_start'] = time
        if 'end' in keywords:
            time = keywords.pop('end')
            keywords['realtime_end'] = time
        if 'sort' in keywords:
            order = keywords.pop('sort')
            keywords['sort_order'] = order
        keywords['api_key'] = self.api_key
        return keywords

    def _output(self, content):
        """Return the output from a given GET request."""
        if self.xml:
            return content
        return json.loads(content)

    def category(self, path=None, throttle=False, **kwargs):
        """
        Get a specific category.

        >>> Fred().category(category_id=125)
        """
        return self.get('category', path, throttle=throttle, **kwargs)

    def release(self, path=None, throttle=False, **kwargs):
        """
        Get a release of economic data.

        >>> Fred().release('series', release_id=51)
        """
        return self.get('release', path, throttle=throttle, **kwargs)

    def releases(self, path=None, throttle=False, **kwargs):
        """
        Get all releases of economic data.

        >>> Fred().releases('dates', limit=10)
        """
        return self.get('releases', path, throttle=throttle, **kwargs)

    def series(self, path=None, throttle=False, **kwargs):
        """
        Get economic series of data.

        >>> Fred().series('search', search_text="money stock")
        """
        return self.get('series', path, throttle=throttle, **kwargs)

    def source(self, path=None, throttle=False, **kwargs):
        """
        Get a single source of economic data.

        >>> Fred().source(source_id=51)
        """
        return self.get('source', path, throttle=throttle, **kwargs)

    def sources(self, path=None, throttle=False, **kwargs):
        """
        Get all of FRED's sources of economic data.

        >>> Fred().sources()
        """
        return self.get('sources', path, throttle=throttle, **kwargs)

    def tags(self, path=None, throttle=False, **kwargs):
        """
        Get all FRED tags of economic data, or search for tags by name.

        >>> Fred().tags()
        """
        return self.get('tags', path, throttle=throttle, **kwargs)

