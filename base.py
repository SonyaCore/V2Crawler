"""
A scraping framework with support for data serialization and HTTP requests.
"""

__all__ = [
    'DeprecatedFeatureWarning',
    'Item', 
    'IntWithGranularity',
    'ScraperException',
    'EntityUnavailable', 
    'Scraper'
]

import abc
import copy
import dataclasses
import datetime
import enum
import functools
import json
import logging
import random
import time
import warnings

import requests
import requests.adapters
import urllib3.connection

_logger = logging.getLogger(__name__)


# ============================================================================
# Custom Exceptions and Warnings
# ============================================================================

class DeprecatedFeatureWarning(FutureWarning):
    """Warning for deprecated features."""
    pass


class ScraperException(Exception):
    """Base exception for scraper errors."""
    pass


class EntityUnavailable(ScraperException):
    """The target entity is unavailable, possibly suspended or non-existent."""
    pass


# ============================================================================
# Utility Classes
# ============================================================================

class _DeprecatedProperty:
    """Descriptor for deprecated properties that issues warnings when accessed."""
    
    def __init__(self, name, repl, replStr):
        self.name = name
        self.repl = repl
        self.replStr = replStr

    def __get__(self, obj, objType):
        if obj is None:  # Access through class rather than instance
            return self
        
        warnings.warn(
            f'{self.name} is deprecated, use {self.replStr} instead',
            DeprecatedFeatureWarning,
            stacklevel=2
        )
        return self.repl(obj)


class IntWithGranularity(int):
    """A number with an associated granularity.
    
    For example, IntWithGranularity(42000, 1000) represents a number on the 
    order of 42000 with two significant digits, counted with granularity of 1000.
    """

    def __new__(cls, value, granularity, *args, **kwargs):
        obj = super().__new__(cls, value, *args, **kwargs)
        obj.granularity = granularity
        return obj

    def __reduce__(self):
        return (IntWithGranularity, (int(self), self.granularity))


# ============================================================================
# JSON Serialization Utilities
# ============================================================================

def _json_serialise_datetime_enum(obj):
    """JSON serializer for datetime and enum objects."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, enum.Enum):
        return obj.value
    raise TypeError(f'Object of type {type(obj)} is not JSON serializable')


def _json_dataclass_to_dict(obj, forBuggyIntParser=False):
    """Convert dataclass objects to dictionaries for JSON serialization."""
    if isinstance(obj, _JSONDataclass) or dataclasses.is_dataclass(obj):
        out = {}
        out['_type'] = f'{type(obj).__module__}.{type(obj).__name__}'
        
        # Add dataclass fields
        for field in dataclasses.fields(obj):
            assert field.name != '_type'
            if field.name.startswith('_'):
                continue
            out[field.name] = _json_dataclass_to_dict(
                getattr(obj, field.name), 
                forBuggyIntParser=forBuggyIntParser
            )
        
        # Add properties
        for k in dir(obj):
            if isinstance(getattr(type(obj), k, None), (property, _DeprecatedProperty)):
                assert k != '_type'
                if k.startswith('_'):
                    continue
                out[k] = _json_dataclass_to_dict(
                    getattr(obj, k), 
                    forBuggyIntParser=forBuggyIntParser
                )
                
    elif isinstance(obj, (tuple, list)):
        return type(obj)(
            _json_dataclass_to_dict(x, forBuggyIntParser=forBuggyIntParser) 
            for x in obj
        )
    elif isinstance(obj, dict):
        out = {
            _json_dataclass_to_dict(k, forBuggyIntParser=forBuggyIntParser): 
            _json_dataclass_to_dict(v, forBuggyIntParser=forBuggyIntParser) 
            for k, v in obj.items()
        }
    elif isinstance(obj, set):
        return {
            _json_dataclass_to_dict(v, forBuggyIntParser=forBuggyIntParser) 
            for v in obj
        }
    else:
        return copy.deepcopy(obj)
    
    # Handle IntWithGranularity and buggy int parser output
    for key, value in list(out.items()):  # Copy items since we're modifying dict
        if isinstance(value, IntWithGranularity):
            out[key] = int(value)
            assert f'{key}.granularity' not in out, f'Granularity collision on {key}.granularity'
            out[f'{key}.granularity'] = value.granularity
        elif forBuggyIntParser and isinstance(value, int) and abs(value) > 2**53:
            assert f'{key}.str' not in out, f'Buggy int collision on {key}.str'
            out[f'{key}.str'] = str(value)
    
    return out


@dataclasses.dataclass
class _JSONDataclass:
    """Base class for dataclasses with JSON conversion capabilities."""

    def json(self, forBuggyIntParser=False):
        """Convert the object to a JSON string.

        Args:
            forBuggyIntParser: If True, emit JSON for parsers that can't 
                correctly decode integers exceeding IEEE 754 double precision 
                limits. Adds x.str fields for integers with magnitude > 2**53.
        """
        with warnings.catch_warnings():
            warnings.filterwarnings(action='ignore', category=DeprecatedFeatureWarning)
            out = _json_dataclass_to_dict(self, forBuggyIntParser=forBuggyIntParser)
        
        assert '_v2crawler' not in out, 'Metadata collision on _v2crawler'
        out['_v2crawler'] = "1.2.0"
        return json.dumps(out, default=_json_serialise_datetime_enum)


# ============================================================================
# Base Item Class
# ============================================================================

@dataclasses.dataclass
class Item(_JSONDataclass):
    """Abstract base class for items returned by scrapers.
    
    An item can be anything. The string representation should be useful 
    for CLI output (e.g. a direct URL for the item).
    """

    @abc.abstractmethod
    def __str__(self):
        pass


# ============================================================================
# HTTP Connection Classes
# ============================================================================

def _random_user_agent():
    """Generate a random Chrome user agent string."""
    def lerp(a1, b1, a2, b2, n):
        return (n - a1) / (b1 - a1) * (b2 - a2) + a2
    
    version = int(lerp(
        datetime.date(2023, 3, 7).toordinal(),
        datetime.date(2030, 9, 24).toordinal(),
        111, 200,
        datetime.date.today().toordinal()
    ))
    version += random.randint(-5, 1)
    version = max(version, 101)
    
    return (f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            f'(KHTML, like Gecko) Chrome/{version}.0.0.0 Safari/537.36')


_DEFAULT_USER_AGENT = _random_user_agent()


class _HTTPSConnection(urllib3.connection.HTTPSConnection):
    """HTTPS connection with enhanced logging."""
    
    def connect(self, *args, **kwargs):
        conn = super().connect(*args, **kwargs)
        
        # Log connection details (using private attributes)
        try:
            _logger.debug(f'Connected to: {self.sock.getpeername()}')
        except AttributeError:
            # self.sock might be SSLTransport which lacks getpeername
            pass
        
        try:
            _logger.debug(f'Connection cipher: {self.sock.cipher()}')
        except AttributeError:
            # Shouldn't be possible, but better safe than sorry
            pass
        
        return conn


class _HTTPSAdapter(requests.adapters.HTTPAdapter):
    """HTTPS adapter with custom connection class."""
    
    def init_poolmanager(self, *args, **kwargs):
        super().init_poolmanager(*args, **kwargs)
        
        # Install TLS cipher logger (uses private urllib3 attributes)
        try:
            self.poolmanager.pool_classes_by_scheme['https'].ConnectionCls = _HTTPSConnection
        except (AttributeError, KeyError) as e:
            _logger.debug(f'Could not install TLS cipher logger: '
                         f'{type(e).__module__}.{type(e).__name__} {e!s}')


# ============================================================================
# Main Scraper Class
# ============================================================================

class Scraper:
    """Abstract base class for web scrapers."""

    name = None

    def __init__(self, *, retries=3, proxies=None):
        """Initialize the scraper.
        
        Args:
            retries: Number of retry attempts for failed requests
            proxies: Proxy configuration for requests
        """
        self._retries = retries
        self._proxies = proxies
        self._session = requests.Session()
        self._session.mount('https://', _HTTPSAdapter())

    @abc.abstractmethod
    def get_items(self):
        """Iterator yielding Items."""
        pass

    def _get_entity(self):
        """Get the entity behind the scraper, if any.

        This method should be implemented by subclasses for actual retrieval.
        For accessing the scraper's entity, use the entity property.
        """
        return None

    @functools.cached_property
    def entity(self):
        """The entity associated with this scraper."""
        return self._get_entity()

    def _request(self, method, url, params=None, data=None, headers=None, 
                timeout=10, responseOkCallback=None, allowRedirects=True, 
                proxies=None):
        """Make an HTTP request with retry logic and logging.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Target URL
            params: URL parameters
            data: Request body data
            headers: HTTP headers
            timeout: Request timeout in seconds
            responseOkCallback: Function to validate response
            allowRedirects: Whether to follow redirects
            proxies: Proxy configuration for this request
            
        Returns:
            requests.Response: The successful response
            
        Raises:
            ScraperException: If all retry attempts fail
        """
        if not headers:
            headers = {}
        if 'User-Agent' not in headers:
            headers['User-Agent'] = _DEFAULT_USER_AGENT
            
        proxies = proxies or self._proxies or {}
        errors = []
        
        for attempt in range(self._retries + 1):
            # Prepare request fresh each time for potential cookie updates
            req = self._session.prepare_request(
                requests.Request(method, url, params=params, data=data, headers=headers)
            )
            environment_settings = self._session.merge_environment_settings(
                req.url, proxies, None, None, None
            )
            
            # Log request details
            _logger.info(f'Retrieving {req.url}')
            _logger.debug(f'... with headers: {headers!r}')
            if data:
                _logger.debug(f'... with data: {data!r}')
            if environment_settings:
                _logger.debug(f'... with environmentSettings: {environment_settings!r}')
            
            try:
                response = self._session.send(
                    req, 
                    allow_redirects=allowRedirects, 
                    timeout=timeout, 
                    **environment_settings
                )
            except requests.exceptions.RequestException as exc:
                level = logging.INFO if attempt < self._retries else logging.ERROR
                retrying = ', retrying' if attempt < self._retries else ''
                _logger.log(level, f'Error retrieving {req.url}: {exc!r}{retrying}')
                errors.append(repr(exc))
            else:
                # Log successful response
                redirected = f' (redirected to {response.url})' if response.history else ''
                _logger.info(f'Retrieved {req.url}{redirected}: {response.status_code}')
                _logger.debug(f'... with response headers: {response.headers!r}')
                
                # Log redirect chain
                if response.history:
                    for i, redirect in enumerate(response.history):
                        _logger.debug(f'... request {i}: {redirect.request.url}: '
                                    f'{redirect.status_code} (Location: '
                                    f'{redirect.headers.get("Location")})')
                        _logger.debug(f'... ... with response headers: {redirect.headers!r}')
                
                # Validate response if callback provided
                if responseOkCallback is not None:
                    success, msg = responseOkCallback(response)
                    if msg:
                        errors.append(msg)
                else:
                    success, msg = (True, None)
                
                msg_suffix = f': {msg}' if msg else ''
                
                if success:
                    _logger.debug(f'{req.url} retrieved successfully{msg_suffix}')
                    return response
                else:
                    level = logging.INFO if attempt < self._retries else logging.ERROR
                    retrying = ', retrying' if attempt < self._retries else ''
                    _logger.log(level, f'Error retrieving {req.url}{msg_suffix}{retrying}')
            
            # Exponential backoff before retry
            if attempt < self._retries:
                sleep_time = 1.0 * 2**attempt  # 1s, 2s, 4s, etc.
                _logger.info(f'Waiting {sleep_time:.0f} seconds')
                time.sleep(sleep_time)
        
        # All attempts failed
        msg = f'{self._retries + 1} requests to {req.url} failed, giving up.'
        _logger.fatal(msg)
        _logger.fatal(f'Errors: {", ".join(errors)}')
        raise ScraperException(msg)

    def _get(self, *args, **kwargs):
        """Make a GET request."""
        return self._request('GET', *args, **kwargs)

    def _post(self, *args, **kwargs):
        """Make a POST request."""
        return self._request('POST', *args, **kwargs)


# Commented out module deprecation helper - uncomment if needed
# __getattr__, __dir__ = utils.module_deprecation_helper(__all__, Entity=Item)