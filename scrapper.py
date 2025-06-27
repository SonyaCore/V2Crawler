__all__ = ['LinkPreview', 'TelegramPost', 'Channel', 'TelegramChannelScraper']

import bs4
import dataclasses
import datetime
import logging
import re
import typing
import urllib.parse

import base

_logger = logging.getLogger(__name__)
_SINGLE_MEDIA_LINK_PATTERN = re.compile(r'^https://t\.me/[^/]+/\d+\?single$')
MAX_FETCH = 200


# ============================================================================
# Data Classes
# ============================================================================

@dataclasses.dataclass
class LinkPreview:
    """Represents a link preview embedded in a Telegram post."""
    
    href: str
    siteName: typing.Optional[str] = None
    title: typing.Optional[str] = None
    description: typing.Optional[str] = None
    image: typing.Optional[str] = None


@dataclasses.dataclass
class TelegramPost(base.Item):
    """Represents a single Telegram post with its content and metadata."""
    
    url: str
    date: datetime.datetime
    content: str
    outlinks: list
    linkPreview: typing.Optional[LinkPreview] = None

    # Deprecated property for backward compatibility
    outlinksss = base._DeprecatedProperty(
        'outlinksss', 
        lambda self: ' '.join(self.outlinks), 
        'outlinks'
    )

    def __str__(self):
        return self.url


@dataclasses.dataclass
class Channel(base.Item):
    """Represents a Telegram channel with its metadata and statistics."""
    
    username: str
    title: str
    verified: bool
    photo: str
    description: typing.Optional[str] = None
    members: typing.Optional[int] = None
    photos: typing.Optional[base.IntWithGranularity] = None
    videos: typing.Optional[base.IntWithGranularity] = None
    links: typing.Optional[base.IntWithGranularity] = None
    files: typing.Optional[base.IntWithGranularity] = None

    # Deprecated properties for backward compatibility
    photosGranularity = base._DeprecatedProperty(
        'photosGranularity', 
        lambda self: self.photos.granularity, 
        'photos.granularity'
    )
    videosGranularity = base._DeprecatedProperty(
        'videosGranularity', 
        lambda self: self.videos.granularity, 
        'videos.granularity'
    )
    linksGranularity = base._DeprecatedProperty(
        'linksGranularity', 
        lambda self: self.links.granularity, 
        'links.granularity'
    )
    filesGranularity = base._DeprecatedProperty(
        'filesGranularity', 
        lambda self: self.files.granularity, 
        'files.granularity'
    )

    def __str__(self):
        return f'https://t.me/s/{self.username}'


# ============================================================================
# Scraper Implementation
# ============================================================================

class TelegramChannelScraper(base.Scraper):
    """Scraper for Telegram channels and their posts."""
    
    name = 'telegram-channel'

    def __init__(self, name, **kwargs):
        """Initialize the Telegram channel scraper.
        
        Args:
            name: The username of the Telegram channel (without @)
            **kwargs: Additional arguments passed to the base Scraper
        """
        super().__init__(**kwargs)
        self._name = name
        self._headers = {
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/81.0.4044.138 Safari/537.36')
        }
        self._initial_page_response = None
        self._initial_page_soup = None

    def _initial_page(self):
        """Get the initial page response and parsed soup.
        
        Returns:
            tuple: (response, BeautifulSoup) for the channel's main page
            
        Raises:
            base.ScraperException: If the page cannot be retrieved
        """
        if self._initial_page_response is None:
            response = self._get(f'https://t.me/s/{self._name}', headers=self._headers)
            if response.status_code != 200:
                raise base.ScraperException(f'Got status code {response.status_code}')
            
            self._initial_page_response = response
            self._initial_page_soup = bs4.BeautifulSoup(response.text, 'lxml')
        
        return self._initial_page_response, self._initial_page_soup

    def _parse_number_with_suffix(self, text):
        """Parse a number that may have K or M suffix.
        
        Args:
            text: String like "1.2K" or "5.5M" or "1234"
            
        Returns:
            tuple: (value, granularity) where granularity indicates precision
        """
        text = text.replace(' ', '')
        
        if text.endswith('M'):
            base_value = float(text[:-1])
            value = int(base_value * 1e6)
            # Calculate granularity based on decimal places
            if '.' not in text:
                granularity = 10**6
            else:
                decimal_places = len(text[:-1].split('.')[1])
                granularity = 10**(6 - decimal_places)
        elif text.endswith('K'):
            base_value = float(text[:-1])
            value = int(base_value * 1000)
            # Calculate granularity based on decimal places
            if '.' not in text:
                granularity = 10**3
            else:
                decimal_places = len(text[:-1].split('.')[1])
                granularity = 10**(3 - decimal_places)
        else:
            value = int(text)
            granularity = 1
            
        return value, granularity

    def _extract_link_preview(self, post_element, page_url):
        """Extract link preview information from a post element.
        
        Args:
            post_element: BeautifulSoup element containing the post
            page_url: Base URL for resolving relative links
            
        Returns:
            LinkPreview or None: The extracted link preview data
        """
        link_preview_element = post_element.find('a', class_='tgme_widget_message_link_preview')
        if not link_preview_element:
            return None
            
        kwargs = {
            'href': urllib.parse.urljoin(page_url, link_preview_element['href'])
        }
        
        # Extract site name
        site_name_div = link_preview_element.find('div', class_='link_preview_site_name')
        if site_name_div:
            kwargs['siteName'] = site_name_div.text
            
        # Extract title
        title_div = link_preview_element.find('div', class_='link_preview_title')
        if title_div:
            kwargs['title'] = title_div.text
            
        # Extract description
        description_div = link_preview_element.find('div', class_='link_preview_description')
        if description_div:
            kwargs['description'] = description_div.text
            
        # Extract image
        image_element = link_preview_element.find('i', class_='link_preview_image')
        if image_element and 'style' in image_element.attrs:
            style = image_element['style']
            if style.startswith("background-image:url('"):
                end_quote = style.index("'", 22)
                kwargs['image'] = style[22:end_quote]
            else:
                _logger.warning(f'Could not process link preview image style: {style}')
                
        return LinkPreview(**kwargs)

    def _extract_outlinks(self, post_element, page_url, raw_url, canonical_url):
        """Extract outbound links from a post element.
        
        Args:
            post_element: BeautifulSoup element containing the post
            page_url: Base URL for resolving relative links
            raw_url: Original post URL
            canonical_url: Canonical post URL
            
        Returns:
            list: List of unique outbound link URLs
        """
        outlinks = []
        
        for link in post_element.find_all('a'):
            # Skip author links (avatar and name)
            parent_classes = link.parent.attrs.get('class', [])
            if any(cls in parent_classes for cls in ('tgme_widget_message_user', 'tgme_widget_message_author')):
                continue
                
            # Skip links to the post itself
            if link['href'] in (raw_url, canonical_url):
                continue
                
            # Skip individual media links
            if _SINGLE_MEDIA_LINK_PATTERN.match(link['href']):
                continue
                
            # Add unique outbound links
            href = urllib.parse.urljoin(page_url, link['href'])
            if href not in outlinks:
                outlinks.append(href)
                
        return outlinks

    def _soup_to_items(self, soup, page_url, only_username=False):
        """Convert soup elements to TelegramPost items.
        
        Args:
            soup: BeautifulSoup object containing the page
            page_url: URL of the page being parsed
            only_username: If True, yield only the username from the first post
            
        Yields:
            TelegramPost or str: Post objects or username string
        """
        posts = soup.find_all('div', attrs={'class': 'tgme_widget_message', 'data-post': True})
        
        for post in reversed(posts):
            if only_username:
                yield post['data-post'].split('/')[0]
                return
                
            # Extract date and URL
            footer = post.find('div', class_='tgme_widget_message_footer')
            date_link = footer.find('a', class_='tgme_widget_message_date')
            raw_url = date_link['href']
            
            # Validate URL format
            if (not raw_url.startswith('https://t.me/') or 
                raw_url.count('/') != 4 or 
                raw_url.rsplit('/', 1)[1].strip('0123456789') != ''):
                _logger.warning(f'Possibly incorrect URL: {raw_url!r}')
                
            # Convert to canonical URL format
            canonical_url = raw_url.replace('//t.me/', '//t.me/s/')
            
            # Parse date
            time_element = date_link.find('time', datetime=True)
            datetime_str = time_element['datetime']
            # Remove hyphens from date part and colons from time part
            normalized_datetime = datetime_str.replace('-', '', 2).replace(':', '')
            date = datetime.datetime.strptime(normalized_datetime, '%Y%m%dT%H%M%S%z')
            
            # Extract content
            message_element = post.find('div', class_='tgme_widget_message_text')
            content = message_element.text if message_element else None
            
            # Extract outlinks
            outlinks = self._extract_outlinks(post, page_url, raw_url, canonical_url)
            
            # Extract link preview
            link_preview = self._extract_link_preview(post, page_url)
            
            yield TelegramPost(
                url=canonical_url,
                date=date,
                content=content,
                outlinks=outlinks,
                linkPreview=link_preview
            )

    def get_items(self):
        """Get up to 1000 latest posts from the Telegram channel.
        
        Yields:
            TelegramPost: Individual posts from the channel (limited to 1000)
            
        Raises:
            base.ScraperException: If pages cannot be retrieved
        """
        response, soup = self._initial_page()
        
        # Check if channel has public posts
        if '/s/' not in response.url:
            _logger.warning('No public post list for this user')
            return
            
        post_count = 0
        max_posts = MAX_FETCH
        
        while post_count < max_posts:
            posts_yielded_this_page = 0
            
            for post in self._soup_to_items(soup, response.url):
                if post_count >= max_posts:
                    break
                yield post
                post_count += 1
                posts_yielded_this_page += 1
            
            # If we've reached the limit, break out of the outer loop
            if post_count >= max_posts:
                _logger.info(f'Reached maximum post limit of {max_posts}')
                break
                
            # Look for "Load more" link
            page_link = soup.find('a', attrs={'class': 'tme_messages_more', 'data-before': True})
            if not page_link:
                _logger.info(f'No more pages to load. Total posts fetched: {post_count}')
                break
                
            # Load next page
            next_page_url = urllib.parse.urljoin(response.url, page_link['href'])
            response = self._get(next_page_url, headers=self._headers)
            
            if response.status_code != 200:
                raise base.ScraperException(f'Got status code {response.status_code}')
                
            soup = bs4.BeautifulSoup(response.text, 'lxml')

    def _get_entity(self):
        """Extract channel information and metadata.
        
        Returns:
            Channel: The channel object with metadata
            
        Raises:
            base.ScraperException: If channel pages cannot be retrieved
        """
        channel_data = {}
        
        # Get member count and photo from /channel page (more accurate)
        response = self._get(f'https://t.me/{self._name}', headers=self._headers)
        if response.status_code != 200:
            raise base.ScraperException(f'Got status code {response.status_code}')
            
        soup = bs4.BeautifulSoup(response.text, 'lxml')
        
        # Extract member count
        members_div = soup.find('div', class_='tgme_page_extra')
        if members_div and members_div.text.endswith(' subscribers'):
            members_text = members_div.text[:-12].replace(' ', '')
            channel_data['members'] = int(members_text)
            
        # Extract profile photo
        photo_img = soup.find('img', class_='tgme_page_photo_image')
        if photo_img:
            channel_data['photo'] = photo_img.attrs['src']

        # Get additional info from /s/ page
        response, soup = self._initial_page()
        
        # Check if channel has public posts
        if '/s/' not in response.url:
            return None
            
        # Extract channel info
        channel_info_div = soup.find('div', class_='tgme_channel_info')
        if not channel_info_div:
            raise base.ScraperException('Channel info div not found')
            
        # Extract title and verification status
        title_div = channel_info_div.find('div', class_='tgme_channel_info_header_title')
        title_span = title_div.find('span')
        channel_data['title'] = title_span.text
        channel_data['verified'] = bool(title_div.find('i', class_='verified-icon'))
        
        # Extract canonical username from first post
        try:
            channel_data['username'] = next(self._soup_to_items(soup, response.url, only_username=True))
        except StopIteration:
            # Fallback to channel info div (may not be properly capitalized)
            _logger.warning('Could not find a post; extracting username from channel info div, '
                          'which may not be capitalized correctly')
            username_div = channel_info_div.find('div', class_='tgme_channel_info_header_username')
            channel_data['username'] = username_div.text[1:]  # Remove @
            
        # Extract description
        description_div = channel_info_div.find('div', class_='tgme_channel_info_description')
        if description_div:
            channel_data['description'] = description_div.text
            
        # Extract media counters
        for counter_div in channel_info_div.find_all('div', class_='tgme_channel_info_counter'):
            value_span = counter_div.find('span', class_='counter_value')
            type_span = counter_div.find('span', class_='counter_type')
            
            if not value_span or not type_span:
                continue
                
            counter_type = type_span.text
            
            # Skip members counter (already extracted more accurately)
            if counter_type == 'members':
                continue
                
            # Parse and store media counters
            if counter_type in ('photos', 'videos', 'links', 'files'):
                value, granularity = self._parse_number_with_suffix(value_span.text)
                channel_data[counter_type] = base.IntWithGranularity(value, granularity)

        return Channel(**channel_data)