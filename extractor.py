import base64 
import re
import json
import logging

import asyncio
import aiohttp 
import time

from urllib.parse import urlparse, urljoin
from typing import List, Dict, Set , Optional

logger = logging.getLogger(__name__)


logger = logging.getLogger(__name__)

class VPNLinkExtractor:
    """Extract and validate VPN links from text content"""
    
    def __init__(self):
        # Improved regex patterns for different VPN protocols
        self.patterns = {
            'vmess': re.compile(r'vmess://[A-Za-z0-9+/=]{8,}(?=\s|$|vmess://|vless://|ss://|trojan://|ssr://)', re.IGNORECASE),
            'vless': re.compile(r'vless://[a-f0-9\-]{36}@[^\s]+?(?=\s|$|vmess://|vless://|ss://|trojan://|ssr://)', re.IGNORECASE),
            'ss': re.compile(r'ss://[A-Za-z0-9+/=]{8,}@[^\s#]+(?:#[^\s]*?)?(?=\s|$|vmess://|vless://|ss://|trojan://|ssr://)', re.IGNORECASE),
            'trojan': re.compile(r'trojan://[a-f0-9\-]{8,}@[^\s]+?(?=\s|$|vmess://|vless://|ss://|trojan://|ssr://)', re.IGNORECASE),
            'ssr': re.compile(r'ssr://[A-Za-z0-9+/=]{12,}(?=\s|$|vmess://|vless://|ss://|trojan://|ssr://)', re.IGNORECASE)
        }
    
    def is_valid_base64(self, s: str) -> bool:
        """Check if string is valid base64"""
        try:
            if len(s) < 4:
                return False
            # Add padding if necessary
            missing_padding = len(s) % 4
            if missing_padding:
                s += '=' * (4 - missing_padding)
            base64.b64decode(s, validate=True)
            return True
        except Exception:
            return False
    
    def validate_ss_link(self, link: str) -> bool:
        """Enhanced validation for Shadowsocks links"""
        try:
            if not link.startswith('ss://'):
                return False
            
            # Remove ss:// prefix
            link_data = link[5:]
            
            # Check if it contains @ (required for server info)
            if '@' not in link_data:
                return False
            
            # Split by @ to get credentials and server info
            parts = link_data.split('@', 1)
            if len(parts) != 2:
                return False
            
            credentials, server_info = parts
            
            # Validate base64 credentials
            if not self.is_valid_base64(credentials):
                return False
            
            # Server info should have host:port format
            server_part = server_info.split('#')[0]  # Remove fragment if present
            if ':' not in server_part:
                return False
            
            host, port = server_part.rsplit(':', 1)
            
            # Basic validation
            if not host or not port:
                return False
            
            # Port should be numeric
            try:
                port_num = int(port)
                if not (1 <= port_num <= 65535):
                    return False
            except ValueError:
                return False
            
            # Decode and validate credentials format
            try:
                decoded = base64.b64decode(credentials, validate=True).decode('utf-8')
                # Should be in format: method:password
                if ':' not in decoded:
                    return False
            except Exception:
                return False
            
            return True
            
        except Exception as e:
            logger.debug(f"SS validation error for {link[:50]}...: {e}")
            return False
    
    def validate_vmess_link(self, link: str) -> bool:
        """Enhanced validation for VMess links"""
        try:
            if not link.startswith('vmess://'):
                return False
            
            link_data = link[8:]
            if not self.is_valid_base64(link_data):
                return False
            
            # Try to decode and parse JSON
            try:
                decoded = base64.b64decode(link_data, validate=True).decode('utf-8')
                config = json.loads(decoded)
                
                # Check required fields
                required_fields = ['add', 'port', 'id', 'ps']
                for field in required_fields:
                    if field not in config or not config[field]:
                        return False
                
                # Validate port
                try:
                    port = int(config['port'])
                    if not (1 <= port <= 65535):
                        return False
                except (ValueError, TypeError):
                    return False
                
                return True
            except (json.JSONDecodeError, UnicodeDecodeError):
                return False
                
        except Exception as e:
            logger.debug(f"VMess validation error for {link[:50]}...: {e}")
            return False
    
    def extract_links(self, text: str) -> Dict[str, List[str]]:
        """Extract all VPN links from text with improved validation"""
        links = {'vmess': [], 'vless': [], 'ss': [], 'trojan': [], 'ssr': []}
        
        for protocol, pattern in self.patterns.items():
            matches = pattern.findall(text)
            
            for match in matches:
                # Additional validation based on protocol
                if protocol == 'ss' and not self.validate_ss_link(match):
                    logger.debug(f"Invalid SS link filtered: {match[:50]}...")
                    continue
                elif protocol == 'vmess' and not self.validate_vmess_link(match):
                    logger.debug(f"Invalid VMess link filtered: {match[:50]}...")
                    continue
                elif self.validate_link(match):
                    links[protocol].append(match)
        
        return links
    
    def validate_link(self, link: str) -> bool:
        """Basic validation of VPN link format"""
        try:
            if link.startswith(('vmess://', 'vless://', 'ss://', 'trojan://', 'ssr://')):
                # Must be longer than just the protocol
                if len(link) <= len(link.split('://')[0]) + 10:
                    return False
                
                # Should not contain obviously invalid characters
                if any(char in link for char in ['\n', '\r', '\t']):
                    return False
                
                return True
        except Exception as e:
            logger.error(f"Error validating link: {e}")
        return False

class GitHubScraper:
    """Simple GitHub scraper that fetches exact URLs provided by user"""
    
    def __init__(self, rate_limit_delay: float = 1.0):
        self.rate_limit_delay = rate_limit_delay
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=100),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def convert_to_raw_url(self, url: str) -> str:
        """Convert GitHub URLs to raw content URLs if needed"""
        try:
            # Already a raw URL or external URL
            if 'raw.githubusercontent.com' in url or 'github.com' not in url:
                return url
                
            if '/blob/' in url:
                return url.replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
                
            if 'github.com' in url:
                return url.replace('github.com', 'raw.githubusercontent.com')
                
            return url
            
        except Exception as e:
            logger.error(f"Error converting GitHub URL {url}: {e}")
            return url
    
    async def fetch_content(self, url: str) -> Optional[str]:
        """Fetch content from any URL"""
        try:
            await asyncio.sleep(self.rate_limit_delay)  # Rate limiting
            
            logger.debug(f"Fetching content from: {url}")
            async with self.session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    logger.info(f"Successfully fetched {len(content)} characters from {url}")
                    return content
                elif response.status == 404:
                    logger.warning(f"File not found (404): {url}")
                    return None
                else:
                    logger.warning(f"HTTP {response.status} for {url}")
                    return None
                    
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching {url}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    async def scrape_url(self, url: str) -> Optional[str]:
        """Scrape a single URL for content"""
        try:
            logger.info(f"Processing URL: {url}")
            
            # Convert to raw URL if it's a GitHub URL
            raw_url = self.convert_to_raw_url(url)
            
            if raw_url != url:
                logger.info(f"Converted to raw URL: {raw_url}")
            
            # Fetch the content
            content = await self.fetch_content(raw_url)
            
            if content:
                # Basic validation - check if it has any VPN-like content
                if self.looks_like_vpn_content(content):
                    logger.info(f"Content appears to contain VPN links: {len(content)} chars")
                else:
                    logger.info(f"Content fetched but no obvious VPN patterns detected: {len(content)} chars")
                
                return content
            else:
                logger.warning(f"No content retrieved from {raw_url}")
                return None
                
        except Exception as e:
            logger.error(f"Error processing URL {url}: {e}")
            return None
    
    def looks_like_vpn_content(self, content: str) -> bool:
        """Basic check if content might contain VPN configs"""
        if not content:
            return False
            
        # Check for VPN protocol patterns
        vpn_patterns = [
            r'vmess://',
            r'vless://',
            r'ss://',
            r'trojan://',
            r'ssr://',
            # Also check for base64-like patterns which might be encoded configs
            r'[A-Za-z0-9+/]{20,}={0,2}',
            # Check for common config keywords
            r'server.*port',
            r'"address"',
            r'"port"',
            r'"id"',
            r'"host"'
        ]
        
        content_lower = content.lower()
        for pattern in vpn_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
                
        return False


class SimpleGitHubChannelScraper:
    """GitHub scraper that mimics the interface of TelegramChannelScraper"""
    
    def __init__(self, urls: List[str], rate_limit_delay: float = 1.0):
        self.urls = urls if isinstance(urls, list) else [urls]
        self.rate_limit_delay = rate_limit_delay
        
    class GitHubPost:
        """Mock post object to match Telegram scraper interface"""
        def __init__(self, content: str, url: str):
            self.content = content
            self.url = url
    
    async def get_items(self) -> List[GitHubPost]:
        """Get items (content) from GitHub URLs"""
        items = []
        
        async with GitHubScraper(self.rate_limit_delay) as scraper:
            for url in self.urls:
                try:
                    content = await scraper.scrape_url(url)
                    if content:
                        items.append(self.GitHubPost(content, url))
                        logger.info(f"Added content from {url}")
                    else:
                        logger.warning(f"No content retrieved from {url}")
                except Exception as e:
                    logger.error(f"Error processing GitHub URL {url}: {e}")
                    
        logger.info(f"Retrieved content from {len(items)} out of {len(self.urls)} URLs")
        return items


