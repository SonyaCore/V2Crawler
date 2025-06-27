import base64
import json
import re
import logging
from urllib.parse import urlparse
from typing import List, Dict, Set


logger = logging.getLogger(__name__)

class DuplicateChecker:
    """Advanced duplicate detection for VPN links"""
    
    def __init__(self):
        self.seen_hashes = set()
        self.seen_configs = set()
    
    def normalize_link(self, link: str) -> str:
        """Normalize link for better duplicate detection"""
        link = re.sub(r'#[^#]*$', '', link)  # Remove fragment
        return link.strip().lower()
    
    def extract_config_signature(self, link: str) -> str:
        """Extract a signature from the link configuration"""
        try:
            if link.startswith('vmess://'):
                decoded = base64.b64decode(link[8:], validate=True).decode('utf-8')
                config = json.loads(decoded)
                # Create signature from essential fields
                signature_parts = [
                    config.get('add', ''),
                    str(config.get('port', '')),
                    config.get('id', ''),
                    config.get('net', ''),
                    config.get('type', '')
                ]
                return '|'.join(signature_parts)
            elif link.startswith('ss://'):
                # For SS, the part before @ is the method:password, after @ is server:port
                link_data = link[5:]
                if '@' in link_data:
                    creds, server = link_data.split('@', 1)
                    server = server.split('#')[0]  # Remove fragment
                    return f"{creds}@{server}"
            elif link.startswith('vless://') or link.startswith('trojan://'):
                # Extract server and port info
                parsed = urlparse(link)
                return f"{parsed.hostname}:{parsed.port}"
        except Exception:
            pass
        
        # Fallback to normalized link
        return self.normalize_link(link)
    
    def is_duplicate(self, link: str) -> bool:
        """Check if link is a duplicate"""
        normalized = self.normalize_link(link)
        config_sig = self.extract_config_signature(link)
        
        # Check if we've seen this exact link or config before
        if normalized in self.seen_hashes or config_sig in self.seen_configs:
            return True
        
        # Add to seen sets
        self.seen_hashes.add(normalized)
        self.seen_configs.add(config_sig)
        return False
    
    def deduplicate_links(self, links: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """Remove duplicates from links dictionary"""
        deduplicated = {'vmess': [], 'vless': [], 'ss': [], 'trojan': [], 'ssr': []}
        duplicate_count = 0
        
        for protocol, protocol_links in links.items():
            for link in protocol_links:
                if not self.is_duplicate(link):
                    deduplicated[protocol].append(link)
                else:
                    duplicate_count += 1
        
        logger.info(f"Removed {duplicate_count} duplicate links")
        return deduplicated
