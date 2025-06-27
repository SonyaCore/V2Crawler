import json
import logging

from typing import List, Dict, Set
from datetime import datetime

logger = logging.getLogger(__name__)

class LinkManager:
    """Manage VPN links - save, load, sort"""
    
    def __init__(self, output_file: str = "result.json"):
        self.output_file = output_file
    
    def save_links(self, links: Dict[str, List[str]], metadata: Dict = None):
        """Save links to JSON file with metadata"""
        # Calculate statistics
        total_links = sum(len(v) for v in links.values())
        link_stats = {protocol: len(protocol_links) for protocol, protocol_links in links.items() if protocol_links}
        
        data = {
            'timestamp': datetime.now().isoformat(),
            'total_links': total_links,
            'link_statistics': link_stats,
            'links': links,
            'metadata': metadata or {}
        }
        
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {total_links} links to {self.output_file}")
            
            # Log statistics
            for protocol, count in link_stats.items():
                logger.info(f"  {protocol.upper()}: {count} links")
                
        except Exception as e:
            logger.error(f"Error saving links: {e}")
    
    def load_links(self) -> Dict:
        """Load links from JSON file"""
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"File {self.output_file} not found")
            return {'links': {'vmess': [], 'vless': [], 'ss': [], 'trojan': [], 'ssr': []}}
        except Exception as e:
            logger.error(f"Error loading links: {e}")
            return {'links': {'vmess': [], 'vless': [], 'ss': [], 'trojan': [], 'ssr': []}}
    
    def export_for_testing(self, links: Dict[str, List[str]], filename: str = "links.txt"):
        """Export all links to a simple text file for the Go testing service"""
        try:
            total_links = 0
            with open(filename, 'w', encoding='utf-8') as f:
                for protocol, protocol_links in links.items():
                    for link in protocol_links:
                        f.write(f"{link}\n")
                        total_links += 1
            logger.info(f"Exported {total_links} validated links for testing to {filename}")
        except Exception as e:
            logger.error(f"Error exporting links for testing: {e}")
