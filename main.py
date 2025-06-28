#!/usr/bin/env python3
"""
V2CRAWELR Service - V2Ray Extended Crawler Service for Railway
"""

import argparse
import asyncio
import logging
import os
import schedule
import time
import threading
import traceback
import requests
import base64
from typing import List, Dict
from datetime import datetime

import aiohttp
import scrapper
import extractor
import duplicate
import manager
from utils import read_channels_from_file, read_github_urls_from_file

__version__ = "1.2.1"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class NamiraInterface:
    """Interface to communicate with the Go rayping service"""

    def __init__(self, namira_xapi: str, namira_url: str = "http://localhost:8080"):
        self.service_url = namira_url
        self.xapi = namira_xapi

    async def send_links(self, links_content: str) -> Dict:
        """Send links content directly as text file"""
        try:
            logger.info(f"Sending {len(links_content.splitlines())} links to namira service")
            
            headers = {"X-API-Key": self.xapi}
            data = aiohttp.FormData()
            data.add_field(
                'file',
                links_content,
                filename='links.txt',
                content_type='text/plain'
            )
            
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.post(
                    f"{self.service_url}/scan",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=100)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info("Links successfully sent to namira service")
                        return result
                    else:
                        error_text = await response.text()
                        logger.error(f"Namira service error: {response.status} - {error_text}")
                        return {}
                        
        except Exception as e:
            logger.error(f"Error communicating with namira service: {e}")
            logger.debug(traceback.format_exc())
            return {}


class UnifiedChannelScraper:
    """Scrape multiple sources (Telegram channels and GitHub repos) for VPN links"""

    def __init__(self, telegram_channels: List[str], github_urls: List[str], github_rate_limit: float = 1.0):
        self.telegram_channels = telegram_channels or []
        self.github_urls = github_urls or []
        self.github_rate_limit = github_rate_limit
        
        self.extractor = extractor.VPNLinkExtractor()
        self.duplicate_checker = duplicate.DuplicateChecker()

    async def scrape_telegram_channel(self, channel: str) -> Dict[str, List[str]]:
        """Scrape a single Telegram channel"""
        try:
            logger.info(f"Scraping Telegram channel: {channel}")
            scraper = scrapper.TelegramChannelScraper(channel)
            items = scraper.get_items()

            all_links = {"vmess": [], "vless": [], "ss": [], "trojan": [], "ssr": []}

            for post in items:
                if hasattr(post, "content") and post.content:
                    links = self.extractor.extract_links(post.content)
                    for protocol in all_links:
                        all_links[protocol].extend(links[protocol])

            total = sum(len(v) for v in all_links.values())
            logger.info(f"Found {total} links from Telegram channel {channel}")
            return all_links

        except Exception as e:
            logger.error(f"Error scraping Telegram channel {channel}: {e}")
            return {"vmess": [], "vless": [], "ss": [], "trojan": [], "ssr": []}

    async def scrape_github_urls(self, urls: List[str]) -> Dict[str, List[str]]:
        """Scrape GitHub URLs using simplified scraper"""
        try:
            logger.info(f"Scraping {len(urls)} GitHub URLs")
            
            github_scraper = scrapper.SimpleGitHubChannelScraper(urls, self.github_rate_limit)
            items = await github_scraper.get_items()

            all_links = {"vmess": [], "vless": [], "ss": [], "trojan": [], "ssr": []}

            for post in items:
                if hasattr(post, "content") and post.content:
                    links = self.extractor.extract_links(post.content)
                    for protocol in all_links:
                        all_links[protocol].extend(links[protocol])

            total = sum(len(v) for v in all_links.values())
            logger.info(f"Found {total} total links from GitHub URLs")
            
            return all_links

        except Exception as e:
            logger.error(f"Error scraping GitHub URLs: {e}")
            logger.debug(traceback.format_exc())
            return {"vmess": [], "vless": [], "ss": [], "trojan": [], "ssr": []}

    async def scrape_all_sources(self) -> Dict[str, List[str]]:
        """Scrape all sources (Telegram and GitHub) concurrently"""
        tasks = []
        
        # Add Telegram channel tasks
        for channel in self.telegram_channels:
            tasks.append(self.scrape_telegram_channel(channel))
            
        # Add GitHub URLs task
        if self.github_urls:
            tasks.append(self.scrape_github_urls(self.github_urls))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine results
        combined_links = {"vmess": [], "vless": [], "ss": [], "trojan": [], "ssr": []}
        for result in results:
            if isinstance(result, dict):
                for protocol in combined_links:
                    combined_links[protocol].extend(result[protocol])
            elif isinstance(result, Exception):
                logger.error(f"Task failed: {result}")

        # Deduplicate across all sources
        combined_links = self.duplicate_checker.deduplicate_links(combined_links)
        return combined_links


class V2CrawlerService:
    """Main service class"""
    
    def __init__(self):
        self.load_config()
        self.setup_components()
    
    def load_config(self):
        """Load configuration from environment variables"""
        self.namira_xapi = os.getenv('NAMIRA_XAPI')
        self.namira_url = os.getenv('NAMIRA_URL', 'http://localhost:8080')
        self.channels_url = os.getenv('CHANNELS_URL', 
                                     'https://raw.githubusercontent.com/NaMiraNet/RayExt/refs/heads/main/results/channels_latest.txt')
        self.github_url = os.getenv('GITHUB_URL',
                                      'https://raw.githubusercontent.com/NaMiraNet/RayExt/refs/heads/main/results/git_latest.txt'
                                      )
        self.github_rate_limit = float(os.getenv('GITHUB_RATE_LIMIT', '1.0'))
        
        # Scheduler config
        self.schedule_enabled = os.getenv('SCHEDULE_ENABLED', 'true').lower() == 'true'
        self.schedule_interval = int(os.getenv('SCHEDULE_INTERVAL_MINUTES', '60'))
        self.run_immediately = os.getenv('RUN_IMMEDIATELY', 'true').lower() == 'true'
    
    def setup_components(self):
        """Initialize service components"""
        self.link_manager = manager.LinkManager('vpn_links.json')
        if self.namira_xapi:
            self.namira = NamiraInterface(self.namira_xapi, self.namira_url)
        else:
            self.namira = None
            logger.warning("NAMIRA_XAPI not set - will not send links to namira service")

    def download_channels_list(self) -> str:
        """Download and decode channels list from URL"""
        try:
            logger.info("Downloading channels list...")
            response = requests.get(self.channels_url, timeout=30)
            response.raise_for_status()
            
            decoded_content = base64.b64decode(response.content).decode('utf-8')
            
            with open('channels.txt', 'w') as f:
                f.write(decoded_content)
                
            lines_count = len(decoded_content.strip().split('\n'))
            logger.info(f"Downloaded {lines_count} channels")
            return 'channels.txt'
            
        except Exception as e:
            logger.error(f"Error downloading channels list: {e}")
            raise

    def download_github_list(self) -> str:
        """Download and decode github list from URL"""
        try:
            logger.info("Downloading github list...")
            response = requests.get(self.github_url, timeout=30)
            response.raise_for_status()
            
            content = response.content.decode('utf-8')

            with open('git_channels.txt', 'w') as f:
                f.write(content)
                
            lines_count = len(content.strip().split('\n'))
            logger.info(f"Downloaded {lines_count} links")
            return 'git_channels.txt'
            
        except Exception as e:
            logger.error(f"Error downloading links list: {e}")
            raise

    async def run_scraper(self):
        """Main scraping function"""
        try:
            logger.info(f"V2Crawler Service {__version__} starting...")

            # Load sources
            channels_file = self.download_channels_list()
            github_file = self.download_github_list()
            telegram_channels = read_channels_from_file(channels_file)
            github_links = read_github_urls_from_file(github_file)
            
            logger.info(f"Sources: {len(telegram_channels)} Telegram channels, {len(github_file)} GitHub URLs")

            # Initialize scraper
            scraper = UnifiedChannelScraper(
                telegram_channels=telegram_channels,
                github_urls=github_links,
                github_rate_limit=self.github_rate_limit
            )
            
            # Scrape all sources
            logger.info("Starting scraping from all sources...")
            links = await scraper.scrape_all_sources()

            total_links = sum(len(v) for v in links.values())
            logger.info(f"Total valid links found: {total_links}")

            if total_links == 0:
                logger.warning("No valid links found")
                return

            # Save links
            metadata = {
                "telegram_channels": telegram_channels,
                "github_urls": github_links,
                "scraping_timestamp": datetime.now().isoformat(),
                "version": __version__,
                "environment": "railway" if os.getenv('RAILWAY_ENVIRONMENT') else "local"
            }
            self.link_manager.save_links(links, metadata)

            # Export and send to namira
            content = self.link_manager.get_content(links)
            if self.namira:
                await self.namira.send_links(content)

            # Print summary
            self.print_summary(links, len(telegram_channels), len(github_links))

        except Exception as e:
            logger.error(f"Scraping process error: {e}")
            logger.debug(traceback.format_exc())

    def print_summary(self, links: Dict[str, List[str]], telegram_count: int, github_count: int):
        """Print scraping summary"""
        logger.info("=== SCRAPING SUMMARY ===")
        for protocol, link_list in links.items():
            if link_list:
                logger.info(f"{protocol.upper()}: {len(link_list)} links")
        
        logger.info(f"Total sources: {telegram_count + github_count}")
        logger.info(f"Telegram channels: {telegram_count}")
        logger.info(f"GitHub repositories: {github_count}")

    async def start_service(self):
        """Start the service with optional scheduling"""
        logger.info(f"V2Crawler Service {__version__}")
        logger.info(f"Schedule: {'enabled' if self.schedule_enabled else 'disabled'}")
        
        if self.schedule_enabled:
            logger.info(f"Schedule interval: {self.schedule_interval} minutes")

        # Run immediately if configured
        if self.run_immediately:
            await self.run_scraper()

        # Setup scheduler if enabled
        if self.schedule_enabled:
            loop = asyncio.get_running_loop()
            schedule.every(self.schedule_interval).minutes.do(
                lambda: asyncio.run_coroutine_threadsafe(self.run_scraper(), loop)
            )
            
            # Start scheduler thread
            scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            scheduler_thread.start()
            logger.info("Scheduler started")

            # Keep service alive
            try:
                while True:
                    await asyncio.sleep(60)
                    logger.debug("Service heartbeat")
            except KeyboardInterrupt:
                logger.info("Service stopped by user")
        else:
            logger.info("Scheduling disabled - service will exit")

    def _run_scheduler(self):
        """Run the scheduler in a separate thread"""
        while True:
            schedule.run_pending()
            time.sleep(60)


async def main():
    """entry point for Railway deployment"""
    if os.getenv('RAILWAY_ENVIRONMENT'):
        logger.info("Running in Railway environment")
        service = V2CrawlerService()
        await service.start_service()
    else:
        await run_cli()


async def run_cli():
    """CLI interface for local development"""
    parser = argparse.ArgumentParser(description="V2Crawler - V2Ray Link Crawler")
    parser.add_argument("-t", "--telegram-input", help="Telegram channels file")
    parser.add_argument("-g", "--github-input", help="GitHub URLs file")
    parser.add_argument("-o", "--output", default="vpn_links.json", help="Output file")
    parser.add_argument("-u", "--namira-url", default="http://localhost:8080", help="Namira service URL")
    parser.add_argument("-x", "--namira-xapi", default="namira-xapi", help="Namira X-API key")
    parser.add_argument("-r", "--github-rate-limit", type=float, default=1.0, help="GitHub rate limit (seconds)")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.telegram_input and not args.github_input:
        logger.error("At least one input source must be provided")
        exit(1)

    # Load sources
    telegram_channels = read_channels_from_file(args.telegram_input) if args.telegram_input else []
    github_urls = read_github_urls_from_file(args.github_input) if args.github_input else []

    # Initialize components
    scraper = UnifiedChannelScraper(
        telegram_channels=telegram_channels,
        github_urls=github_urls,
        github_rate_limit=args.github_rate_limit
    )
    link_manager = manager.LinkManager(args.output)
    namira = NamiraInterface(args.namira_xapi, args.namira_url)

    try:
        # Scrape links
        links = await scraper.scrape_all_sources()
        total_links = sum(len(v) for v in links.values())
        
        if total_links == 0:
            logger.warning("No valid links found")
            return

        # Save and send
        metadata = {
            "telegram_channels": telegram_channels,
            "github_urls": github_urls,
            "scraping_timestamp": datetime.now().isoformat(),
            "version": __version__,
            "environment": "local"
        }
        link_manager.save_links(links, metadata)
        link_manager.export_for_testing(links)
        content = link_manager.get_content(links)
        
        await namira.send_links(content)

        # Summary
        logger.info("=== SCRAPING SUMMARY ===")
        for protocol, link_list in links.items():
            if link_list:
                logger.info(f"{protocol.upper()}: {len(link_list)} links")

    except Exception as e:
        logger.error(f"CLI process error: {e}")
        logger.debug(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(main())