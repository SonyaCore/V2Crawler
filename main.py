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
from typing import List, Dict
from datetime import datetime  
import requests
import base64

import scrapper
import extractor
import duplicate
import manager
import traceback

# req session
import io
import aiohttp
import tempfile

# schedular
import schedule

# utils
from utils import read_channels_from_file, read_github_urls_from_file


__version__ = "1.2.0"

# Configure logging for Railway
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],  # Railway captures stdout
)
logger = logging.getLogger(__name__)



class NamiraInterface:
    """Interface to communicate with the Go rayping service"""

    def __init__(self, namira_xapi: str, namira_url: str = "http://localhost:8080"):
        self.service_url = namira_url
        self.xapi = namira_xapi

    async def send_links(self, links_dict: dict) -> Dict:
        """Send links from a dictionary to namira service as actual file"""
        temp_file_path = None
        try:
            links_content = "\n".join(str(link) for link in links_dict.values())
            
            # Create temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as temp_file:
                temp_file.write(links_content)
                temp_file_path = temp_file.name
            
            logger.info(f"Created temporary file: {temp_file_path} with {len(links_dict)} links")

            headers = {"X-API-Key": self.xapi}
            async with aiohttp.ClientSession(headers=headers) as session:
                data = aiohttp.FormData()
                
                # Open and read the actual file
                with open(temp_file_path, 'rb') as file:
                    data.add_field(
                        'file',
                        file,
                        filename='links.txt',
                        content_type='text/plain'
                    )

                    async with session.post(
                        f"{self.service_url}/scan",
                        data=data,
                        timeout=aiohttp.ClientTimeout(total=100)
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            logger.info("namira data has been sent to URI")
                            return result
                        else:
                            error_text = await response.text()
                            logger.error(f"namira service error: {response.status} - {error_text}")
                            return {}
        except Exception:
            logger.error(
                "Error communicating with rayping service on %s:\n%s",
                self.service_url,
                traceback.format_exc(),
            )
            return {}
        finally:
            # Clean up temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.debug(f"Cleaned up temporary file: {temp_file_path}")
                except Exception as e:
                    logger.warning(f"Failed to clean up temporary file {temp_file_path}: {e}")


# Updated unified scraper
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

            logger.info(
                f"Telegram channel {channel}: Found {sum(len(v) for v in all_links.values())} valid links"
            )
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
                    logger.info(f"Extracting links from content ({len(post.content)} chars) from {post.url}")
                    links = self.extractor.extract_links(post.content)  
                    for protocol in all_links:
                        if links[protocol]:
                            logger.info(f"Found {len(links[protocol])} {protocol} links from {post.url}")
                        all_links[protocol].extend(links[protocol])

            total_found = sum(len(v) for v in all_links.values())
            logger.info(f"GitHub URLs: Found {total_found} total valid links")
            
            # Log breakdown by protocol
            for protocol, link_list in all_links.items():
                if link_list:
                    logger.info(f"  {protocol.upper()}: {len(link_list)} links")
            
            return all_links

        except Exception as e:
            logger.error(f"Error scraping GitHub URLs: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"vmess": [], "vless": [], "ss": [], "trojan": [], "ssr": []}

    async def scrape_all_sources(self) -> Dict[str, List[str]]:
        """Scrape all sources (Telegram and GitHub) concurrently"""
        tasks = []
        
        for channel in self.telegram_channels:
            tasks.append(self.scrape_telegram_channel(channel))
            
        if self.github_urls:
            tasks.append(self.scrape_github_urls(self.github_urls))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        combined_links = {"vmess": [], "vless": [], "ss": [], "trojan": [], "ssr": []}

        for result in results:
            if isinstance(result, dict):
                for protocol in combined_links:
                    combined_links[protocol].extend(result[protocol])
            elif isinstance(result, Exception):
                logger.error(f"Task failed with exception: {result}")

        # Final global deduplication across all sources
        combined_links = self.duplicate_checker.deduplicate_links(combined_links)

        return combined_links


def download_channels_list(channels_url: str) -> str:
    """Download and decode channels list from URL"""
    try:
        logger.info(f"Downloading channels from: {channels_url}")
        response = requests.get(channels_url, timeout=30)
        response.raise_for_status()
        
        # Decode base64 content
        decoded_content = base64.b64decode(response.content).decode('utf-8')
        
        # Save to temporary file
        with open('channels.txt', 'w') as f:
            f.write(decoded_content)
            
        lines_count = len(decoded_content.strip().split('\n'))
        logger.info(f"Downloaded and decoded {lines_count} lines to channels.txt")
        return 'channels.txt'
        
    except Exception as e:
        logger.error(f"Error downloading channels list: {e}")
        raise


async def run_scraper():
    """Main scraping function"""
    try:
        # Get environment variables
        namira_xapi = os.getenv('NAMIRA_XAPI')
        namira_url = os.getenv('NAMIRA_URL', 'http://localhost:8080')
        channels_url = os.getenv('CHANNELS_URL', 
                                'https://raw.githubusercontent.com/NaMiraNet/ChanExt/refs/heads/main/results/channels_latest.txt')
        github_input = os.getenv('GITHUB_INPUT')
        test_timeout = int(os.getenv('TEST_TIMEOUT', '15'))
        github_rate_limit = float(os.getenv('GITHUB_RATE_LIMIT', '1.0'))

        logger.info(f"V2Crawler Service {__version__} starting on Railway...")

        # Download channels list
        channels_file = download_channels_list(channels_url)
        telegram_channels = read_channels_from_file(channels_file)
        logger.info(f"Telegram channels to scrape: {len(telegram_channels)}")

        github_urls = []
        if github_input:
            github_urls = read_github_urls_from_file(github_input)
            logger.info(f"GitHub URLs to scrape: {len(github_urls)}")

        # Initialize components with unified scraper
        scraper = UnifiedChannelScraper(
            telegram_channels=telegram_channels,
            github_urls=github_urls,
            github_rate_limit=github_rate_limit
        )
        link_manager = manager.LinkManager('vpn_links.json')
        namira = NamiraInterface(namira_xapi, namira_url) # type: ignore
        
        logger.info("Starting scraping from all sources...")
        links = await scraper.scrape_all_sources()

        total_links = sum(len(v) for v in links.values())
        logger.info(f"Total valid links found: {total_links}")

        if total_links == 0:
            logger.warning("No valid links found. Check your input sources and try again.")
            return

        logger.info("Saving validated links...")
        metadata = {
            "telegram_channels": telegram_channels,
            "github_urls": github_urls,
            "scraping_timestamp": datetime.now().isoformat(),
            "version": __version__,
            "validation_enabled": True,
            "source_types": ["telegram", "github"] if telegram_channels and github_urls else 
                           ["telegram"] if telegram_channels else ["github"],
            "environment": "railway"
        }
        link_manager.save_links(links, metadata)

        logger.info("Exporting links for testing...")
        link_manager.export_for_testing(links)

        logger.info("Scraping completed successfully!")
        await namira.send_links(links)        
        # Print summary
        logger.info("=== SCRAPING SUMMARY ===")
        for protocol, link_list in links.items():
            if link_list:
                logger.info(f"{protocol.upper()}: {len(link_list)} links")
        
        logger.info(f"Total sources: {len(telegram_channels) + len(github_urls)}")
        logger.info(f"Telegram channels: {len(telegram_channels)}")
        logger.info(f"GitHub repositories: {len(github_urls)}")

    except Exception as e:
        logger.error(f"Scraping process error: {e}")
        logger.error(traceback.format_exc())


def run_scheduler():
    """Run the scheduler in a separate thread"""
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


async def main():
    """Main function for Railway deployment"""
    # Get scheduling configuration from environment
    schedule_enabled = os.getenv('SCHEDULE_ENABLED', 'true').lower() == 'true'
    schedule_interval = int(os.getenv('SCHEDULE_INTERVAL_MINUTES', '10'))
    run_immediately = os.getenv('RUN_IMMEDIATELY', 'true').lower() == 'true'

    logger.info(f"Railway V2Crawler Service {__version__}")
    logger.info(f"Schedule enabled: {schedule_enabled}")
    logger.info(f"Schedule interval: {schedule_interval} hours")
    logger.info(f"Run immediately: {run_immediately}")

    if run_immediately:
        logger.info("Running scraper immediately...")
        await run_scraper()

    loop = asyncio.get_running_loop()
    if schedule_enabled:
        logger.info(f"Setting up scheduler to run every {schedule_interval} minutes")
        schedule.every(schedule_interval).minutes.do(lambda: asyncio.run_coroutine_threadsafe(run_scraper(), loop)
        )
        # Start scheduler in background thread
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("Scheduler started in background thread")

        # Keep the main process alive
        try:
            while True:
                await asyncio.sleep(60)
                logger.info("Service running... (Railway keepalive)")
        except KeyboardInterrupt:
            logger.info("Service stopped by user")
    else:
        logger.info("Scheduling disabled. Service will exit after initial run.")


if __name__ == "__main__":
    # Check if running in Railway environment
    if os.getenv('RAILWAY_ENVIRONMENT'):
        logger.info("Detected Railway environment")
        asyncio.run(main())
    else:
        # Fallback to original CLI interface for local development
        parser = argparse.ArgumentParser(
            description="V2Crawler - V2Ray Link Crawler"
        )

        parser.add_argument(
            "-t", "--telegram-input", 
            help="List of Telegram channels to scrape"
        )
        parser.add_argument(
            "-g", "--github-input",
            help="List of GitHub URLs to scrape for raw content"
        )
        parser.add_argument(
            "-o", "--output", 
            default="vpn_links.json", 
            help="Output file for scraped links"
        )
        parser.add_argument(
            "-u", "--namira-url",
            default="http://localhost:8080",
            help="URL of the namira service"
        )
        parser.add_argument(
            "-x", "--namira-xapi", 
            default="namira-xapi", 
            help="X-API key for namira service"
        )
        parser.add_argument(
            "-T", "--test-timeout",
            type=int,
            default=10,
            help="Timeout for link testing in seconds"
        )
        parser.add_argument(
            "-d", "--debug", 
            action="store_true", 
            help="Enable debug logging"
        )
        parser.add_argument(
            "-r", "--github-rate-limit",
            type=float,
            default=1.0,
            help="Rate limit delay for GitHub requests (seconds)"
        )

        args = parser.parse_args()

        if args.debug:
            logging.getLogger().setLevel(logging.DEBUG)

        if not args.telegram_input and not args.github_input:
            logger.error("At least one input source (--telegram-input or --github-input) must be provided")
            exit(1)

        telegram_channels = []
        github_urls = []
        
        if args.telegram_input:
            telegram_channels = read_channels_from_file(args.telegram_input)
            logger.info(f"Telegram channels to scrape: {telegram_channels}")
        
        if args.github_input:
            github_urls = read_github_urls_from_file(args.github_input)
            logger.info(f"GitHub URLs to scrape: {github_urls}")

        async def run_local():
            scraper = UnifiedChannelScraper(
                telegram_channels=telegram_channels,
                github_urls=github_urls,
                github_rate_limit=args.github_rate_limit
            )
            link_manager = manager.LinkManager(args.output)
            namira = NamiraInterface(args.namira_xapi, args.namira_url)
            try:
                logger.info("Starting scraping from all sources...")
                links = await scraper.scrape_all_sources()

                total_links = sum(len(v) for v in links.values())
                logger.info(f"Total valid links found: {total_links}")

                if total_links == 0:
                    logger.warning("No valid links found. Check your input sources and try again.")
                    return

                logger.info("Saving validated links...")
                metadata = {
                    "telegram_channels": telegram_channels,
                    "github_urls": github_urls,
                    "scraping_timestamp": datetime.now().isoformat(),
                    "version": __version__,
                    "validation_enabled": True,
                    "source_types": ["telegram", "github"] if telegram_channels and github_urls else 
                                   ["telegram"] if telegram_channels else ["github"]
                }
                link_manager.save_links(links, metadata)

                logger.info("Exporting links for testing...")
                link_manager.export_for_testing(links)

                logger.info("Scraping completed successfully!")

                logger.info("Sending links to namira service")
                await namira.send_links(links)

                # Print summary
                logger.info("=== SCRAPING SUMMARY ===")
                for protocol, link_list in links.items():
                    if link_list:
                        logger.info(f"{protocol.upper()}: {len(link_list)} links")
                
                logger.info(f"Total sources: {len(telegram_channels) + len(github_urls)}")
                logger.info(f"Telegram channels: {len(telegram_channels)}")
                logger.info(f"GitHub repositories: {len(github_urls)}")

            except Exception as e:
                logger.error(f"Main process error: {e}")
                logger.error(traceback.format_exc())
                raise

        asyncio.run(run_local())