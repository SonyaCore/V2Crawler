from typing import List

def read_channels_from_file(file_path):
    """Read Telegram channels from a text file, one channel per line."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            channels = [line.strip() for line in f if line.strip()]
        return channels
    except FileNotFoundError:
        print(f"Error: Input file '{file_path}' not found.")
        return []
    except Exception as e:
        print(f"Error reading input file: {e}")
        return []

def read_github_urls_from_file(filepath: str) -> List[str]:
    """Read GitHub URLs from file, one per line"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        return urls
    except Exception as e:
        print(f"Error reading GitHub URLs from {filepath}: {e}")
        return []