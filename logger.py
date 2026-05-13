import logging
import sys

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('iptv_collector.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)