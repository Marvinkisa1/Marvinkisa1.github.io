import logging
import sys

def setup_logger(name=None):
    """
    Configure the root logger so that any module using logging.getLogger(__name__)
    automatically outputs to console and to 'iptv_collector.log'.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Avoid duplicate handlers if already configured
    if root_logger.handlers:
        return logging.getLogger(name) if name else root_logger

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root_logger.addHandler(console)

    # File handler
    try:
        file_handler = logging.FileHandler('iptv_collector.log', encoding='utf-8')
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        print(f"Warning: Could not create log file: {e}")

    return logging.getLogger(name) if name else root_logger