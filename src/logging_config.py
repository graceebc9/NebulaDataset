# src/utils/logging_config.py
import logging
import argparse
from pathlib import Path

def setup_logging(default_level='INFO'):
    parser = argparse.ArgumentParser()
    parser.add_argument('--log',
                       default=default_level,
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       help='Set the logging level')
    args = parser.parse_args()
    
    logging.basicConfig(
        level=getattr(logging, args.log),
        format='%(asctime)s - %(pathname)s:%(lineno)d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def get_logger(name):
    return logging.getLogger(name)