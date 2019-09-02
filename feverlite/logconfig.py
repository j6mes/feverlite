import logging
import os

def setup():
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"),
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
