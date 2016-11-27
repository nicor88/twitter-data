import logging
from crate import client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CRATE_HOST = 'localhost:4200'

connection = client.connect(CRATE_HOST)
