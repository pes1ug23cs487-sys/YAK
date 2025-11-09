import requests
import time
import logging
import sys

# --- Configuration ---
# !!! IMPORTANT !!!
# These MUST be the ZeroTier IPs. 'localhost' will not work.
# The producer teammate must edit these.
BROKER_URLS = [
    "http://192.168.191.203:5001",
    "http://192.168.191.242:5002"
]

# Topic Configuration (Kept for the web server to import)
TOPIC_CPU = "topic-cpu"
TOPIC_MEM = "topic-mem"
TOPIC_NET = "topic-net"
TOPIC_DISK = "topic-disk"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Producer:
    """
    An intelligent producer for YAK (Yet Another Kafka) that handles
    leader discovery and automatic failover.
    """
    def __init__(self, broker_urls):
        self.broker_urls = broker_urls
        self.current_leader_url = None
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        logger.info(f"Producer initialized with brokers: {broker_urls}")
        self.discover_leader()

    def _get_leader_id(self):
        for broker_url in self.broker_urls:
            try:
                response = self.session.get(f"{broker_url}/metadata/leader", timeout=3)
                if response.status_code == 200:
                    leader_id = response.json().get("leader_id")
                    logger.info(f"Broker {broker_url} reports leader is: {leader_id}")
                    return leader_id
                elif response.status_code == 404:
                    logger.warning(f"Broker {broker_url} reports no leader elected (404).")
            except requests.RequestException as e:
                logger.warning(f"Could not contact broker {broker_url}: {e}")
        logger.error("Failed to contact any broker to find leader.")
        return None

    def _find_leader_url_by_id(self, leader_id):
        if not leader_id:
            return False
        for broker_url in self.broker_urls:
            try:
                response = self.session.get(f"{broker_url}/health", timeout=3)
                if response.status_code == 200:
                    broker_id = response.json().get("broker_id")
                    if broker_id == leader_id:
                        logger.info(f"Leader {leader_id} found at URL: {broker_url}")
                        self.current_leader_url = broker_url
                        return True
            except requests.RequestException as e:
                logger.warning(f"Could not contact broker {broker_url} for health check: {e}")
        logger.error(f"Could not find a broker URL matching leader_id: {leader_id}")
        self.current_leader_url = None
        return False

    def discover_leader(self):
        logger.info("Discovering leader...")
        leader_id = self._get_leader_id()
        if leader_id:
            return self._find_leader_url_by_id(leader_id)
        logger.error("Leader discovery failed.")
        self.current_leader_url = None
        return False

    def send(self, msg_id, message_data, max_retries=3):
        """
        Sends a message with a PRE-DEFINED msg_id for idempotence.
        
        :param msg_id: A unique, deterministic ID for this message.
        :param message_data: The dict {"topic": ..., "key": ..., "payload": ...}
        :param max_retries: Number of retries on failure.
        """
        
        payload = {
            "msg_id": str(msg_id), # We use the provided msg_id
            "data": message_data
        }
        
        for attempt in range(max_retries):
            try:
                if not self.current_leader_url:
                    if not self.discover_leader():
                        logger.warning("No leader found. Retrying after delay...")
                        time.sleep(3)
                        continue
                
                response = self.session.post(
                    f"{self.current_leader_url}/produce",
                    json=payload,
                    timeout=5
                )

                if response.status_code == 200:
                    return response.json()
                
                elif response.status_code == 307 or response.status_code == 400:
                    if "leader_id" in response.json():
                        leader_id = response.json().get("leader_id")
                        logger.warning(f"Broker {self.current_leader_url} is not leader. New leader is {leader_id}.")
                        self._find_leader_url_by_id(leader_id)
                    else:
                        logger.error(f"Broker error: {response.text}")
                        time.sleep(1)
                        
                elif response.status_code == 503:
                    logger.warning("Broker reports 503 Service Unavailable (no leader). Retrying...")
                    time.sleep(2)
                    
                else:
                    logger.error(f"HTTP Error {response.status_code}: {response.text}")
                    response.raise_for_status()

            except requests.RequestException as e:
                logger.error(f"Leader at {self.current_leader_url} is unreachable: {e}")
                logger.warning("Marking leader as dead. Discovering new leader...")
                self.current_leader_url = None
                time.sleep(1)
            
            time.sleep(1)

        raise Exception(f"Failed to send message after {max_retries} retries.")