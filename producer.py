import requests
import time
import logging
import sys
import uuid

# --- Configuration ---
# !! IMPORTANT !!
# This list MUST contain the addresses of ALL your brokers.
# For your 4-system setup, this will be the IPs and ports of Node 1 and Node 2.
# Example: ["http://192.168.1.10:5001", "http://192.168.1.11:5001"]
# For local testing, you can run two brokers on different ports:
BROKER_URLS = [
    "http://localhost:5001", 
    "http://localhost:5002" # Assuming you run a second broker on port 5002
]

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
        # Use a session for connection pooling and performance
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        logger.info(f"Producer initialized with brokers: {broker_urls}")

    def _get_leader_id(self):
        """
        Helper function to ask any broker who the current leader is.
        It iterates through all known brokers until one responds.
        """
        for broker_url in self.broker_urls:
            try:
                response = self.session.get(f"{broker_url}/metadata/leader", timeout=3)
                if response.status_code == 200:
                    leader_id = response.json().get("leader_id")
                    logger.info(f"Broker {broker_url} reports leader is: {leader_id}")
                    return leader_id
                elif response.status_code == 404:
                    # This is fine, just means no leader is elected yet
                    logger.warning(f"Broker {broker_url} reports no leader elected (404).")
                    
            except requests.RequestException as e:
                logger.warning(f"Could not contact broker {broker_url}: {e}")
        
        logger.error("Failed to contact any broker to find leader.")
        return None

    def _find_leader_url_by_id(self, leader_id):
        """
        Helper function to map a leader_id (e.g., "broker-node1") 
        to a full URL (e.g., "http://192.168.1.10:5001").
        
        It does this by checking the /health endpoint of every broker.
        """
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
        self.current_leader_url = None # Invalidate current leader
        return False

    def discover_leader(self):
        """
        Finds the current leader and sets it for the producer.
        This is a two-step process:
        1. Get the *ID* of the leader (e.g., "broker-hostname")
        2. Find the *URL* that corresponds to that ID.
        """
        logger.info("Discovering leader...")
        leader_id = self._get_leader_id()
        if leader_id:
            return self._find_leader_url_by_id(leader_id)
        
        logger.error("Leader discovery failed.")
        return False

    def send(self, message_data, max_retries=3):
        """
        Sends a message to the leader, handling failover and retries.
        
        Args:
            message_data (dict): The JSON-serializable data to send.
            max_retries (int): Number of times to retry before failing.
        """
        
        # Add a unique ID for idempotency (helps the broker prevent duplicates)
        payload = {
            "msg_id": str(uuid.uuid4()),
            "data": message_data
        }
        
        # As requested, requests handles converting the dict to a JSON string
        # and sending it as bytes with the correct Content-Type.
        
        for attempt in range(max_retries):
            try:
                # 1. Ensure we have a leader
                if not self.current_leader_url:
                    if not self.discover_leader():
                        logger.warning("No leader found. Retrying after delay...")
                        time.sleep(3)
                        continue
                
                logger.debug(f"Attempt {attempt+1}: Sending to {self.current_leader_url}")
                
                # 2. Try to send the message
                response = self.session.post(
                    f"{self.current_leader_url}/produce",
                    json=payload,
                    timeout=5
                )

                # 3. Handle responses
                if response.status_code == 200:
                    logger.info(f"Successfully sent message. Offset: {response.json().get('offset')}")
                    return response.json()
                
                elif response.status_code == 307:
                    # "Not the leader" redirect. This is our "soft failover".
                    leader_id = response.json().get("leader_id")
                    logger.warning(f"Broker {self.current_leader_url} is not leader. New leader is {leader_id}.")
                    self._find_leader_url_by_id(leader_id)
                    # Continue to the next attempt in the loop (will retry)
                    
                elif response.status_code == 503:
                    # "No leader elected yet".
                    logger.warning("Broker reports 503 Service Unavailable (no leader). Retrying...")
                    time.sleep(2)
                    # Continue to the next attempt in the loop (will retry)
                    
                else:
                    # Any other HTTP error
                    logger.error(f"HTTP Error {response.status_code}: {response.text}")
                    response.raise_for_status() # Raise exception to trigger hard failover

            except requests.RequestException as e:
                # "Hard failover" - Connection error, timeout, etc.
                logger.error(f"Leader at {self.current_leader_url} is unreachable: {e}")
                logger.warning("Marking leader as dead. Discovering new leader...")
                self.current_leader_url = None # Invalidate leader to force rediscovery
                time.sleep(1) # Give system a moment to elect new leader
                # Continue to the next attempt in the loop
            
            time.sleep(1) # Backoff before retrying

        raise Exception(f"Failed to send message after {max_retries} retries.")

# --- Main execution ---
if __name__ == "__main__":
    logger.info("Starting YAK Producer...")
    
    # Create the producer. It will find the leader on its first send.
    producer = Producer(broker_urls=BROKER_URLS)
    
    # First, explicitly discover the leader
    while not producer.discover_leader():
        logger.info("Waiting for a leader to be elected...")
        time.sleep(3)
    
    logger.info(f"Initial leader found: {producer.current_leader_url}")
    
    # Send 20 messages, one per second
    for i in range(1, 21):
        message = {
            "message_number": i,
            "content": f"This is message {i}."
        }
        
        try:
            logger.info(f"Sending message {i}...")
            result = producer.send(message)
            
        except Exception as e:
            logger.error(f"Failed to send message {i}: {e}")
            logger.error("Stopping producer.")
            sys.exit(1)
            
        time.sleep(1)
        
        # --- TEST FAILOVER HERE ---
        # After message 5, go to the terminal running the LEADER broker
        # and press Ctrl+C to kill it.
        # You should see the producer pause, log errors, find the
        # new leader, and continue sending messages 6, 7, 8...
        if i == 5:
            logger.warning("---")
            logger.warning("TESTING FAILOVER: Please kill the leader broker NOW.")
            logger.warning("Producer will pause for 10 seconds to give you time.")
            logger.warning("---")
            time.sleep(10)
            
    logger.info("Successfully sent 20 messages.")
