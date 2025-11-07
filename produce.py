import requests
import time
import logging
import sys
import uuid
import pandas as pd  # <-- Import pandas
import itertools   # <-- Import itertools to cycle the dataset
import json        # <-- Import json for proper data handling

# --- Configuration ---
BROKER_URLS = [
    "http://localhost:5001", 
    "http://192.168.191.242:5002"
]

DATASET_FILE = 'dataset (1).csv' # <-- Path to your dataset

# --- New Topic and Alert Configuration ---
TOPIC_METRICS = "server_metrics"
TOPIC_ALERTS = "system_alerts"
CPU_ALERT_THRESHOLD = 90.0 # Alert if CPU usage is over 90%


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Producer:
    """
    An intelligent producer for YAK (Yet Another Kafka) that handles
    leader discovery and automatic failover.
    (This class is 100% UNCHANGED from your file)
    """
    def __init__(self, broker_urls):
        self.broker_urls = broker_urls
        self.current_leader_url = None
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        logger.info(f"Producer initialized with brokers: {broker_urls}")

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
        return False

    def send(self, message_data, max_retries=3):
        payload = {
            "msg_id": str(uuid.uuid4()),
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
                    json=payload, # requests handles the dict -> json bytes
                    timeout=5
                )

                if response.status_code == 200:
                    logger.info(f"Successfully sent message. Offset: {response.json().get('offset')}")
                    return response.json()
                
                elif response.status_code == 307:
                    leader_id = response.json().get("leader_id")
                    logger.warning(f"Broker {self.current_leader_url} is not leader. New leader is {leader_id}.")
                    self._find_leader_url_by_id(leader_id)
                    
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

# --- Main execution (MODIFIED to read from CSV) ---
if __name__ == "__main__":
    logger.info("Starting YAK Producer...")
    
    producer = Producer(broker_urls=BROKER_URLS)
    
    # Discover the leader at startup
    while not producer.discover_leader():
        logger.info("Waiting for a leader to be elected...")
        time.sleep(3)
    
    logger.info(f"Initial leader found: {producer.current_leader_url}")
    
    logger.info(f"Reading dataset from {DATASET_FILE}...")
    try:
        # Read the entire CSV into memory.
        # This is fine for this dataset, but for huge files, you'd read in chunks.
        df = pd.read_csv(DATASET_FILE)
        # Convert dataframe to a list of dictionaries (one per row)
        records = df.to_dict('records')
        logger.info(f"Successfully loaded {len(records)} records. Starting infinite stream...")
        
        # Use itertools.cycle to loop over the dataset forever
        record_stream = itertools.cycle(records)
        
    except FileNotFoundError:
        logger.error(f"FATAL: Dataset file not found at {DATASET_FILE}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"FATAL: Failed to read CSV: {e}")
        sys.exit(1)

    # --- This is the new streaming loop ---
    try:
        for row in record_stream:
            # `row` is now a dictionary like:
            # {'ts': '20:53:00', 'server_id': 'server_1', 'cpu_pct': 63.34, ...}
            
            # --- This is your "Topic" and "Key" logic ---
            key = row.get('server_id')
            topic = TOPIC_METRICS # Default topic
            
            # Create the final message payload
            message = {
                "topic": topic,
                "key": key,
                "payload": row  # Send the full CSV row as the payload
            }
            
            # --- Logic to create a "variety of topics" ---
            # Also send a separate "alert" message if CPU is high
            try:
                if float(row.get('cpu_pct', 0)) > CPU_ALERT_THRESHOLD:
                    topic = TOPIC_ALERTS
                    logger.warning(f"HIGH CPU DETECTED on {key}! Sending to {topic} topic.")
                    
                    alert_message = {
                        "topic": topic,
                        "key": key,
                        "payload": {
                            "server": key,
                            "alert_type": "HIGH_CPU",
                            "cpu_value": row.get('cpu_pct'),
                            "timestamp": row.get('ts')
                        }
                    }
                    # Send the alert
                    producer.send(alert_message)
                    
            except Exception as e:
                logger.error(f"Error processing alert logic: {e}")

            # --- Send the main metrics message ---
            logger.info(f"Sending event (Topic: {message['topic']}, Key: {message['key']})...")
            try:
                result = producer.send(message)
                
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
                logger.error("Producer will keep trying...")
            
            # Stream one record every 0.5 seconds
            time.sleep(0.5) 

    except KeyboardInterrupt:
        logger.info("\nStopping producer...")
        sys.exit(0)