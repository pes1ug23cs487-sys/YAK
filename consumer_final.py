import requests
import time
import json
import os
import sys

class ConsumerClient:
    """
    An intelligent consumer compatible with the PARTITION-AWARE broker.
    This script is designed to:
    1. Discover the leader.
    2. Discover all partitions for a given topic.
    3. Consume from all partitions concurrently.
    4. Manage local offsets for each partition.
    5. Handle leader failover.
    """
    def __init__(self, broker_list, topic, consumer_group="default_group"):
        """
        Initializes the consumer for a specific topic and group.
        """
        if not topic:
            raise ValueError("A topic must be specified.")
            
        self.brokers = broker_list
        self.topic = topic
        self.consumer_group = consumer_group
        
        # --- MODIFIED: Offset Management ---
        # We now store a dictionary of offsets, e.g. {0: 10, 1: 12, 2: 9}
        self.offsets = {}
        self.offset_file = f"{consumer_group}_{topic}_offsets.json" # Store as JSON
        self.messages_file = f"{consumer_group}_{topic}_messages.txt"
        
        self.current_leader = None
        self.session = requests.Session() 
        self.partition_count = 0
        
        print(f"Consumer for group '{consumer_group}' on topic '{topic}' initialized.")
        print(f" > Offset file: {self.offset_file}")
        print(f" > Messages file: {self.messages_file}")

    # --- MODIFIED: Offset Management ---
    def load_offsets(self):
        """
        Loads the last-saved partition offsets from the local JSON file.
        """
        if os.path.exists(self.offset_file):
            try:
                with open(self.offset_file, 'r') as f:
                    # Load offsets as integers
                    self.offsets = {int(k): int(v) for k, v in json.load(f).items()}
                    print(f"Loaded offsets: {self.offsets}")
            except (IOError, ValueError, json.JSONDecodeError) as e:
                print(f"Warning: Could not read offset file: {e}. Starting from 0.")
                self.offsets = {}
        else:
            print("No offset file found. Starting from offset 0 for all partitions.")
            self.offsets = {}

    # --- MODIFIED: Offset Management ---
    def save_offsets(self):
        """
        Stores the current partition offsets to its local JSON file.
        """
        try:
            with open(self.offset_file, 'w') as f:
                json.dump(self.offsets, f)
        except IOError as e:
            print(f"FATAL: Could not write offset file: {e}")
            
    # --- LEADER DISCOVERY (Unchanged, this logic is correct) ---
    def find_leader(self):
        """
        Correctly queries broker /metadata/leader and /health endpoints
        to find the active leader.
        """
        print("Attempting to find leader (2-step discovery)...")
        leader_id = None
        for broker_url in self.brokers:
            try:
                response = self.session.get(f"{broker_url}/metadata/leader", timeout=3)
                if response.status_code == 200:
                    leader_id = response.json().get("leader_id")
                    if leader_id:
                        print(f"Any broker ({broker_url}) reports leader ID is: {leader_id}")
                        break 
                else:
                    print(f"Broker {broker_url} responded {response.status_code} to /metadata/leader")
            except requests.exceptions.RequestException as e:
                print(f"Could not contact {broker_url} for leader ID: {e}")
        
        if not leader_id:
            print("Could not find leader ID from any broker.")
            self.current_leader = None
            return False
            
        for broker_url in self.brokers:
            try:
                response = self.session.get(f"{broker_url}/health", timeout=3)
                if response.status_code == 200:
                    broker_id = response.json().get("broker_id")
                    if broker_id == leader_id:
                        print(f"Leader {leader_id} found at URL: {broker_url}")
                        self.current_leader = broker_url
                        return True
            except requests.exceptions.RequestException as e:
                print(f"Could not contact {broker_url} for /health check: {e}")

        print(f"Found leader ID {leader_id}, but could not find matching broker URL.")
        self.current_leader = None
        return False

    # --- NEW: Partition Discovery ---
    def discover_partitions(self):
        """
        Asks the leader for topic metadata to discover partitions.
        This handles the "Topic does not exist" error by retrying.
        """
        if not self.current_leader:
            print("No leader to discover partitions from.")
            return False
        
        print(f"Discovering partitions for topic '{self.topic}' from leader...")
        try:
            response = self.session.get(f"{self.current_leader}/topics", timeout=3)
            if response.status_code == 200:
                topics_info = response.json().get("topics", [])
                for topic_data in topics_info:
                    if topic_data.get("name") == self.topic:
                        self.partition_count = topic_data.get("partitions", 0)
                        print(f"Discovered {self.partition_count} partitions for topic '{self.topic}'.")
                        
                        # Initialize offsets for any new partitions
                        for i in range(self.partition_count):
                            if i not in self.offsets:
                                self.offsets[i] = 0 # Start from beginning
                        
                        return True
                
                # If we get here, the /topics endpoint worked but our topic isn't listed
                print(f"Warning: Topic '{self.topic}' does not exist on the leader yet. Waiting for producer...")
                return False
                
            else:
                print(f"Error discovering topics: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            print(f"Failed to connect to leader for partition discovery: {e}")
            self.current_leader = None # Force re-find leader
            return False

    # --- MODIFIED: Data Consumption ---
    def consume_from_partition(self, partition_id):
        """
        Consumes messages for a *single* specified partition.
        """
        if not self.current_leader:
            print(f"[P{partition_id}] No leader, skipping consumption.")
            return

        current_offset = self.offsets.get(partition_id, 0)
        
        try:
            print(f"[P{partition_id}] Consuming topic '{self.topic}' at offset {current_offset}...")
            response = self.session.get(
                f"{self.current_leader}/consume",
                params={'topic': self.topic, 'partition': partition_id, 'offset': current_offset},
                timeout=5
            )

            if response.status_code == 200:
                data = response.json()
                messages = data.get('messages', [])
                
                if not messages:
                    print(f"[P{partition_id}] No new messages.")
                    return

                print(f"[P{partition_id}] Received {len(messages)} message(s).")
                
                try:
                    with open(self.messages_file, 'a', encoding='utf-8') as f:
                        for msg in messages:
                            message_content = msg.get('data')
                            msg_offset = msg.get('offset')
                            
                            print(f"  > [P{partition_id}] Processing Offset {msg_offset}: {message_content.get('key')}")
                            f.write(json.dumps(msg) + "\n")
                            
                            # Update our offset to the last one we've processed
                            self.offsets[partition_id] = msg_offset
                
                except IOError as e:
                    print(f"FATAL: Could not write to messages file {self.messages_file}: {e}")
                
            elif response.status_code == 404:
                print(f"[P{partition_id}] Received error: {response.json().get('error')}")
                print("Leader's metadata might be stale. Forcing re-discovery...")
                self.current_leader = None # Force re-find leader
                self.partition_count = 0 # Force re-discover partitions
                self.offsets = {} # Reset offsets
                self.load_offsets() # Reload from file
                
            elif 400 <= response.status_code < 500:
                print(f"[P{partition_id}] Received error from broker: {response.status_code} {response.text}")
                if "Not the leader" in response.text:
                    self.current_leader = None # Force re-find leader
                
            else:
                print(f"[P{partition_id}] Server error: {response.status_code}")
                self.current_leader = None # Assume leader failure

        except requests.exceptions.ConnectionError:
            print(f"\n--- [P{partition_id}] CONNECTION FAILED ---")
            print(f"Leader at {self.current_leader} is down.")
            self.current_leader = None # Force re-find leader
            
        except requests.exceptions.RequestException as e:
            print(f"[P{partition_id}] An unknown request error occurred: {e}")
            self.current_leader = None

    def run(self):
        """
        Main run loop for the consumer.
        """
        self.load_offsets() 
        
        print("\nStarting consumer poll loop... (Press Ctrl+C to stop)")
        try:
            while True:
                # --- State 1: Find Leader ---
                if not self.current_leader:
                    if not self.find_leader():
                        print("No leader found. Retrying in 3s...")
                        time.sleep(3)
                        continue # Restart loop
                
                # --- State 2: Find Partitions ---
                if self.partition_count == 0:
                    if not self.discover_partitions():
                        print("Could not discover partitions (topic might not exist yet). Retrying in 3s...")
                        time.sleep(3)
                        continue # Restart loop
                
                # --- State 3: Consume from all partitions ---
                print(f"Polling {self.partition_count} partitions for '{self.topic}'...")
                for p_id in range(self.partition_count):
                    self.consume_from_partition(p_id)
                
                # Save offsets after polling all partitions
                self.save_offsets()
                
                print("-------------------------")
                time.sleep(3) # Poll every 3 seconds
                
        except KeyboardInterrupt:
            print("\nShutting down consumer...")
            self.save_offsets() # Save one last time
            print("Offsets saved.")
            sys.exit(0)

if __name__ == "__main__":
    
    # !!! IMPORTANT !!!
    # Make sure these are the ZeroTier IPs for the leader and follower nodes
    
    BROKER_NODES = [
        'http://192.168.191.152:5001', 
        'http://192.168.191.242:5002' 
    ]
    
    
    # --- TOPIC SELECTION ---
    # Choose "server_metrics" or "system_alerts"
    CONSUME_TOPIC = "topic-cpu"
    
    print(f"Starting YAK Consumer Client for topic: {CONSUME_TOPIC}")

    consumer = ConsumerClient(
        broker_list=BROKER_NODES,
        topic=CONSUME_TOPIC,
        consumer_group="my-first-group" 
    )
    
    consumer.run()