import requests
import time
import json
import os
import sys

class ConsumerClient:
    """
    An intelligent consumer compatible with the new broker scripts.

    This client:
    1.  Maintains a list of all known broker nodes.
    2.  Actively discovers the LEADER by querying /metadata/leader.
    3.  Tracks its offset locally to ensure "at-least-once" delivery.
    4.  Handles leader failure by re-running leader discovery.
    """
    def __init__(self, broker_list, offset_file='consumer_offset.txt', messages_file='consumed_messages.txt'):
        """
        Initializes the consumer.

        Args:
            broker_list (list): A list of base URLs for all known brokers
            offset_file (str): The local file to persist the last-read offset.
            messages_file (str): The local file to append received messages to.
        """
        self.brokers = broker_list
        self.offset_file = offset_file
        self.messages_file = messages_file
        self.current_leader = None
        self.offset = 0 # <-- CHANGED: Default offset is 0
        self.session = requests.Session() # Use a session for connection pooling
        
        # Consumer will now persist its state between runs.
        # To reset, manually delete consumer_offset.txt and consumed_messages.txt

    def load_offset(self):
        """
        Loads the last-saved offset from the local offset file.
        If the file doesn't exist, starts from offset 0.
        """
        if os.path.exists(self.offset_file):
            try:
                with open(self.offset_file, 'r') as f:
                    self.offset = int(f.read().strip())
                    print(f"Loaded offset: {self.offset}")
            except (IOError, ValueError) as e:
                print(f"Warning: Could not read offset file: {e}. Starting from 0.")
                self.offset = 0 # <-- CHANGED: Default offset is 0
        else:
            print("No offset file found. Starting from offset 0.")
            self.offset = 0 # <-- CHANGED: Default offset is 0

    def save_offset(self):
        """Saves the current offset to the local file."""
        try:
            with open(self.offset_file, 'w') as f:
                # self.offset already holds the *next* offset to read
                f.write(str(self.offset))
        except IOError as e:
            print(f"FATAL: Could not write offset file: {e}")
            
    def find_leader(self):
        """
        Queries brokers to find the current leader using the two-step
        discovery logic from the producer.
        
        1. Get the leader's unique ID (e.g., "broker-hostname").
        2. Find the URL for that ID by checking /health on all brokers.
        
        Returns:
            bool: True if a leader was found, False otherwise.
        """
        print("Attempting to find leader (2-step discovery)...")
        
        # 1. Get the leader's unique ID
        leader_id = None
        for broker_url in self.brokers:
            try:
                response = self.session.get(f"{broker_url}/metadata/leader", timeout=3)
                if response.status_code == 200:
                    leader_id = response.json().get("leader_id")
                    if leader_id:
                        print(f"Any broker ({broker_url}) reports leader ID is: {leader_id}")
                        break # Found the ID, can stop asking
                else:
                    # This handles 404 "No leader elected"
                    print(f"Broker {broker_url} responded {response.status_code} to /metadata/leader")
            except requests.exceptions.RequestException as e:
                print(f"Could not contact {broker_url} for leader ID: {e}")
        
        if not leader_id:
            print("Could not find leader ID from any broker.")
            self.current_leader = None
            return False
            
        # 2. Find the URL for that leader ID
        for broker_url in self.brokers:
            try:
                response = self.session.get(f"{broker_url}/health", timeout=3)
                if response.status_code == 200:
                    broker_id = response.json().get("broker_id")
                    # Check if this broker's ID matches the leader's ID
                    if broker_id == leader_id:
                        print(f"Leader {leader_id} found at URL: {broker_url}")
                        self.current_leader = broker_url
                        return True
                else:
                    print(f"Broker {broker_url} responded {response.status_code} to /health")
            except requests.exceptions.RequestException as e:
                print(f"Could not contact {broker_url} for /health check: {e}")

        print(f"Found leader ID {leader_id}, but could not find matching broker URL.")
        self.current_leader = None
        return False

    def consume_messages(self):
        """
        Attempts to consume a batch of messages from the current leader.
        This is compatible with the new broker's /consume endpoint.
        """
        if not self.current_leader:
            print("No leader known. Attempting to find one.")
            if not self.find_leader():
                # If still no leader, wait before retrying
                return

        try:
            # We send our current offset to tell the broker where we are.
            print(f"Consuming from {self.current_leader} starting at offset {self.offset}...")
            response = self.session.get(
                f"{self.current_leader}/consume",
                params={'offset': self.offset},
                timeout=5
            )

            if response.status_code == 200:
                data = response.json()
                messages = data.get('messages', [])
                
                if not messages:
                    print("No new messages.")
                    return

                print(f"Received {len(messages)} message(s):")
                
                processed_at_least_one = False
                
                try:
                    # Open in 'a' (append) mode
                    with open(self.messages_file, 'a', encoding='utf-8') as f:
                        for msg in messages:
                            if msg['offset'] == self.offset:
                                # This is the exact message we're waiting for
                                
                                # Check if the 'data' field exists and is not empty
                                message_content = msg.get('data')
                                if not message_content: 
                                    print(f"  > Received message at offset {self.offset} but it was blank. Ignoring and retrying.")
                                    # We break here to retry getting the same offset.
                                    # This will "stick" at this offset until a
                                    # non-blank message is received.
                                    break 
                                
                                # If we are here, the message is valid
                                print(f"  > Offset {msg['offset']}: {message_content}")
                                f.write(json.dumps(msg) + "\n")
                                
                                # Increment self.offset to look for the *next* one
                                self.offset += 1 
                                processed_at_least_one = True
                            
                            elif msg['offset'] > self.offset:
                                # We received a future offset. This means the one
                                # we wanted was skipped (e.g., replication failed).
                                # We will STOP processing this batch and ask again
                                # for self.offset (which hasn't changed).
                                print(f"  > Received offset {msg['offset']}, but was waiting for {self.offset}. Stopping batch to retry.")
                                break # Stop processing this batch
                            
                            # else: msg['offset'] < self.offset
                            # This shouldn't happen, but we ignore it if it does.

                    # After the loop, if we processed any messages, save our new offset
                    if processed_at_least_one:
                        self.save_offset() # save_offset() writes the new self.offset

                except IOError as e:
                    print(f"FATAL: Could not write to messages file {self.messages_file}: {e}")
                
            elif 400 <= response.status_code < 500:
                # This handles "Not the leader" or "Invalid offset"
                print(f"Received error from broker: {response.status_code} {response.text}")
                print("Assuming leader has changed. Re-discovering...")
                self.current_leader = None # Force leader re-discovery
                
            else:
                # Handle server errors (500, etc.)
                print(f"Server error at {self.current_leader}: {response.status_code}")
                print("Assuming leader failure. Re-discovering...")
                self.current_leader = None # Force leader re-discovery

        except requests.exceptions.ConnectionError:
            print(f"\n--- CONNECTION FAILED ---")
            print(f"Leader at {self.current_leader} is down.")
            print("Triggering failover: searching for new leader...")
            print("-------------------------\n")
            self.current_leader = None # Force leader re-discovery
            
        except requests.exceptions.RequestException as e:
            print(f"An unknown request error occurred: {e}")
            self.current_leader = None # Force leader re-discovery

    def run(self):
        """
        Main run loop for the consumer.
        """
        self.load_offset() # Load offset from file (or set to 0)
        self.find_leader() # Find initial leader
        
        print("\nStarting consumer poll loop... (Press Ctrl+C to stop)")
        try:
            while True:
                self.consume_messages()
                # Poll every 3 seconds
                time.sleep(3)
        except KeyboardInterrupt:
            print("\nShutting down consumer.")
            sys.exit(0)

if __name__ == "__main__":
    
    # Make sure these match the IPs and Ports of your two broker scripts
    BROKER_NODES = [
        'http://192.168.191.152:5001', # Your original leader
        'http://192.168.191.242:5002'  # Your new follower
    ]
    
    print("Starting YAK Consumer Client")

    consumer = ConsumerClient(
        broker_list=BROKER_NODES,
        offset_file='consumer_offset.txt',
        messages_file='consumed_messages.txt'
    )
    
    consumer.run()