YAK (Yet Another Kafka) - Intelligent Consumer Client (System 4)

This directory contains the "Intelligent Consumer Client" for the YAK distributed message broker project. This client is designed to run as System 4 in the project architecture.

Overview

This consumer is responsible for reading messages from the YAK broker cluster. It is "intelligent" because it can automatically handle leader failure, which is a core requirement of the project.

Its main features include:

Leader Discovery: Automatically queries the brokers' /metadata/leader endpoint to find the current active leader.

Automatic Failover: If the connection to the current leader is lost (simulating a crash), the consumer will automatically re-run the leader discovery process to find the new leader (the promoted follower).

Offset Tracking: It tracks the last-read message offset in a local file (consumer_offset.txt by default) to ensure it doesn't re-process messages and can resume from where it left off after a restart or failover.

Resilient Polling: Continuously polls the leader's /consume endpoint for new messages.

Requirements

Python 3.x

The Python libraries listed in requirements.txt.

Setup

Install pip (if not already present):
On Debian/Ubuntu-based systems, you may need to install pip first:

sudo apt update
sudo apt install python3-pip


Install Python Dependencies:
Navigate to this directory in your terminal and run:

pip3 install -r requirements.txt


Configuration

Before running the consumer, you must configure it to point to your broker nodes.

Open consumer_client.py in an editor.

Find the if __name__ == "__main__": block at the bottom of the file.

Update BROKER_NODES: Change the IP addresses and ports to match the network addresses of your System 1 (Initial Leader) and System 2 (Initial Follower).

# Example for a real network setup:
BROKER_NODES = [
    '[http://192.168.1.101:8000](http://192.168.1.101:8000)',  # System 1 (Initial Leader)
    '[http://192.168.1.102:8000](http://192.168.1.102:8000)'   # System 2 (Initial Follower)
]


(Optional) Update offset_file: You can change the path where the consumer saves its progress.

consumer = ConsumerClient(
    broker_list=BROKER_NODES,
    offset_file='/home/user/yak_progress.txt' # (Optional) Change this
)


Running the Consumer

Once configured, you can run the client from your terminal on System 4:

python3 consumer_client.py


The consumer will start, attempt to find the leader, and begin polling for messages.
