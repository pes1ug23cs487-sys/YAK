YAK: Yet Another Kafka

A high-performance, resilient message broker inspired by the core mechanics of Apache Kafka. This project is a deep dive into distributed systems, focusing on fault tolerance, consensus, and data replication.

🚀 Project Overview

The goal of this project is to build a fault-tolerant message broker system that can withstand a catastrophic leader failure with zero data loss.

The system is composed of four main components:

Leader Broker: The single authority for all data writes.

Follower Broker: A hot-standby, replicating all data from the leader and ready to take over instantly.

Metadata Store (Redis): The "source of truth" that manages consensus, storing the leader lease and the High Water Mark (HWM).

Intelligent Clients (Producer/Consumer): Clients that can automatically discover the leader and seamlessly failover in the event of a crash.

🏗️ Architecture

The system uses a Leader-Follower model with synchronous replication. A message is not considered "committed" until it is safely replicated from the Leader to the Follower.

Leader Election: A lease-based mechanism in Redis (SETNX) ensures only one broker can be the leader at any time.

Data Replication: The Leader synchronously POSTs all new messages to the Follower's /internal/replicate endpoint.

High Water Mark (HWM): The HWM (stored in Redis) is only advanced after the Leader receives an ACK from the Follower. This prevents consumers from reading un-replicated data.

Client Failover: Clients fetch the current leader's identity from a /metadata/leader endpoint. If they receive a connection error or a "Not the Leader" response, they automatically query this endpoint again to find the new leader and retry their operation.

✅ Core Features

Atomic Leader Election using a Redis-based lease.

Synchronous Data Replication to guarantee durability.

High Water Mark (HWM) enforcement for consumers.

Seamless Client Failover for producers and consumers.

🔧 How to Run

This guide covers running the broker.py and producer.py on a local machine for testing.

1. Prerequisites

Python 3.10+

Redis (running on localhost:6379)

Python packages: flask, redis, requests

2. Setup (Recommended)

It is highly recommended to use a Python virtual environment to manage dependencies.

# 1. Create a virtual environment
python3 -m venv yak_env

# 2. Activate the environment
source yak_env/bin/activate

# 3. Install required packages
pip install flask redis requests


3. Run the System

You will need at least three terminal windows open.

Terminal 1: Run the Leader Broker

This broker will start up, acquire the leader lease, and listen on port 5001.

# Make sure your yak_env is active
python broker.py


(You may need to modify broker.py to change the port to 5001 if it's not the default)

Terminal 2: Run the Follower Broker

(Currently, this uses the same broker.py script. We modify the port via an environment variable or script argument in a real setup. For now, we'll assume you've modified broker.py to run on port 5002 if it's on the same machine).

# Make sure your yak_env is active
# (This assumes you've modified the PORT in broker.py to 5002)
python broker.py 


You will see this broker attempt to get the lease, fail (which is correct), and enter a follower state.

Terminal 3: Run the Intelligent Producer

This client will start, find the leader (Broker 1), and begin sending messages.

# Make sure your yak_env is active
python producer.py


💥 How to Test Failover

This is the most important test:

Have all three terminals (Leader, Follower, Producer) running.

The Producer will start sending messages 1, 2, 3... to the Leader (Broker 1).

After message 5, the producer will pause and ask you to kill the leader.

Go to Terminal 1 (Leader Broker) and press Ctrl+C to stop the process.

Observe the other terminals:

Terminal 2 (Follower): Will log that the lease has expired and that it is "Attempting leader election" and has "Successfully became the leader."

Terminal 3 (Producer): Will log a ConnectionError. It will then log "Discovering new leader..." and find the broker on port 5002. It will then continue sending messages 6, 7, 8... to the new leader.

This test proves the system is resilient to a leader crash with zero data loss.

📁 Component Files

broker.py: The main broker application (runs as Leader or Follower). Handles leader election, lease renewal, and data replication.

producer.py: The "intelligent client" that sends messages, discovers the leader, and handles automatic failover.