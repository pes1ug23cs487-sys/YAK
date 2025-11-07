import os
import json
import time
import threading
from flask import Flask, render_template, request, jsonify
import requests
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BROKER_NODES = [
    'http://192.168.191.152:5001',
    'http://192.168.191.242:5002'
]

# Global state
current_leader = None
session = requests.Session()
subscriptions = {}  # {subscription_id: {topic, partition, offset, messages}}
subscription_counter = 0
running_subscriptions = {}  # {subscription_id: thread}

def discover_leader():
    """Find the current leader broker."""
    global current_leader
    
    # Step 1: Get leader ID
    leader_id = None
    for broker_url in BROKER_NODES:
        try:
            response = session.get(f"{broker_url}/metadata/leader", timeout=3)
            if response.status_code == 200:
                leader_id = response.json().get("leader_id")
                logger.info(f"Broker {broker_url} reports leader is: {leader_id}")
                break
        except requests.RequestException as e:
            logger.warning(f"Could not contact broker {broker_url}: {e}")
    
    if not leader_id:
        return None
    
    # Step 2: Find URL for that leader ID
    for broker_url in BROKER_NODES:
        try:
            response = session.get(f"{broker_url}/health", timeout=3)
            if response.status_code == 200:
                broker_id = response.json().get("broker_id")
                if broker_id == leader_id:
                    current_leader = broker_url
                    logger.info(f"Leader found at: {broker_url}")
                    return broker_url
        except requests.RequestException as e:
            logger.warning(f"Could not contact broker {broker_url}: {e}")
    
    return None

def poll_messages(subscription_id):
    """Background thread to poll messages for a subscription."""
    sub = subscriptions[subscription_id]
    
    while subscription_id in running_subscriptions:
        try:
            if not current_leader:
                discover_leader()
            
            if not current_leader:
                time.sleep(3)
                continue
            
            response = session.get(
                f"{current_leader}/consume",
                params={
                    'topic': sub['topic'],
                    'partition': sub['partition'],
                    'offset': sub['offset']
                },
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                messages = data.get('messages', [])
                
                if messages:
                    for msg in messages:
                        sub['messages'].append(msg)
                        sub['offset'] = msg['offset']
                    
                    # Keep only last 100 messages
                    if len(sub['messages']) > 100:
                        sub['messages'] = sub['messages'][-100:]
                    
                    logger.info(f"Subscription {subscription_id}: Received {len(messages)} messages")
            
            elif response.status_code == 404:
                logger.warning(f"Topic {sub['topic']} or partition {sub['partition']} not found")
            
            elif response.status_code == 400:
                # Leader changed
                discover_leader()
            
            time.sleep(2)  # Poll every 2 seconds
            
        except requests.RequestException as e:
            logger.error(f"Error polling messages: {e}")
            discover_leader()
            time.sleep(3)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(3)

@app.route('/')
def index():
    """Serve the consumer UI."""
    return render_template('consumer.html')

@app.route('/api/leader', methods=['GET'])
def get_leader():
    """Get current leader information."""
    leader = discover_leader()
    if leader:
        return jsonify({"leader_url": leader, "status": "connected"})
    return jsonify({"error": "No leader found", "status": "disconnected"}), 503

@app.route('/api/topics', methods=['GET'])
def get_topics():
    """Get list of available topics from the broker."""
    if not current_leader:
        discover_leader()
    
    if not current_leader:
        return jsonify({"error": "No leader available"}), 503
    
    try:
        response = session.get(f"{current_leader}/topics", timeout=3)
        if response.status_code == 200:
            return jsonify(response.json())
        return jsonify({"topics": []})
    except requests.RequestException as e:
        logger.error(f"Failed to get topics: {e}")
        return jsonify({"topics": []})

@app.route('/api/subscribe', methods=['POST'])
def subscribe():
    """Subscribe to a topic and partition."""
    global subscription_counter
    
    data = request.get_json()
    topic = data.get('topic')
    partition = data.get('partition', 0)
    
    if not topic:
        return jsonify({"error": "Topic is required"}), 400
    
    subscription_id = f"sub_{subscription_counter}"
    subscription_counter += 1
    
    subscriptions[subscription_id] = {
        'topic': topic,
        'partition': partition,
        'offset': 0,
        'messages': [],
        'created_at': time.time()
    }
    
    # Start polling thread
    thread = threading.Thread(target=poll_messages, args=(subscription_id,), daemon=True)
    running_subscriptions[subscription_id] = thread
    thread.start()
    
    logger.info(f"Created subscription {subscription_id} for {topic}:p{partition}")
    
    return jsonify({
        "subscription_id": subscription_id,
        "topic": topic,
        "partition": partition,
        "status": "active"
    })

@app.route('/api/subscriptions', methods=['GET'])
def get_subscriptions():
    """Get all active subscriptions."""
    subs = []
    for sub_id, sub_data in subscriptions.items():
        subs.append({
            'id': sub_id,
            'topic': sub_data['topic'],
            'partition': sub_data['partition'],
            'offset': sub_data['offset'],
            'message_count': len(sub_data['messages']),
            'status': 'active' if sub_id in running_subscriptions else 'stopped'
        })
    return jsonify({"subscriptions": subs})

@app.route('/api/messages/<subscription_id>', methods=['GET'])
def get_messages(subscription_id):
    """Get messages for a subscription."""
    if subscription_id not in subscriptions:
        return jsonify({"error": "Subscription not found"}), 404
    
    sub = subscriptions[subscription_id]
    return jsonify({
        "subscription_id": subscription_id,
        "topic": sub['topic'],
        "partition": sub['partition'],
        "offset": sub['offset'],
        "messages": sub['messages'][-50:]  # Return last 50 messages
    })

@app.route('/api/unsubscribe/<subscription_id>', methods=['DELETE'])
def unsubscribe(subscription_id):
    """Unsubscribe from a topic."""
    if subscription_id not in subscriptions:
        return jsonify({"error": "Subscription not found"}), 404
    
    # Stop the polling thread
    if subscription_id in running_subscriptions:
        del running_subscriptions[subscription_id]
    
    # Remove subscription
    del subscriptions[subscription_id]
    
    logger.info(f"Removed subscription {subscription_id}")
    
    return jsonify({"status": "unsubscribed"})

if __name__ == '__main__':
    logger.info("Starting Consumer Web UI on port 5004...")
    app.run(host='0.0.0.0', port=5004, debug=False)