import os
import pandas as pd
from flask import Flask, request, jsonify, render_template, send_from_directory
import logging
import uuid
import json

# Import from your refactored producer.py
from producer import (
    Producer, 
    BROKER_URLS, 
    logger, 
    TOPIC_CPU, 
    TOPIC_MEM, 
    TOPIC_NET, 
    TOPIC_DISK
)

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# --- NEW: Create a unique ID for this server instance ---
# This ID will change every time the app is restarted.
APP_SESSION_ID = str(uuid.uuid4())
# --- End of new logic ---

# Create upload folder if it doesn't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Create a single, shared producer instance
logger.info("Initializing global YAK producer...")
producer = Producer(broker_urls=BROKER_URLS)

# --- Create a stable UUIDv5 namespace for our project ---
YAK_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, 'yak.my-project.com')
logger.info(f"Deterministic ID namespace created: {YAK_NAMESPACE}")
logger.info(f"This App Session ID: {APP_SESSION_ID}")


# --- Web Page Route ---

@app.route('/')
def index():
    """Serves the index.html file."""
    return send_from_directory('.', 'index.html')

# --- API Endpoints (for the JavaScript) ---

# --- NEW: Endpoint for clients to check the app session ---
@app.route('/api/session', methods=['GET'])
def get_session():
    """API for the client to check if the server has restarted."""
    return jsonify({"session_id": APP_SESSION_ID})
# --- End of new logic ---

@app.route('/api/leader', methods=['GET'])
def get_leader():
    """API for the status indicator to check leader connection."""
    if not producer.current_leader_url:
        producer.discover_leader()
        
    if producer.current_leader_url:
        return jsonify({"leader_url": producer.current_leader_url})
    else:
        return jsonify({"leader_url": None}), 404

@app.route('/api/send', methods=['POST'])
def handle_send():
    """API for the 'Send Single Message' card."""
    data = request.get_json()
    if not data or 'topic' not in data or 'payload' not in data:
        return jsonify({"error": "Missing 'topic' or 'payload'"}), 400

    try:
        topic = data.get('topic')
        key = data.get('key')
        payload = data.get('payload')

        # --- Create deterministic ID ---
        payload_str = json.dumps(payload, sort_keys=True)
        stable_string = f"{topic}:{key}:{payload_str}"
        msg_id = uuid.uuid5(YAK_NAMESPACE, stable_string)
        # --- End of new logic ---

        message_data = {
            "topic": topic,
            "key": key,
            "payload": payload
        }
        
        result = producer.send(msg_id, message_data)
        logger.info(f"Sent single message to {topic}. ID: {msg_id}. Result: {result}")
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Failed to send single message: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/upload', methods=['POST'])
def handle_upload():
    """API for the 'Upload & Send File' card. Reads the CSV."""
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file and file.filename.endswith('.csv'):
        try:
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(filepath)
            
            df = pd.read_csv(filepath)
            
            # CRITICAL: Sort data just like in your original script
            df['server_num'] = df['server_id'].str.replace('server_', '').astype(int)
            df = df.sort_values(by=['ts', 'server_num'])
            df = df.drop(columns=['server_num'])
            
            records = df.to_dict('records')
            
            return jsonify({
                "preview": records[:5],
                "records": records, # This is the full dataset
                "count": len(records)
            })
            
        except Exception as e:
            logger.error(f"Failed to process CSV: {e}")
            return jsonify({"error": f"Failed to process CSV: {e}"}), 500
    
    return jsonify({"error": "Invalid file type, must be .csv"}), 400

@app.route('/api/send_batch_chunk', methods=['POST'])
def handle_send_batch_chunk():
    """
    API to send a CHUNK of the batch.
    This is called multiple times by the client.
    """
    data = request.get_json()
    records_chunk = data.get('chunk')
    key_field = data.get('key_field')
    
    if not records_chunk or not key_field:
        return jsonify({"error": "Missing 'chunk' or 'key_field'"}), 400

    logger.info(f"Processing chunk of {len(records_chunk)} rows, split into 4 topics...")
    sent_count = 0
    failed_count = 0

    for row in records_chunk:
        try:
            key = row.get(key_field)
            ts = row.get('ts')
            
            # --- Create 4 deterministic IDs ---
            
            # 1. CPU
            cpu_data = {'ts': ts, 'server_id': key, 'cpu_pct': row.get('cpu_pct')}
            stable_cpu = f"{TOPIC_CPU}:{key}:{ts}:{row.get('cpu_pct')}"
            msg_id_cpu = uuid.uuid5(YAK_NAMESPACE, stable_cpu)
            msg_cpu = {"topic": TOPIC_CPU, "key": key, "payload": cpu_data}
            
            # 2. MEM
            mem_data = {'ts': ts, 'server_id': key, 'mem_pct': row.get('mem_pct')}
            stable_mem = f"{TOPIC_MEM}:{key}:{ts}:{row.get('mem_pct')}"
            msg_id_mem = uuid.uuid5(YAK_NAMESPACE, stable_mem)
            msg_mem = {"topic": TOPIC_MEM, "key": key, "payload": mem_data}
            
            # 3. NET
            net_data = {'ts': ts, 'server_id': key, 'net_in': row.get('net_in'), 'net_out': row.get('net_out')}
            stable_net = f"{TOPIC_NET}:{key}:{ts}:{row.get('net_in')}:{row.get('net_out')}"
            msg_id_net = uuid.uuid5(YAK_NAMESPACE, stable_net)
            msg_net = {"topic": TOPIC_NET, "key": key, "payload": net_data}
            
            # 4. DISK
            disk_data = {'ts': ts, 'server_id': key, 'disk_io': row.get('disk_io')}
            stable_disk = f"{TOPIC_DISK}:{key}:{ts}:{row.get('disk_io')}"
            msg_id_disk = uuid.uuid5(YAK_NAMESPACE, stable_disk)
            msg_disk = {"topic": TOPIC_DISK, "key": key, "payload": disk_data}
            
            # 5. Send all four messages
            res_cpu = producer.send(msg_id_cpu, msg_cpu)
            res_mem = producer.send(msg_id_mem, msg_mem)
            res_net = producer.send(msg_id_net, msg_net)
            res_disk = producer.send(msg_id_disk, msg_disk)
            
            sent_count += 4
            
            logger.info(
                f"Sent 4 msgs for {key} | "
                f"CPU (P:{res_cpu.get('partition', 'N/A')}, O:{res_cpu.get('offset')}) | "
                f"MEM (P:{res_mem.get('partition', 'N/A')}, O:{res_mem.get('offset')}) | "
                f"NET (P:{res_net.get('partition', 'N/A')}, O:{res_net.get('offset')}) | "
                f"DISK (P:{res_disk.get('partition', 'N/A')}, O:{res_disk.get('offset')})"
            )
            
        except Exception as e:
            logger.error(f"Failed to send message block for row {row.get(key_field)}: {e}")
            failed_count += 4 # All 4 failed for this row

    logger.info(f"Chunk send complete. Sent: {sent_count}, Failed: {failed_count}")
    return jsonify({"sent": sent_count, "failed": failed_count})


if __name__ == '__main__':
    logger.info("Starting YAK Producer Web Portal on http://0.0.0.0:8080")
    app.run(host='0.0.0.0', port=8080, debug=True)