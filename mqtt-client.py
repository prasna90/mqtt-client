#!/usr/bin/env python3
"""
Sparkplug B MQTT Client with SQLite Storage
------------------------------------------
This script subscribes to Sparkplug B messages via MQTT, decodes the Protocol Buffers format,
and stores the data in a SQLite database.
"""

import os
import sys
import time
import json
import sqlite3
import datetime
import paho.mqtt.client as mqtt
import sparkplug_b_pb2
from google.protobuf.json_format import MessageToDict

# Configuration
MQTT_HOST = "localhost"  # Change to your MQTT broker address
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
MQTT_USERNAME = None  # Set if your broker requires auth
MQTT_PASSWORD = None
SPARKPLUG_TOPIC = "spBv1.0/#"  # Standard Sparkplug B topic namespace
DATABASE_FILE = "sparkplug_data.db"

# Setup SQLite database
def setup_database():
    """Initialize the SQLite database with the required tables."""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # Create tables for the Sparkplug B data model
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id TEXT,
        node_id TEXT,
        device_id TEXT,
        timestamp INTEGER,
        online INTEGER,
        UNIQUE(group_id, node_id, device_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id INTEGER,
        name TEXT,
        timestamp INTEGER,
        datatype INTEGER,
        value TEXT,
        FOREIGN KEY (device_id) REFERENCES devices(id)
    )
    ''')
    
    cursor.execute('''
    CREATE INDEX IF NOT EXISTS idx_metrics_device ON metrics(device_id)
    ''')
    
    conn.commit()
    conn.close()
    print("Database setup complete.")

# Parse Sparkplug B payloads
def parse_payload(payload):
    """Parse the Sparkplug B Protocol Buffers payload."""
    try:
        # Decode the protobuf message
        sparkplug_message = sparkplug_b_pb2.Payload()
        sparkplug_message.ParseFromString(payload)
        
        # Convert to Python dictionary for easier handling
        payload_dict = MessageToDict(sparkplug_message)
        return payload_dict
    except Exception as e:
        print(f"Error parsing Sparkplug B payload: {e}")
        return None

# Extract and store device information and metrics
def store_data(topic_parts, payload_dict):
    """Store the Sparkplug B data in the SQLite database."""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # Parse the Sparkplug B topic parts
        # Format: spBv1.0/[group_id]/[message_type]/[edge_node_id]/[device_id]
        group_id = topic_parts[1] if len(topic_parts) > 1 else None
        message_type = topic_parts[2] if len(topic_parts) > 2 else None
        node_id = topic_parts[3] if len(topic_parts) > 3 else None
        device_id = topic_parts[4] if len(topic_parts) > 4 else None
        
        # Determine if the device is online based on message type
        online = 1
        if message_type in ["NDEATH", "DDEATH"]:
            online = 0
        
        # Timestamp (use the one in the payload or current time)
        timestamp = int(payload_dict.get('timestamp', int(time.time() * 1000)))
        
        # Insert or update device information
        cursor.execute('''
        INSERT INTO devices (group_id, node_id, device_id, timestamp, online)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(group_id, node_id, device_id) 
        DO UPDATE SET timestamp=?, online=?
        ''', (group_id, node_id, device_id, timestamp, online, timestamp, online))
        
        device_row_id = cursor.lastrowid
        if not device_row_id:
            # If no new row was inserted, get the existing device ID
            cursor.execute('''
            SELECT id FROM devices WHERE group_id=? AND node_id=? AND device_id=?
            ''', (group_id, node_id, device_id))
            device_row_id = cursor.fetchone()[0]
        
        # Process metrics if present
        if 'metrics' in payload_dict:
            for metric in payload_dict['metrics']:
                # Extract metric name and value
                name = metric.get('name', '')
                
                # Handle different data types - store the value and datatype
                datatype = metric.get('datatype', 0)
                value = None
                
                # Get the appropriate value field based on datatype
                for val_type in ['intValue', 'longValue', 'floatValue', 'doubleValue', 
                                'booleanValue', 'stringValue', 'bytesValue']:
                    if val_type in metric:
                        value = str(metric[val_type])
                        break
                
                if value is not None:
                    # Store metric in database
                    cursor.execute('''
                    INSERT INTO metrics (device_id, name, timestamp, datatype, value)
                    VALUES (?, ?, ?, ?, ?)
                    ''', (device_row_id, name, timestamp, datatype, value))
        
        conn.commit()
        conn.close()
        print(f"Stored data for {group_id}/{node_id}/{device_id}")
    except Exception as e:
        print(f"Error storing data: {e}")

# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    """Callback when the client connects to the MQTT broker."""
    print(f"Connected to MQTT broker with result code {rc}")
    # Subscribe to the Sparkplug B topic namespace
    client.subscribe(SPARKPLUG_TOPIC)
    print(f"Subscribed to {SPARKPLUG_TOPIC}")

def on_message(client, userdata, msg):
    """Callback when a message is received from the MQTT broker."""
    try:
        print(f"Received message on topic: {msg.topic}")
        
        # Split the topic into its components
        topic_parts = msg.topic.split('/')
        
        # Check if this is a Sparkplug B message
        if topic_parts[0] != "spBv1.0":
            print("Not a Sparkplug B message, ignoring.")
            return
        
        # Parse the payload
        payload_dict = parse_payload(msg.payload)
        if payload_dict:
            # Store the data
            store_data(topic_parts, payload_dict)
        else:
            print("Failed to parse payload, skipping.")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    """Main function to set up the MQTT client and database."""
    # Setup database
    setup_database()
    
    # Setup MQTT client
    client = mqtt.Client()
    
    # Set username and password if configured
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        # Connect to MQTT broker
        print(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
        client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
        
        # Start the loop
        print("Starting MQTT client loop...")
        client.loop_forever()
    except KeyboardInterrupt:
        print("Interrupted by user, shutting down...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print("Disconnecting MQTT client...")
        client.disconnect()

if __name__ == "__main__":
    main()