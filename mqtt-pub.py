#!/usr/bin/env python3
"""
Sample Sparkplug B Publisher
---------------------------
This script publishes sample Sparkplug B messages to test the client.
"""

import time
import random
import paho.mqtt.client as mqtt
import sparkplug_b_pb2
from google.protobuf.json_format import MessageToDict

# Configuration
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60
MQTT_USERNAME = None
MQTT_PASSWORD = None

# Sparkplug B topic format: spBv1.0/group_id/message_type/edge_node_id/device_id
GROUP_ID = "Group01"
EDGE_NODE_ID = "Node01"
DEVICE_ID = "Device01"

def create_birth_payload():
    """Create a node birth (NBIRTH) message payload."""
    payload = sparkplug_b_pb2.Payload()
    payload.timestamp = int(time.time() * 1000)
    payload.seq = 0
    
    # Add some metrics
    add_metric(payload, "Temperature", sparkplug_b_pb2.DataType.Float, float_value=22.5)
    add_metric(payload, "Humidity", sparkplug_b_pb2.DataType.Float, float_value=45.0)
    add_metric(payload, "Pressure", sparkplug_b_pb2.DataType.Float, float_value=1013.25)
    add_metric(payload, "Status", sparkplug_b_pb2.DataType.Boolean, boolean_value=True)
    add_metric(payload, "DeviceName", sparkplug_b_pb2.DataType.String, string_value="Environmental Sensor")
    
    return payload.SerializeToString()

def create_data_payload():
    """Create a device data (DDATA) message payload with random values."""
    payload = sparkplug_b_pb2.Payload()
    payload.timestamp = int(time.time() * 1000)
    payload.seq = 0
    
    # Add some metrics with random values
    add_metric(payload, "Temperature", sparkplug_b_pb2.DataType.Float, 
               float_value=20.0 + random.uniform(0, 10))
    add_metric(payload, "Humidity", sparkplug_b_pb2.DataType.Float, 
               float_value=40.0 + random.uniform(0, 20))
    add_metric(payload, "Pressure", sparkplug_b_pb2.DataType.Float, 
               float_value=1000.0 + random.uniform(0, 30))
    add_metric(payload, "Status", sparkplug_b_pb2.DataType.Boolean, 
               boolean_value=random.choice([True, False]))
    
    return payload.SerializeToString()

def add_metric(payload, name, datatype, **kwargs):
    """Add a metric to the payload."""
    metric = payload.metrics.add()
    metric.name = name
    metric.timestamp = int(time.time() * 1000)
    metric.datatype = datatype

    # Set the appropriate value based on datatype
    if datatype == sparkplug_b_pb2.DataType.Int8:
        metric.intValue = kwargs.get('int_value', 0)
    elif datatype == sparkplug_b_pb2.DataType.Int16:
        metric.intValue = kwargs.get('int_value', 0)
    elif datatype == sparkplug_b_pb2.DataType.Int32:
        metric.intValue = kwargs.get('int_value', 0)
    elif datatype == sparkplug_b_pb2.DataType.Int64:
        metric.longValue = kwargs.get('long_value', 0)
    elif datatype == sparkplug_b_pb2.DataType.UInt8:
        metric.intValue = kwargs.get('int_value', 0)
    elif datatype == sparkplug_b_pb2.DataType.UInt16:
        metric.intValue = kwargs.get('int_value', 0)
    elif datatype == sparkplug_b_pb2.DataType.UInt32:
        metric.intValue = kwargs.get('int_value', 0)
    elif datatype == sparkplug_b_pb2.DataType.UInt64:
        metric.longValue = kwargs.get('long_value', 0)
    elif datatype == sparkplug_b_pb2.DataType.Float:
        metric.floatValue = kwargs.get('float_value', 0.0)
    elif datatype == sparkplug_b_pb2.DataType.Double:
        metric.doubleValue = kwargs.get('double_value', 0.0)
    elif datatype == sparkplug_b_pb2.DataType.Boolean:
        metric.booleanValue = kwargs.get('boolean_value', False)
    elif datatype == sparkplug_b_pb2.DataType.String:
        metric.stringValue = kwargs.get('string_value', "")
    elif datatype == sparkplug_b_pb2.DataType.DateTime:
        metric.longValue = kwargs.get('long_value', int(time.time() * 1000))
    elif datatype == sparkplug_b_pb2.DataType.Text:
        metric.stringValue = kwargs.get('string_value', "")
    elif datatype == sparkplug_b_pb2.DataType.UUID:
        metric.stringValue = kwargs.get('string_value', "")
    elif datatype == sparkplug_b_pb2.DataType.Bytes:
        metric.bytesValue = kwargs.get('bytes_value', b"")

def on_connect(client, userdata, flags, rc):
    """Callback when the client connects to the MQTT broker."""
    print(f"Connected to MQTT broker with result code {rc}")
    
    # Publish a node birth message
    nbirth_topic = f"spBv1.0/{GROUP_ID}/NBIRTH/{EDGE_NODE_ID}"
    client.publish(nbirth_topic, create_birth_payload(), qos=1, retain=True)
    print(f"Published NBIRTH message on {nbirth_topic}")
    
    # Publish a device birth message
    dbirth_topic = f"spBv1.0/{GROUP_ID}/DBIRTH/{EDGE_NODE_ID}/{DEVICE_ID}"
    client.publish(dbirth_topic, create_birth_payload(), qos=1, retain=True)
    print(f"Published DBIRTH message on {dbirth_topic}")

def main():
    """Main function to set up the MQTT client and publish data."""
    # Setup MQTT client
    client = mqtt.Client()
    
    # Set username and password if configured
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    
    # Set callbacks
    client.on_connect = on_connect
    
    try:
        # Connect to MQTT broker
        print(f"Connecting to MQTT broker at {MQTT_HOST}:{MQTT_PORT}...")
        client.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
        
        # Start the loop in a non-blocking way
        client.loop_start()
        
        # Publish data messages periodically
        count = 0
        while count < 100:  # Publish 100 messages
            ddata_topic = f"spBv1.0/{GROUP_ID}/DDATA/{EDGE_NODE_ID}/{DEVICE_ID}"
            client.publish(ddata_topic, create_data_payload(), qos=0)
            print(f"Published DDATA message #{count+1} on {ddata_topic}")
            
            count += 1
            time.sleep(1)  # Publish every second
        
        # Publish a node death message before exiting
        ndeath_topic = f"spBv1.0/{GROUP_ID}/NDEATH/{EDGE_NODE_ID}"
        payload = sparkplug_b_pb2.Payload()
        payload.timestamp = int(time.time() * 1000)
        client.publish(ndeath_topic, payload.SerializeToString(), qos=1, retain=True)
        print(f"Published NDEATH message on {ndeath_topic}")
        
    except KeyboardInterrupt:
        print("Interrupted by user, shutting down...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print("Disconnecting MQTT client...")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()