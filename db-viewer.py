#!/usr/bin/env python3
"""
SQLite Database Viewer for Sparkplug B Data
-------------------------------------------
This script provides a simple way to view the Sparkplug B data stored in the SQLite database.
"""

import sqlite3
import sys
import datetime
import argparse

# Database file
DATABASE_FILE = "sparkplug_data.db"

def print_devices():
    """Print all devices stored in the database."""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, group_id, node_id, device_id, timestamp, online
        FROM devices
        ORDER BY group_id, node_id, device_id
        ''')
        
        devices = cursor.fetchall()
        
        if not devices:
            print("No devices found in the database.")
            return
        
        print("\n=== Devices ===")
        print(f"{'ID':<5} {'Group':<10} {'Node':<10} {'Device':<10} {'Timestamp':<20} {'Status':<10}")
        print("-" * 70)
        
        for device in devices:
            device_id, group_id, node_id, device_id_str, timestamp, online = device
            timestamp_str = datetime.datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
            status = "ONLINE" if online else "OFFLINE"
            print(f"{device_id:<5} {group_id:<10} {node_id:<10} {device_id_str or 'N/A':<10} {timestamp_str:<20} {status:<10}")
        
        conn.close()
    except Exception as e:
        print(f"Error querying devices: {e}")

def print_latest_metrics(device_id=None, limit=10):
    """Print the latest metrics for a device or all devices."""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        query = '''
        SELECT 
            d.group_id, d.node_id, d.device_id,
            m.name, m.datatype, m.value, m.timestamp
        FROM metrics m
        JOIN devices d ON m.device_id = d.id
        '''
        
        params = []
        if device_id:
            query += " WHERE d.id = ?"
            params.append(device_id)
        
        query += " ORDER BY m.timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        
        metrics = cursor.fetchall()
        
        if not metrics:
            print(f"No metrics found for {'device ID ' + str(device_id) if device_id else 'any device'}.")
            return
        
        print("\n=== Latest Metrics ===")
        print(f"{'Group':<10} {'Node':<10} {'Device':<10} {'Metric Name':<20} {'Datatype':<10} {'Value':<20} {'Timestamp':<20}")
        print("-" * 100)
        
        for metric in metrics:
            group_id, node_id, device_id_str, name, datatype, value, timestamp = metric
            timestamp_str = datetime.datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
            
            # Map datatype number to name (simplified)
            datatype_names = {
                1: "Int8", 2: "Int16", 3: "Int32", 4: "Int64", 5: "UInt8", 
                6: "UInt16", 7: "UInt32", 8: "UInt64", 9: "Float", 10: "Double",
                11: "Boolean", 12: "String", 13: "DateTime", 14: "Text"
            }
            datatype_name = datatype_names.get(datatype, str(datatype))
            
            print(f"{group_id:<10} {node_id:<10} {device_id_str or 'N/A':<10} {name:<20} {datatype_name:<10} {value:<20} {timestamp_str:<20}")
        
        conn.close()
    except Exception as e:
        print(f"Error querying metrics: {e}")

def print_device_metrics(device_id):
    """Print all metrics for a specific device."""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        
        # First get device info
        cursor.execute('''
        SELECT group_id, node_id, device_id
        FROM devices
        WHERE id = ?
        ''', (device_id,))
        
        device = cursor.fetchone()
        if not device:
            print(f"No device found with ID {device_id}")
            return
        
        group_id, node_id, device_id_str = device
        print(f"\n=== Metrics for Device {device_id}: {group_id}/{node_id}/{device_id_str or 'N/A'} ===")
        
        # Get metrics for this device
        cursor.execute('''
        SELECT name, datatype, value, timestamp
        FROM metrics
        WHERE device_id = ?
        ORDER BY name, timestamp DESC
        ''', (device_id,))
        
        metrics = cursor.fetchall()
        
        if not metrics:
            print("No metrics found for this device.")
            return
        
        # Group metrics by name
        metric_groups = {}
        for name, datatype, value, timestamp in metrics:
            if name not in metric_groups:
                metric_groups[name] = []
            metric_groups[name].append((datatype, value, timestamp))
        
        # Print metrics by name
        for name, values in metric_groups.items():
            print(f"\nMetric: {name}")
            print(f"{'Datatype':<10} {'Value':<20} {'Timestamp':<20}")
            print("-" * 50)
            
            # Print only the latest 5 values
            for datatype, value, timestamp in values[:5]:
                timestamp_str = datetime.datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
                
                # Map datatype number to name (simplified)
                datatype_names = {
                    1: "Int8", 2: "Int16", 3: "Int32", 4: "Int64", 5: "UInt8", 
                    6: "UInt16", 7: "UInt32", 8: "UInt64", 9: "Float", 10: "Double",
                    11: "Boolean", 12: "String", 13: "DateTime", 14: "Text"
                }
                datatype_name = datatype_names.get(datatype, str(datatype))
                
                print(f"{datatype_name:<10} {value:<20} {timestamp_str:<20}")
            
            if len(values) > 5:
                print(f"... and {len(values) - 5} more values")
        
        conn.close()
    except Exception as e:
        print(f"Error querying device metrics: {e}")

def main():
    """Main function to handle command line arguments and display database content."""
    parser = argparse.ArgumentParser(description='View Sparkplug B data stored in SQLite database')
    parser.add_argument('--devices', action='store_true', help='List all devices')
    parser.add_argument('--metrics', action='store_true', help='Show latest metrics')
    parser.add_argument('--device', type=int, help='Show metrics for specific device ID')
    parser.add_argument('--limit', type=int, default=10, help='Limit number of metrics to show')
    
    args = parser.parse_args()
    
    # If no arguments provided, show all
    if not (args.devices or args.metrics or args.device is not None):
        args.devices = True
        args.metrics = True
    
    if args.devices:
        print_devices()
    
    if args.metrics:
        print_latest_metrics(limit=args.limit)
    
    if args.device is not None:
        print_device_metrics(args.device)

if __name__ == "__main__":
    main()