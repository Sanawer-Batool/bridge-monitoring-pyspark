"""
Bridge IoT Sensor Data Generator
Continuously generates simulated sensor data for temperature, vibration, and tilt sensors
"""

import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
import argparse


class BridgeSensorGenerator:
    def __init__(self, output_base_dir="streams", bridge_ids=None):
        self.output_base_dir = Path(output_base_dir)
        self.bridge_ids = bridge_ids or [f"bridge_{i:03d}" for i in range(1, 6)]
        
        # Create output directories
        self.temp_dir = self.output_base_dir / "bridge_temperature"
        self.vib_dir = self.output_base_dir / "bridge_vibration"
        self.tilt_dir = self.output_base_dir / "bridge_tilt"
        
        for directory in [self.temp_dir, self.vib_dir, self.tilt_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def generate_temperature_event(self, bridge_id, event_time):
        """Generate temperature reading (°C)"""
        return {
            "event_time": event_time.isoformat(),
            "bridge_id": bridge_id,
            "sensor_type": "temperature",
            "value": round(random.uniform(15.0, 35.0), 2),
            "ingest_time": datetime.now().isoformat()
        }
    
    def generate_vibration_event(self, bridge_id, event_time):
        """Generate vibration reading (Hz)"""
        return {
            "event_time": event_time.isoformat(),
            "bridge_id": bridge_id,
            "sensor_type": "vibration",
            "value": round(random.uniform(0.5, 15.0), 2),
            "ingest_time": datetime.now().isoformat()
        }
    
    def generate_tilt_event(self, bridge_id, event_time):
        """Generate tilt reading (degrees)"""
        return {
            "event_time": event_time.isoformat(),
            "bridge_id": bridge_id,
            "sensor_type": "tilt",
            "value": round(random.uniform(0.0, 5.0), 2),
            "ingest_time": datetime.now().isoformat()
        }
    
    def add_random_delay(self, base_time, max_delay_seconds=60):
        """Add random delay to simulate late arrivals"""
        delay = random.randint(0, max_delay_seconds)
        return base_time - timedelta(seconds=delay)
    
    def write_batch(self, events, output_dir, batch_id):
        """Write a batch of events to JSON file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = output_dir / f"batch_{timestamp}_{batch_id}.json"
        
        with open(filename, 'w') as f:
            for event in events:
                f.write(json.dumps(event) + '\n')
        
        print(f"Written {len(events)} events to {filename}")
    
    def run(self, duration_minutes=None, rate_per_minute=1, batch_interval_seconds=30):
        """
        Run the data generator
        
        Args:
            duration_minutes: How long to run (None = infinite)
            rate_per_minute: Events per minute per sensor per bridge
            batch_interval_seconds: How often to write batches
        """
        print(f"Starting data generation for bridges: {self.bridge_ids}")
        print(f"Rate: {rate_per_minute} events/min/sensor, Batch interval: {batch_interval_seconds}s")
        
        start_time = datetime.now()
        batch_id = 0
        
        try:
            while True:
                # Check duration
                if duration_minutes and (datetime.now() - start_time).seconds / 60 >= duration_minutes:
                    print("Duration reached. Stopping.")
                    break
                
                batch_id += 1
                current_time = datetime.now()
                
                temp_events = []
                vib_events = []
                tilt_events = []
                
                # Generate events for each bridge
                for bridge_id in self.bridge_ids:
                    for _ in range(rate_per_minute):
                        # Add random delay to simulate late arrivals
                        event_time = self.add_random_delay(current_time)
                        
                        temp_events.append(self.generate_temperature_event(bridge_id, event_time))
                        vib_events.append(self.generate_vibration_event(bridge_id, event_time))
                        tilt_events.append(self.generate_tilt_event(bridge_id, event_time))
                
                # Write batches
                self.write_batch(temp_events, self.temp_dir, batch_id)
                self.write_batch(vib_events, self.vib_dir, batch_id)
                self.write_batch(tilt_events, self.tilt_dir, batch_id)
                
                print(f"Batch {batch_id} complete. Sleeping {batch_interval_seconds}s...")
                time.sleep(batch_interval_seconds)
                
        except KeyboardInterrupt:
            print("\nStopping data generation...")


def main():
    parser = argparse.ArgumentParser(description='Bridge Sensor Data Generator')
    parser.add_argument('--output-dir', default='streams', help='Output directory for streams')
    parser.add_argument('--duration', type=int, help='Duration in minutes (omit for infinite)')
    parser.add_argument('--rate', type=int, default=1, help='Events per minute per sensor')
    parser.add_argument('--interval', type=int, default=30, help='Batch write interval in seconds')
    parser.add_argument('--bridges', type=int, default=5, help='Number of bridges to simulate')
    
    args = parser.parse_args()
    
    bridge_ids = [f"bridge_{i:03d}" for i in range(1, args.bridges + 1)]
    generator = BridgeSensorGenerator(args.output_dir, bridge_ids)
    generator.run(duration_minutes=args.duration, rate_per_minute=args.rate, 
                  batch_interval_seconds=args.interval)


if __name__ == "__main__":
    main()
