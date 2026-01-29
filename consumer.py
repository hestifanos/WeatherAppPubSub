# consumer.py
from google.cloud import pubsub_v1
import glob
import json
import os
import csv
from datetime import datetime

# ============================================
# SECTION 1: AUTHENTICATION SETUP
# ============================================
# Find service account key file
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
    print(f"Using service account key: {files[0]}")
else:
    print("Warning: No service account JSON file found!")
    print("Please ensure you have a service account key file in the current directory.")

# ============================================
# SECTION 2: CONFIGURATION
# ============================================
project_id = ""  
topic_name = "weather"
subscription_id = "weather-sub"

# ============================================
# SECTION 3: SUBSCRIBER CLIENT INITIALIZATION
# ============================================
# Initialize the Pub/Sub subscriber client
subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(project_id, subscription_id)
print(f"Subscription path: {subscription_path}")

# ============================================
# SECTION 4: MESSAGE PROCESSOR CLASS
# ============================================
class MessageProcessor:
    """
    Class to process Pub/Sub messages with detailed deserialization and printing
    
    Main Process Flow for Each Message:
    1. Receive raw message bytes from Pub/Sub
    2. Deserialize bytes to JSON string
    3. Parse JSON string into Python dictionary
    4. Print dictionary values in a readable format
    5. Handle any errors during processing
    """
    
    def __init__(self, save_to_csv=False, csv_filename="consumed_messages.csv"):
        """
        Initialize the message processor
        
        Parameters:
        -----------
        save_to_csv : bool
            Whether to save messages to a CSV file
        csv_filename : str
            Name of the CSV file to save to (if save_to_csv is True)
        """
        self.message_count = 0
        self.save_to_csv = save_to_csv
        self.csv_filename = csv_filename
        self.csv_writer = None
        self.csv_file = None
        
        # Statistics
        self.stats = {
            'total_received': 0,
            'successfully_processed': 0,
            'failed_deserialization': 0,
            'failed_processing': 0,
            'start_time': datetime.now()
        }
        
        if save_to_csv:
            self.setup_csv_writer()
    
    def setup_csv_writer(self):
        """Setup CSV writer for saving processed messages"""
        try:
            self.csv_file = open(self.csv_filename, 'w', newline='', encoding='utf-8')
        
            print(f"CSV file ready: {self.csv_filename}")
        except Exception as e:
            print(f"Error setting up CSV file: {e}")
            self.save_to_csv = False
    
    def save_message_to_csv(self, message_dict):
        """Save a processed message dictionary to CSV"""
        if not self.save_to_csv or not self.csv_file:
            return
        
        try:
            # Initialize CSV writer with first message's keys
            if self.csv_writer is None:
                # Get all keys from the message dictionary
                fieldnames = list(message_dict.keys())
                self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
                self.csv_writer.writeheader()
                print(f"CSV header written with columns: {fieldnames}")
            
            # Write the message as a row
            self.csv_writer.writerow(message_dict)
            self.csv_file.flush()  
            
        except Exception as e:
            print(f"Error saving to CSV: {e}")
    
    def process_message(self, message: pubsub_v1.subscriber.message.Message):
        """
        Main method to process a single Pub/Sub message
        
        Parameters:
        -----------
        message : pubsub_v1.subscriber.message.Message
            The raw message received from Pub/Sub
        """
        self.message_count += 1
        self.stats['total_received'] += 1
        
        print(f"\n{'='*60}")
        print(f" MESSAGE #{self.message_count}")
        print(f"{'='*60}")
        
        # ============================================
        # STEP 1: DISPLAY MESSAGE METADATA
        # ============================================
        print("\n MESSAGE METADATA:")
        print(f"  Message ID: {message.message_id}")
        print(f"  Publish Time: {message.publish_time}")
        print(f"  Delivery Attempt: {message.delivery_attempt}")
        
        # Display message attributes if they exist
        if message.attributes:
            print(f"\n  Attributes:")
            for key, value in message.attributes.items():
                print(f"    {key}: {value}")
        else:
            print(f"\n  Attributes: (none)")
        
        # ============================================
        # STEP 2: DESERIALIZE MESSAGE DATA
        # ============================================
        print(f"\n DESERIALIZATION PROCESS:")
        
        try:
            # Step 2A: Decode bytes to string
            print(f"  1. Decoding bytes to string...")
            message_string = message.data.decode('utf-8')
            print(f"     Raw string (first 200 chars):")
            print(f"     '{message_string[:200]}...'")
            
            # Step 2B: Parse JSON string to dictionary
            print(f"  2. Parsing JSON to dictionary...")
            message_dict = json.loads(message_string)
            print(f"     ✓ Successfully parsed JSON")
            
            # ============================================
            # STEP 3: ADD PROCESSING METADATA
            # ============================================
            print(f"\n ADDING PROCESSING METADATA:")
            
            # Add metadata about when we processed this message
            processing_time = datetime.now()
            message_dict['_processing_metadata'] = {
                'consumed_at': processing_time.isoformat(),
                'message_id': message.message_id,
                'sequence_number': self.message_count,
                'processed_by': 'csv_consumer_v1'
            }
            
            # Calculate processing latency if publish time is available
            if hasattr(message, 'publish_time') and message.publish_time:
                publish_time = message.publish_time
                latency = (processing_time - publish_time).total_seconds()
                message_dict['_processing_metadata']['processing_latency_seconds'] = latency
                print(f"  Processing latency: {latency:.3f} seconds")
            
            self.stats['successfully_processed'] += 1
            
            # ============================================
            # STEP 4: PRINT DICTIONARY VALUES
            # ============================================
            self.print_dictionary_values(message_dict)
            
            # ============================================
            # STEP 5: SAVE TO CSV IF ENABLED
            # ============================================
            if self.save_to_csv:
                self.save_message_to_csv(message_dict)
            
            # ============================================
            # STEP 6: ACKNOWLEDGE THE MESSAGE
            # ============================================
            message.ack()
            print(f"\n MESSAGE ACKNOWLEDGED (ACK sent)")
            
        except json.JSONDecodeError as e:
            print(f"\n JSON DESERIALIZATION FAILED:")
            print(f"   Error: {e}")
            print(f"   Problematic JSON: {message_string}")
            self.stats['failed_deserialization'] += 1
            message.nack()  # Negative acknowledgment - ask for redelivery
            
        except UnicodeDecodeError as e:
            print(f"\n BYTE DECODING FAILED:")
            print(f"   Error: {e}")
            self.stats['failed_deserialization'] += 1
            message.nack()
            
        except Exception as e:
            print(f"\n UNEXPECTED ERROR:")
            print(f"   Error: {e}")
            import traceback
            traceback.print_exc()
            self.stats['failed_processing'] += 1
            message.nack()
    
    def print_dictionary_values(self, message_dict, indent=2):
        """
        Print all values from a dictionary in a readable format
        
        Parameters:
        -----------
        message_dict : dict
            Dictionary to print
        indent : int
            Number of spaces for indentation
        """
        print(f"\n DICTIONARY CONTENTS ({len(message_dict)} items):")
        print(f"{'-'*40}")
        
        # Sort keys for consistent output
        sorted_keys = sorted(message_dict.keys())
        
        for key in sorted_keys:
            value = message_dict[key]
            prefix = "  " * indent
            
            # Handle different value types with appropriate formatting
            if isinstance(value, dict):
                print(f"{prefix}{key}:")
                # Recursively print nested dictionaries
                self.print_dictionary_values(value, indent + 2)
            elif isinstance(value, list):
                print(f"{prefix}{key}: [List with {len(value)} items]")
                # Print first few items if it's a list
                for i, item in enumerate(value[:3]):  # Show first 3 items
                    print(f"{prefix}  [{i}]: {item}")
                if len(value) > 3:
                    print(f"{prefix}  ... and {len(value) - 3} more items")
            elif isinstance(value, str):
                # Truncate long strings
                if len(value) > 100:
                    display_value = value[:100] + "..."
                else:
                    display_value = value
                print(f"{prefix}{key}: \"{display_value}\"")
            elif value is None:
                print(f"{prefix}{key}: null")
            else:
                print(f"{prefix}{key}: {value}")
            
            # Add type information
            print(f"{prefix}  (Type: {type(value).__name__})")
    
    def print_statistics(self):
        """Print processing statistics"""
        print(f"\n{'='*60}")
        print(f" PROCESSING STATISTICS")
        print(f"{'='*60}")
        
        elapsed_time = (datetime.now() - self.stats['start_time']).total_seconds()
        
        print(f"Start Time: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Elapsed Time: {elapsed_time:.2f} seconds")
        print(f"\nMessages Received: {self.stats['total_received']}")
        print(f"Successfully Processed: {self.stats['successfully_processed']}")
        print(f"Failed Deserialization: {self.stats['failed_deserialization']}")
        print(f"Failed Processing: {self.stats['failed_processing']}")
        
        if self.stats['total_received'] > 0:
            success_rate = (self.stats['successfully_processed'] / self.stats['total_received']) * 100
            print(f"Success Rate: {success_rate:.2f}%")
        
        if elapsed_time > 0 and self.stats['successfully_processed'] > 0:
            messages_per_second = self.stats['successfully_processed'] / elapsed_time
            print(f"Processing Speed: {messages_per_second:.2f} messages/second")
    
    def start_consuming(self, timeout=None):
        """
        Start consuming messages from the subscription
        
        Parameters:
        -----------
        timeout : float or None
            How long to consume messages (in seconds)
            If None, runs indefinitely until Ctrl+C
        """
        print(f"\n{'='*60}")
        print(f" STARTING MESSAGE CONSUMER")
        print(f"{'='*60}")
        print(f"Subscription: {subscription_path}")
        print(f"Save to CSV: {'Yes' if self.save_to_csv else 'No'}")
        if self.save_to_csv:
            print(f"CSV File: {self.csv_filename}")
        print(f"\nListening for messages...")
        print(f"Press Ctrl+C to stop")
        print(f"{'='*60}")
        
        # Define callback function that will be called for each message
        def callback_wrapper(message):
            """Wrapper to call our process_message method"""
            self.process_message(message)
        
        # Subscribe to the topic
        streaming_pull_future = subscriber.subscribe(
            subscription_path,
            callback=callback_wrapper
        )
        
        try:
            # Start consuming messages
            if timeout:
                print(f"\n  Will consume messages for {timeout} seconds...")
                streaming_pull_future.result(timeout=timeout)
                print(f"\n Timeout reached. Stopping consumption...")
            else:
                print(f"\n Consuming messages indefinitely...")
                streaming_pull_future.result()  # Blocks forever
            
        except KeyboardInterrupt:
            print(f"\n\n INTERRUPTED BY USER (Ctrl+C)")
            streaming_pull_future.cancel()
            
        except Exception as e:
            print(f"\n ERROR IN CONSUMPTION: {e}")
            streaming_pull_future.cancel()
            
        finally:
            # Always run cleanup
            print(f"\n{'='*60}")
            print(f" CLEANING UP")
            print(f"{'='*60}")
            
            # Print final statistics
            self.print_statistics()
            
            # Close CSV file if open
            if self.csv_file:
                try:
                    self.csv_file.close()
                    print(f"CSV file closed: {self.csv_filename}")
                except Exception as e:
                    print(f"Error closing CSV file: {e}")
            
            # Close subscriber connection
            try:
                subscriber.close()
                print("Subscriber client closed")
            except Exception as e:
                print(f"Error closing subscriber: {e}")
            
            print(f"\n CONSUMER STOPPED")

# ============================================
# SECTION 5: ALTERNATIVE - SIMPLE CONSUMER
# ============================================
def simple_consumer():
    """
    Simple version without classes - good for understanding basics
    """
    print("Starting simple consumer...")
    
    def callback(message):
        """Simple callback for message processing"""
        try:
            # STEP 1: Deserialize message data
            print(f"\n{'='*40}")
            print(f"Received Message:")
            
            # Get raw bytes and decode to string
            message_data = message.data.decode('utf-8')
            print(f"Raw data: {message_data[:100]}...")
            
            # Parse JSON to dictionary
            data_dict = json.loads(message_data)
            
            # STEP 2: Print dictionary values
            print(f"\nDictionary contents:")
            for key, value in data_dict.items():
                print(f"  {key}: {value}")
            
            # Acknowledge the message
            message.ack()
            print(f"\nMessage acknowledged")
            
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()
    
    # Start consuming
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    print(f"Listening for messages on {subscription_path}")
    print("Press Ctrl+C to stop...")
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()



# ============================================
# SECTION 7: MAIN EXECUTION
# ============================================
if __name__ == "__main__":
    """
    Main entry point of the consumer script
    
    Three ways to run:
    1. Full-featured consumer (recommended)
    2. Simple consumer (basic version)
    3. Test deserialization (without Pub/Sub)
    """
    
    print("="*60)
    print("GOOGLE PUB/SUB MESSAGE CONSUMER")
    print("="*60)
    
    # ============================================
    # OPTION SELECTION
    # ============================================
    print("\nSelect an option:")
    print("1. Full-featured consumer (save to CSV)")
    print("2. Full-featured consumer (console only)")
    print("3. Simple consumer (basic version)")
    print("4. Test deserialization (without Pub/Sub)")
    print("5. Timed consumption (run for 30 seconds)")
    
    try:
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == "1":
            #  Full consumer with CSV saving
            print("\n" + "="*60)
            print("STARTING FULL CONSUMER WITH CSV SAVING")
            print("="*60)
            
            processor = MessageProcessor(
                save_to_csv=True,
                csv_filename="consumed_messages.csv"
            )
            processor.start_consuming()  # Runs indefinitely
            
        elif choice == "2":
            #  Full consumer without CSV
            print("\n" + "="*60)
            print("STARTING FULL CONSUMER (CONSOLE ONLY)")
            print("="*60)
            
            processor = MessageProcessor(save_to_csv=False)
            processor.start_consuming()  
            
        elif choice == "3":
            #  Simple consumer
            print("\n" + "="*60)
            print("STARTING SIMPLE CONSUMER")
            print("="*60)
            
            simple_consumer()
            
        elif choice == "4":
            # Test deserialization
            print("\n" + "="*60)
            print("RUNNING DESERIALIZATION TEST")
            print("="*60)
            
            test_deserialization()
            
        elif choice == "5":
            #  Timed consumption
            print("\n" + "="*60)
            print("STARTING TIMED CONSUMPTION (30 SECONDS)")
            print("="*60)
            
            processor = MessageProcessor(save_to_csv=True)
            processor.start_consuming(timeout=30)  
            
        else:
            print("\n Invalid choice. Using default option 2.")
            processor = MessageProcessor(save_to_csv=False)
            processor.start_consuming()
            
    except KeyboardInterrupt:
        print("\n\n Script interrupted by user")
    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()