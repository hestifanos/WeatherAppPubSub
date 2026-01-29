# producer.py
from google.cloud import pubsub_v1
import glob
import json
import os
import csv
import time
from datetime import datetime

# ============================================
#  AUTHENTICATION SETUP
# ============================================
files = glob.glob("*.json")
if files:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
    print(f"Using service account key: {files[0]}")
else:
    print("Warning: No service account JSON file found!")
    print("Please ensure you have a service account key file in the current directory.")

# ============================================
#  CONFIGURATION
# ============================================
project_id = ""  
topic_name = "weather"

# ============================================
#  PUBLISHER CLIENT INITIALIZATION
# ============================================
# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(project_id, topic_name)
print(f"Topic path: {topic_path}")

# ============================================
#  CSV PROCESSING AND PUBLISHING FUNCTION
# ============================================
def process_csv_and_publish(csv_file_path):
    """
    Main function that iterates over CSV records and publishes them to Pub/Sub
    
    Process Flow:
    1. Open CSV file
    2. Iterate over each row
    3. Convert row to dictionary
    4. Serialize dictionary to JSON message
    5. Publish message to Pub/Sub topic
    6. Track results and progress
    """
    
    try:
        # ============================================
        #  OPEN CSV FILE
        # ============================================
        print(f"\n{'='*60}")
        print(f"Opening CSV file: {csv_file_path}")
        print('='*60)
        
        # Open the CSV file in read mode
        with open(csv_file_path, 'r') as csv_file:
            
            # ============================================
            #  CREATE CSV READER
            # ============================================
            csv_reader = csv.DictReader(csv_file)
            
            # Print CSV information
            if csv_reader.fieldnames:
                print(f"CSV Columns detected: {', '.join(csv_reader.fieldnames)}")
                print(f"Total columns: {len(csv_reader.fieldnames)}")
            else:
                print("Warning: No columns detected in CSV file")
                return 0
            
            # ============================================
            #  INITIALIZE COUNTERS AND TRACKERS
            # ============================================
            total_rows = 0
            successful_publishes = 0
            failed_publishes = 0
            start_time = time.time()
            
            print("\n" + "="*60)
            print("STARTING CSV PROCESSING AND PUBLISHING")
            print("="*60)
            
            # ============================================
            #  ITERATE OVER EACH RECORD IN CSV
            # ============================================
            for row_index, row_dict in enumerate(csv_reader):
                total_rows += 1
                
                print(f"\n{'─'*40}")
                print(f"PROCESSING RECORD #{total_rows}")
                print(f"{'─'*40}")
                
                # Display original CSV data
                print(f"Original CSV row {row_index + 1}:")
                for key, value in row_dict.items():
                    print(f"  {key}: {value}")
                
                # ============================================
                #  CONVERT RECORD TO DICTIONARY
                # ============================================
               
             
                # Create enhanced dictionary with original data
                message_dict = dict(row_dict)  
                
                # Add metadata for tracking
                message_dict['_metadata'] = {
                    'csv_row_number': total_rows,
                    'processing_timestamp': datetime.now().isoformat(),
                    'source_file': os.path.basename(csv_file_path)
                }
                
                #  Clean and validate data
                for key, value in message_dict.items():
                    if key != '_metadata': 
                        # Try to convert to float if it looks like a number
                        try:
                            if '.' in value:  
                                message_dict[key] = float(value)
                            elif value.isdigit():  
                                message_dict[key] = int(value)
                        except (ValueError, AttributeError):
                            pass  
                
                print(f"\nEnhanced dictionary (with metadata):")
                print(json.dumps(message_dict, indent=2))
                
                # ============================================
                #  SERIALIZE DICTIONARY INTO MESSAGE
                # ============================================
                # Convert Python dictionary to JSON string
                try:

                    message_json = json.dumps(message_dict)
                    
                    print(f"\nSerialized JSON message (first 200 chars):")
                    print(f"{message_json[:200]}...")
                    
                 
                    parsed_back = json.loads(message_json)
                    if parsed_back == message_dict:
                        print(" Serialization verified successfully")
                    else:
                        print(" Serialization verification failed")
                        
                except json.JSONDecodeError as e:
                    print(f" JSON serialization error: {e}")
                    failed_publishes += 1
                    continue 
                except TypeError as e:
                    print(f" Type error during serialization: {e}")
                    failed_publishes += 1
                    continue
                
                # ============================================
                #  PUBLISH MESSAGE TO TOPIC
                # ============================================
                try:
                    print(f"\nPublishing to topic: {topic_name}")
                    
                    # Encode the JSON string to bytes 
                    message_bytes = message_json.encode('utf-8')
                    
                    # Create attributes for the message 
                    attributes = {
                        'csv_row': str(total_rows),
                        'publish_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'message_type': 'csv_record',
                        'source': 'csv_producer_v1'
                    }
                    
                    # Publish the message
                    future = publisher.publish(
                        topic_path,          
                        message_bytes,       
                        **attributes        
                    )
                    
                    # ============================================
                    #  HANDLE PUBLISHING RESULT
                    # ============================================
                    # Define a callback function to handle the publishing result
                    def publishing_callback(publish_future):
                        """Callback executed when publishing completes"""
                        try:
                            # Get the message ID assigned by Pub/Sub
                            message_id = publish_future.result()
                            print(f"  ✓ Published successfully!")
                            print(f"  Message ID: {message_id}")
                            
                            # Update success counter
                            nonlocal successful_publishes
                            successful_publishes += 1
                            
                        except Exception as e:
                            print(f"  ✗ Publishing failed: {e}")
                            
                            # Update failure counter
                            nonlocal failed_publishes
                            failed_publishes += 1
                    
                    # Add callback to handle the publishing result asynchronously
                    future.add_done_callback(publishing_callback)
                    
                
                    time.sleep(0.05)
                    
                except Exception as e:
                    print(f" Error during publishing: {e}")
                    failed_publishes += 1
                
                # ============================================
                #  PROGRESS REPORTING
                # ============================================
                # Show progress every 10 records
                if total_rows % 10 == 0:
                    elapsed_time = time.time() - start_time
                    records_per_second = total_rows / elapsed_time if elapsed_time > 0 else 0
                    
                    print(f"\n{'='*60}")
                    print(f"PROGRESS: Processed {total_rows} records")
                    print(f"Success: {successful_publishes} | Failed: {failed_publishes}")
                    print(f"Speed: {records_per_second:.2f} records/second")
                    print(f"Elapsed time: {elapsed_time:.2f} seconds")
                    print('='*60)
                
                # Small delay to prevent overwhelming the system
                time.sleep(0.1)
            
            # ============================================
            #  FINAL SUMMARY
            # ============================================
            total_time = time.time() - start_time
            
            print(f"\n{'='*60}")
            print("CSV PROCESSING COMPLETE")
            print('='*60)
            print(f"Total CSV records processed: {total_rows}")
            print(f"Successfully published: {successful_publishes}")
            print(f"Failed to publish: {failed_publishes}")
            print(f"Success rate: {(successful_publishes/total_rows*100):.2f}%" if total_rows > 0 else "N/A")
            print(f"Total processing time: {total_time:.2f} seconds")
            
            if total_time > 0:
                print(f"Average speed: {total_rows/total_time:.2f} records/second")
            
            # Wait a moment for any pending callbacks to complete
            print("\nWaiting for pending publishes to complete...")
            time.sleep(2)
            
            return successful_publishes
            
    except FileNotFoundError:
        print(f"\n ERROR: CSV file not found at '{csv_file_path}'")
        print("Please check the file path and try again.")
        return 0
    except csv.Error as e:
        print(f"\n ERROR: CSV parsing error: {e}")
        return 0
    except Exception as e:
        print(f"\n ERROR: Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 0

# ============================================
#  ALTERNATIVE 
# ============================================
def process_csv_simple(csv_file_path):
    """
    Simplified version with minimal logging
    Good for high-volume processing
    """
    try:
        with open(csv_file_path, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            published_count = 0
            
            print(f"Processing {csv_file_path}...")
            
            for row in csv_reader:
                #  Convert row to dictionary
                message_dict = dict(row)
                
                #  Serialize to JSON
                message_json = json.dumps(message_dict)
                
                #  Publish to Pub/Sub
                future = publisher.publish(
                    topic_path,
                    message_json.encode('utf-8'),
                    source="csv_simple"
                )
                
                
                try:
                    message_id = future.result(timeout=10)  
                    published_count += 1
                    
                    # Print progress dots
                    if published_count % 100 == 0:
                        print(f".", end="", flush=True)
                        
                except Exception as e:
                    print(f"\nFailed to publish: {e}")
            
            print(f"\nPublished {published_count} messages")
            return published_count
            
    except Exception as e:
        print(f"Error: {e}")
        return 0

# ============================================
#  UTILITY FUNCTIONS
# ============================================
def publish_single_record(record_dict):
    """Publish a single dictionary record to Pub/Sub"""
    try:
        #  Ensure we have a dictionary
        if not isinstance(record_dict, dict):
            print("Error: Input must be a dictionary")
            return None
        
        #  Serialize dictionary to JSON
        message_json = json.dumps(record_dict)
        print(f"Serialized message: {message_json}")
        
        #  Publish to topic
        future = publisher.publish(
            topic_path,
            message_json.encode('utf-8'),
            message_type="single_record"
        )
        
        #  Get result
        message_id = future.result()
        print(f"Published with message ID: {message_id}")
        return message_id
        
    except Exception as e:
        print(f"Error publishing single record: {e}")
        return None


# ============================================
#  MAIN EXECUTION
# ============================================
if __name__ == "__main__":
    """
    Main entry point of the script
    
    Three ways to run:
    1. Process a CSV file (primary use case)
    2. Test with sample data
    3. Process CSV in simple mode
    """
    
    print("="*60)
    print("GOOGLE PUB/SUB CSV PRODUCER")
    print("="*60)
    
    # Configuration
    CSV_FILE_PATH = "Labels.csv"  
    
    # ============================================
    #  PROCESS CSV WITH DETAILED LOGGING
    # ============================================
    print("\nOption 1: Processing CSV with detailed logging")
    print("-" * 40)
    
    # Check if CSV file exists
    if os.path.exists(CSV_FILE_PATH):
        print(f"Found CSV file: {CSV_FILE_PATH}")
        print(f"File size: {os.path.getsize(CSV_FILE_PATH)} bytes")
        
        # Ask for confirmation
        response = input("\nStart processing? (y/n): ").strip().lower()
        
        if response == 'y':
            print("\nStarting CSV processing...")
            success_count = process_csv_and_publish(CSV_FILE_PATH)
            
            if success_count > 0:
                print(f"\n Successfully processed {success_count} records!")
            else:
                print(f"\n No records were successfully published")
        else:
            print("Processing cancelled by user")
    else:
        print(f"\n⚠  CSV file not found: {CSV_FILE_PATH}")
        print("Please ensure the CSV file exists in the current directory.")
        
        # List files in current directory
        print("\nFiles in current directory:")
        current_files = [f for f in os.listdir('.') if f.endswith('.csv')]
        if current_files:
            for file in current_files:
                print(f"  - {file}")
        else:
            print("  (No CSV files found)")
    
  
    
    print("\n" + "="*60)
    print("PRODUCER SCRIPT COMPLETED")
    print("="*60)