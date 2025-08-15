import csv
import urllib.request
import urllib.parse
import urllib.error
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import sys

class URLChecker:
    def __init__(self, csv_file_path, max_workers=50, timeout=10):
        self.csv_file_path = csv_file_path
        self.max_workers = max_workers
        self.timeout = timeout
        self.results = []
        self.results_lock = threading.Lock()
        self.processed_count = 0
        self.total_count = 0
        
    def normalize_url(self, url):
        """Normalize URL by adding protocol if missing"""
        url = url.strip()
        if not url:
            return url
        
        # If URL doesn't start with http:// or https://, add https://
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url
    
    def is_valid_url(self, url):
        """Check if URL is properly formatted (after normalization)"""
        if not url or not url.strip():
            return False
            
        normalized_url = self.normalize_url(url)
        
        try:
            result = urllib.parse.urlparse(normalized_url)
            return all([result.scheme, result.netloc])
        except:
            return False
    
    def check_url(self, row_data):
        """Check individual URL and return result"""
        row_index, url, full_row = row_data
        
        # Check if URL is empty or missing
        if not url or url.strip() == '':
            return {
                'row_index': row_index,
                'url': url,
                'status': 'bad',
                'reason': 'no_url',
                'http_status': None,
                'full_row': full_row
            }
        
        # Check if URL is malformed
        if not self.is_valid_url(url):
            return {
                'row_index': row_index,
                'url': url,
                'status': 'bad',
                'reason': 'malformed_url',
                'http_status': None,
                'full_row': full_row
            }
        
    def check_url(self, row_data):
        """Check individual URL and return result"""
        row_index, url, full_row = row_data
        original_url = url  # Keep original for reporting
        
        # Check if URL is empty or missing
        if not url or url.strip() == '':
            return {
                'row_index': row_index,
                'url': original_url,
                'status': 'bad',
                'reason': 'no_url',
                'http_status': None,
                'full_row': full_row
            }
        
        # Normalize URL (add protocol if missing)
        normalized_url = self.normalize_url(url)
        
        # Check if URL is malformed (even after normalization)
        if not self.is_valid_url(url):
            return {
                'row_index': row_index,
                'url': original_url,
                'status': 'bad',
                'reason': 'malformed_url',
                'http_status': None,
                'full_row': full_row
            }
        
        try:
            # Make HTTP request with normalized URL
            req = urllib.request.Request(normalized_url, headers={'User-Agent': 'Mozilla/5.0 (URL Checker)'})
            
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                status_code = response.getcode()
                
                if status_code == 200:
                    status = 'good'
                    reason = 'success'
                elif status_code == 404:
                    status = 'bad'
                    reason = '404_not_found'
                else:
                    status = 'bad'
                    reason = f'http_{status_code}'
                    
                return {
                    'row_index': row_index,
                    'url': original_url,  # Report original URL
                    'status': status,
                    'reason': reason,
                    'http_status': status_code,
                    'full_row': full_row
                }
            
        except urllib.error.HTTPError as e:
            # Handle HTTP errors (like 404, 500, etc.)
            if e.code == 404:
                reason = '404_not_found'
            else:
                reason = f'http_{e.code}'
                
            return {
                'row_index': row_index,
                'url': original_url,
                'status': 'bad',
                'reason': reason,
                'http_status': e.code,
                'full_row': full_row
            }
        except urllib.error.URLError as e:
            # Handle connection errors (host doesn't exist, etc.)
            return {
                'row_index': row_index,
                'url': original_url,
                'status': 'bad',
                'reason': 'host_not_exist',
                'http_status': None,
                'full_row': full_row
            }
        except Exception as e:
            return {
                'row_index': row_index,
                'url': original_url,
                'status': 'bad',
                'reason': f'error_{type(e).__name__}',
                'http_status': None,
                'full_row': full_row
            }
    
    def update_progress(self, result):
        """Thread-safe progress update"""
        with self.results_lock:
            self.results.append(result)
            self.processed_count += 1
            
            if self.processed_count % 100 == 0 or self.processed_count == self.total_count:
                progress = (self.processed_count / self.total_count) * 100
                print(f"Progress: {self.processed_count}/{self.total_count} ({progress:.1f}%)")
    
    def detect_encoding(self, file_path):
        """Detect file encoding by trying different encodings"""
        encodings_to_try = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1', 'cp437']
        
        # Read file as binary first
        try:
            with open(file_path, 'rb') as file:
                raw_data = file.read()
        except Exception as e:
            print(f"Error reading file: {e}")
            return 'latin-1'
        
        # Try to decode with different encodings
        for encoding in encodings_to_try:
            try:
                raw_data.decode(encoding)
                print(f"Detected encoding: {encoding}")
                return encoding
            except UnicodeDecodeError:
                continue
        
        print("Warning: Could not detect encoding, using 'latin-1' as fallback")
        return 'latin-1'  # This can decode any byte sequence
    
    def load_csv_data(self):
        """Load CSV data and extract ALL rows for processing"""
        rows_to_check = []
        
        try:
            # First try to detect encoding
            encoding = self.detect_encoding(self.csv_file_path)
            
            # If detection failed, try with error handling
            if encoding == 'latin-1':
                print("Using latin-1 encoding with error handling...")
            
            with open(self.csv_file_path, 'r', encoding=encoding, errors='replace') as file:
                csv_reader = csv.reader(file, quoting=csv.QUOTE_ALL)
                
                # Skip header row
                try:
                    headers = next(csv_reader)
                    print(f"Headers: {headers}")
                except Exception as e:
                    print(f"Warning: Could not read headers: {e}")
                    # Try without assuming quotes
                    file.seek(0)
                    csv_reader = csv.reader(file)
                    headers = next(csv_reader)
                    print(f"Headers (no quotes): {headers}")
                
                row_count = 0
                for row_index, row in enumerate(csv_reader, start=1):
                    try:
                        # Process ALL rows, regardless of content
                        if len(row) >= 3:
                            url = row[2].strip()  # Third field (index 2)
                        else:
                            # Pad row if it has fewer than 3 columns
                            while len(row) < 3:
                                row.append('')
                            url = ''
                        
                        # Add ALL rows to processing queue
                        rows_to_check.append((row_index, url, row))
                        row_count += 1
                        
                    except Exception as e:
                        print(f"Warning: Error processing row {row_index}: {e}")
                        # Still add the row with empty URL
                        rows_to_check.append((row_index, '', ['ERROR', 'ERROR', 'ERROR']))
                        row_count += 1
                
                print(f"Successfully loaded {row_count} rows (all rows) for processing")
                        
        except FileNotFoundError:
            print(f"Error: File '{self.csv_file_path}' not found")
            return []
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            print("Trying alternative approach...")
            
            # Fallback: try reading with different parameters
            try:
                with open(self.csv_file_path, 'r', encoding='latin-1', errors='ignore') as file:
                    csv_reader = csv.reader(file, delimiter=',', quotechar='"')
                    headers = next(csv_reader)
                    print(f"Fallback method - Headers: {headers}")
                    
                    for row_index, row in enumerate(csv_reader, start=1):
                        if len(row) >= 3:
                            url = row[2].strip()
                        else:
                            while len(row) < 3:
                                row.append('')
                            url = ''
                        rows_to_check.append((row_index, url, row))
                            
            except Exception as e2:
                print(f"Fallback method also failed: {e2}")
                return []
            
        return rows_to_check
    
    def process_urls(self):
        """Main processing function with multi-threading"""
        print(f"Loading CSV data from: {self.csv_file_path}")
        rows_to_check = self.load_csv_data()
        
        if not rows_to_check:
            print("No rows to process")
            return
        
        self.total_count = len(rows_to_check)
        print(f"Found {self.total_count} total rows to process")
        
        # Count how many actually have URLs
        url_count = sum(1 for _, url, _ in rows_to_check if url and url.strip())
        print(f"Rows with URLs: {url_count}")
        print(f"Rows without URLs: {self.total_count - url_count}")
        print(f"Using {self.max_workers} worker threads with {self.timeout}s timeout")
        print("-" * 50)
        
        start_time = time.time()
        
        # Process all rows with thread pool
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_row = {
                executor.submit(self.check_url, row_data): row_data 
                for row_data in rows_to_check
            }
            
            # Process completed tasks
            for future in as_completed(future_to_row):
                try:
                    result = future.result()
                    self.update_progress(result)
                except Exception as e:
                    row_data = future_to_row[future]
                    print(f"Error processing row {row_data[0]} URL {row_data[1]}: {e}")
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        self.print_summary(processing_time)
    
    def print_summary(self, processing_time):
        """Print processing summary"""
        good_count = sum(1 for r in self.results if r['status'] == 'good')
        bad_count = len(self.results) - good_count
        
        print("-" * 50)
        print(f"Processing completed in {processing_time:.2f} seconds")
        print(f"Total rows processed: {len(self.results)}")
        print(f"Good URLs (200): {good_count}")
        print(f"Bad entries: {bad_count}")
        print(f"Average time per row: {processing_time/len(self.results):.3f}s")
        
        # Breakdown of bad entry reasons
        reason_counts = {}
        for result in self.results:
            if result['status'] == 'bad':
                reason = result['reason']
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
        
        if reason_counts:
            print("\nBad entry breakdown:")
            for reason, count in sorted(reason_counts.items()):
                print(f"  {reason}: {count}")
                
        # Show what 'no_url' means
        if 'no_url' in reason_counts:
            print(f"\nNote: 'no_url' means the row had no URL in the third column ({reason_counts['no_url']} rows)")
    
    def save_results(self, output_file='url_check_results.csv'):
        """Save results to CSV file"""
        if not self.results:
            print("No results to save")
            return
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                
                # Write header
                writer.writerow(['row_index', 'url', 'status', 'reason', 'http_status', 'original_row'])
                
                # Sort results by row index
                sorted_results = sorted(self.results, key=lambda x: x['row_index'])
                
                # Write results
                for result in sorted_results:
                    writer.writerow([
                        result['row_index'],
                        result['url'],
                        result['status'],
                        result['reason'],
                        result['http_status'],
                        '|'.join(result['full_row'])  # Join original row with pipe separator
                    ])
            
            print(f"\nResults saved to: {output_file}")
            
        except Exception as e:
            print(f"Error saving results: {e}")


def main():
    # Configuration
    CSV_FILE = 'results.csv'  # Change this to your CSV file path
    MAX_WORKERS = 50  # Adjust based on your system and network capacity
    TIMEOUT = 10  # Timeout in seconds for each request
    
    # Create and run checker
    checker = URLChecker(CSV_FILE, max_workers=MAX_WORKERS, timeout=TIMEOUT)
    checker.process_urls()
    
    # Save results
    checker.save_results('url_check_results.csv')
    
    # Optional: Save only bad URLs
    bad_results = [r for r in checker.results if r['status'] == 'bad']
    if bad_results:
        with open('bad_urls.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['row_index', 'url', 'reason', 'http_status'])
            for result in sorted(bad_results, key=lambda x: x['row_index']):
                writer.writerow([result['row_index'], result['url'], result['reason'], result['http_status']])
        print("Bad URLs saved to: bad_urls.csv")


if __name__ == "__main__":
    main()