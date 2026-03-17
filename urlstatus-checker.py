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
        
    def normalise_url(self, url):
        """Normalise URL by adding protocol if missing"""
        url = url.strip()
        if not url:
            return url
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url      
        return url
    
    def is_valid_url(self, url):
        """Check if URL is properly formatted (after normalization)"""
        if not url or not url.strip():
            return False
            
        normalised_url = self.normalise_url(url)
        
        try:
            result = urllib.parse.urlparse(normalised_url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def check_url(self, row_data):
        """Check individual URL and return result"""
        row_index, url, full_row = row_data
        original_url = url  # Keep original for reporting
        
        if not url or url.strip() == '':
            return {
                'row_index': row_index,
                'url': original_url,
                'status': 'bad',
                'reason': 'no_url',
                'http_status': None,
                'full_row': full_row
            }        
        normalised_url = self.normalise_url(url)       
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
            req = urllib.request.Request(normalised_url, headers={'User-Agent': 'Mozilla/5.0 (URL Checker)'})
            
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
            # connection errors - host doesn't exist, etc
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
        
        print(f"Warning: Could not detect encoding, using 'latin-1' as fallback")
        return 'latin-1'  # This can decode any byte sequence
    
    def load_csv_data(self):
        """Load CSV data and extract ALL rows for processing"""
        rows_to_check = []
        URL_INDEX = 34        

        try:
            encoding = self.detect_encoding(self.csv_file_path)
            
            if encoding == 'latin-1':
                print(f"Using latin-1 encoding with error handling...")
            
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
                        # Ensure row has enough columns
                        if len(row) <= URL_INDEX:
                            # Pad missing columns
                            row += [''] * (URL_INDEX + 1 - len(row))

                        url = row[URL_INDEX].strip()                        
                        # Add ALL rows to processing queue
                        rows_to_check.append((row_index, url, row))
                        row_count += 1
                        
                    except Exception as e:
                        print(f"Warning: Error processing row {row_index}: {e}")                        
                        rows_to_check.append((row_index, '', ['ERROR', 'ERROR', 'ERROR']))
                        row_count += 1
                
                print(f"Successfully loaded {row_count} rows (all rows) for processing")
                        
        except FileNotFoundError:
            print(f"Error: File '{self.csv_file_path}' not found")
            return []
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            print(f"Trying alternative approach...")
            
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
            print(f"No rows to process")
            return
        
        self.total_count = len(rows_to_check)
        print(f"Found {self.total_count} total rows to process")
        
        url_count = sum(1 for _, url, _ in rows_to_check if url and url.strip())
        print(f"Rows with URLs: {url_count}")
        print(f"Rows without URLs: {self.total_count - url_count}")
        print(f"Using {self.max_workers} worker threads with {self.timeout}s timeout")
        print("-" * 50)
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:        
            future_to_row = {
                executor.submit(self.check_url, row_data): row_data 
                for row_data in rows_to_check
            }
            
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
            print(f"\nBad entry breakdown:")
            for reason, count in sorted(reason_counts.items()):
                print(f"  {reason}: {count}")
                
        if 'no_url' in reason_counts:
            print(f"\nNote: 'no_url' means the row had no URL in the third column ({reason_counts['no_url']} rows)")
    
    def save_results(self, output_file='url_check_results.csv'):
        """Save results to CSV file"""
        if not self.results:
            print(f"No results to save")
            return
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                
                # add the csv header
                writer.writerow(['row_index', 'url', 'status', 'reason', 'http_status', 'original_row'])                
                sorted_results = sorted(self.results, key=lambda x: x['row_index'])                
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
    CSV_FILE = 'results.csv'  # CSV file path - hard coded for now
    MAX_WORKERS = 50  # Adjust based on system and network capacity
    TIMEOUT = 10  # Timeout in seconds for each request
    RESULTS_OUTPUT = 'url_check_results.csv'  # Output file for results
    BAD_URL_OUTPUT = 'bad_urls.csv' # Output file for bad URLs
    
    checker = URLChecker(CSV_FILE, max_workers=MAX_WORKERS, timeout=TIMEOUT)
    checker.process_urls()
    
    checker.save_results(RESULTS_OUTPUT)
    
    bad_results = [r for r in checker.results if r['status'] == 'bad']
    if bad_results:
        with open(BAD_URL_OUTPUT, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['row_index', 'url', 'reason', 'http_status'])
            for result in sorted(bad_results, key=lambda x: x['row_index']):
                writer.writerow([result['row_index'], result['url'], result['reason'], result['http_status']])
        print(f"Bad URLs saved to: {BAD_URL_OUTPUT}")

if __name__ == "__main__":
    main()
    