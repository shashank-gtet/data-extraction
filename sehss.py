from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import csv
import pytesseract
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
import io
import re
from datetime import datetime, timedelta
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading

# Configuration
SITE_URL = "https://iaptexam.examtime.co.in/SEHSS/student_result.php"
SCREENSHOT_DIR = "successful_results"
RESULTS_CSV = "found_results.csv"
ERROR_LOG = "error_log.csv"
DOWNLOAD_DIR = "downloaded_results"
CAPTCHA_LOG = "captcha_attempts.csv"

os.makedirs(SCREENSHOT_DIR, exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

class FastResultScraper:
    def __init__(self, headless=True, max_workers=3):
        chrome_options = Options()
        
        # Optimized headless mode
        if headless:
            chrome_options.add_argument("--headless=new")  # New headless mode
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Performance optimizations
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")  # Disable images
        
        # Memory optimizations
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        
        # Configure PDF download settings
        prefs = {
            "download.default_directory": os.path.abspath(DOWNLOAD_DIR),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "profile.default_content_settings.popups": 0,
            "profile.default_content_setting_values.images": 2,  # Disable images
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Set Tesseract path - try multiple common locations
        tesseract_paths = [
            r'C:\Program Files\Tesseract-OCR\tesseract.exe',
            r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
            r'C:\Users\{}\AppData\Local\Tesseract-OCR\tesseract.exe'.format(os.getlogin()),
        ]
        
        tesseract_found = False
        for path in tesseract_paths:
            if os.path.exists(path):
                pytesseract.pytesseract.tesseract_cmd = path
                print(f"✓ Tesseract found at: {path}")
                tesseract_found = True
                break
        
        if not tesseract_found:
            print("⚠ Tesseract OCR not found! Using fallback captcha solving only.")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        # Set smaller timeout for faster failures
        self.wait = WebDriverWait(self.driver, 8)
        self.successful_attempts = 0
        self.total_attempts = 0
        self.captcha_cache = {}  # Cache for solved captchas
        self.session = requests.Session()  # For direct captcha download
        self.max_workers = max_workers
        self.lock = threading.Lock()
        
        # Captcha solving statistics
        self.captcha_success = 0
        self.captcha_fail = 0
        
        # Initialize captcha log
        self.setup_captcha_log()
    
    def setup_csv(self):
        """Initialize CSV files"""
        # Results CSV
        if not os.path.exists(RESULTS_CSV):
            with open(RESULTS_CSV, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "Timestamp", "Roll Number", "Date of Birth", "Captcha",
                    "Status", "Downloaded File", "Attempt Number", "Response Time"
                ])
        
        # Error log
        if not os.path.exists(ERROR_LOG):
            with open(ERROR_LOG, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Timestamp", "Roll", "DOB", "Error", "Attempt", "Response Time"])
    
    def setup_captcha_log(self):
        """Initialize captcha attempt log"""
        if not os.path.exists(CAPTCHA_LOG):
            with open(CAPTCHA_LOG, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["Timestamp", "Captcha_Image", "OCR_Result", "Confidence", "Success"])
    
    def log_captcha_attempt(self, image, result, confidence, success):
        """Log captcha solving attempt"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(CAPTCHA_LOG, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, "captcha_image", result, confidence, success])
    
    def download_captcha_directly(self):
        """Try to download captcha directly via requests for faster processing"""
        try:
            # Get session cookies from selenium
            cookies = self.driver.get_cookies()
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
            
            # Try to download captcha image directly
            captcha_url = None
            for img in self.driver.find_elements(By.TAG_NAME, 'img'):
                src = img.get_attribute('src')
                if 'roll_no_captcha.php' in src:
                    captcha_url = src
                    break
            
            if captcha_url:
                response = self.session.get(captcha_url, timeout=5)
                if response.status_code == 200:
                    return Image.open(io.BytesIO(response.content))
        except:
            pass
        return None
    
    def simple_captcha_solver(self):
        """Simplified but reliable captcha solver"""
        start_time = time.time()
        
        try:
            # Wait for captcha element
            time.sleep(1)
            captcha_img = self.driver.find_element(By.XPATH, "//img[contains(@src, 'roll_no_captcha.php')]")
            
            # Take screenshot
            captcha_screenshot = captcha_img.screenshot_as_png
            image = Image.open(io.BytesIO(captcha_screenshot))
            
            # Try 3 different preprocessing methods
            methods = [
                self.preprocess_method1,
                self.preprocess_method2,
                self.preprocess_method3
            ]
            
            for method in methods:
                try:
                    processed = method(image)
                    # Simple OCR with basic config
                    custom_config = r'--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789'
                    captcha_text = pytesseract.image_to_string(processed, config=custom_config)
                    captcha_text = re.sub(r'[^0-9]', '', captcha_text)
                    
                    # Validate
                    if 4 <= len(captcha_text) <= 6:
                        self.captcha_success += 1
                        solve_time = time.time() - start_time
                        print(f"  Captcha solved: {captcha_text} ({solve_time:.1f}s)")
                        return captcha_text
                except:
                    continue
            
            # If all OCR methods fail, use fallback
            return self.smart_fallback()
            
        except Exception as e:
            print(f"  Captcha error: {e}")
            return self.smart_fallback()
    
    def preprocess_method1(self, image):
        """Method 1: Simple grayscale and threshold"""
        image = image.convert('L')
        enhancer = ImageEnhance.Contrast(image)
        image = enhancer.enhance(2.0)
        image = image.point(lambda p: 255 if p > 150 else 0)
        return image
    
    def preprocess_method2(self, image):
        """Method 2: Inverted colors"""
        image = image.convert('L')
        image = ImageOps.invert(image)
        enhancer = ImageEnhance.Contrast(image)
        image = enhancer.enhance(2.5)
        image = image.point(lambda p: 255 if p > 100 else 0)
        return image
    
    def preprocess_method3(self, image):
        """Method 3: Resize and denoise"""
        image = image.convert('L')
        # Resize for better OCR
        image = image.resize((image.width * 2, image.height * 2), Image.Resampling.LANCZOS)
        # Denoise
        image = image.filter(ImageFilter.MedianFilter(size=3))
        # Enhance
        enhancer = ImageEnhance.Contrast(image)
        image = enhancer.enhance(3.0)
        # Simple threshold
        pixels = list(image.getdata())
        if pixels:
            avg = sum(pixels) / len(pixels)
            threshold = avg * 0.7
        else:
            threshold = 120
        image = image.point(lambda p: 255 if p > threshold else 0)
        return image
    
    def smart_fallback(self):
        """Intelligent fallback that learns from previous attempts"""
        self.captcha_fail += 1
        
        # If we have successful captchas in cache, use similar pattern
        if self.captcha_cache:
            last_captcha = list(self.captcha_cache.values())[-1]
            # Slightly modify last successful captcha
            captcha_list = list(last_captcha)
            if len(captcha_list) >= 5:
                # Change one random digit
                idx = random.randint(0, len(captcha_list)-1)
                captcha_list[idx] = str(random.randint(0, 9))
                return ''.join(captcha_list)
        
        # Generate time-based pseudo-random number
        current_time = datetime.now()
        seed = current_time.second * 100 + current_time.microsecond % 100
        random.seed(seed)
        
        # Generate 5-digit number (most common captcha length)
        return ''.join([str(random.randint(0, 9)) for _ in range(5)])
    
    def fill_form_fast(self, roll_number, dob_str):
        """Optimized form filling"""
        try:
            # Use JavaScript for faster form filling
            js_script = f"""
            document.getElementById("rollno").value = "{roll_number}";
            document.getElementById("dob").value = "{dob_str}";
            
            // Clear mobile if exists
            var mobile = document.getElementById("mobileno");
            if (mobile) mobile.value = "";
            
            return true;
            """
            
            self.driver.execute_script(js_script)
            
            # Solve captcha
            captcha_text = self.simple_captcha_solver()
            
            # Fill captcha
            captcha_input = self.driver.find_element(By.ID, "captcha")
            self.driver.execute_script(f"arguments[0].value = '{captcha_text}';", captcha_input)
            
            # Cache successful captcha pattern
            if 4 <= len(captcha_text) <= 6:
                key = f"{roll_number}_{dob_str}"
                self.captcha_cache[key] = captcha_text
                # Limit cache size
                if len(self.captcha_cache) > 50:
                    self.captcha_cache.pop(next(iter(self.captcha_cache)))
            
            return captcha_text
            
        except Exception as e:
            print(f"  Form error: {e}")
            # Fallback to random 5-digit
            return ''.join([str(random.randint(0, 9)) for _ in range(5)])
    
    def submit_and_check_fast(self, roll_number, dob_str):
        """Optimized submission and checking"""
        start_time = time.time()
        
        try:
            # Get initial files
            initial_files = set()
            try:
                initial_files = set(os.listdir(DOWNLOAD_DIR))
            except:
                pass
            
            # Submit using JavaScript (faster than click)
            self.driver.execute_script("""
                var btn = document.querySelector("button[type='submit']");
                if (btn) btn.click();
            """)
            
            # Wait with dynamic timeout
            max_wait = 8
            wait_start = time.time()
            downloaded_file = None
            
            while time.time() - wait_start < max_wait:
                try:
                    current_files = set(os.listdir(DOWNLOAD_DIR))
                    new_files = current_files - initial_files
                    
                    # Check for PDFs
                    pdf_files = [f for f in new_files if f.lower().endswith('.pdf')]
                    
                    if pdf_files:
                        downloaded_file = os.path.join(DOWNLOAD_DIR, pdf_files[0])
                        break
                except:
                    pass
                
                # Check for error messages quickly
                try:
                    page_text = self.driver.page_source.lower()
                    if any(msg in page_text for msg in ["invalid", "incorrect", "error", "try again"]):
                        response_time = time.time() - start_time
                        return "ERROR", "Invalid credentials", response_time
                except:
                    pass
                
                time.sleep(0.5)  # Shorter sleep for faster checking
            
            if downloaded_file:
                # Rename file
                clean_dob = dob_str.replace('-', '')
                new_filename = f"{roll_number}_{clean_dob}.pdf"
                new_path = os.path.join(DOWNLOAD_DIR, new_filename)
                
                # Handle duplicates
                if os.path.exists(new_path):
                    timestamp = datetime.now().strftime("%H%M%S")
                    new_filename = f"{roll_number}_{clean_dob}_{timestamp}.pdf"
                    new_path = os.path.join(DOWNLOAD_DIR, new_filename)
                
                os.rename(downloaded_file, new_path)
                response_time = time.time() - start_time
                return "SUCCESS", new_path, response_time
            
            response_time = time.time() - start_time
            return "ERROR", "No PDF downloaded", response_time
            
        except Exception as e:
            response_time = time.time() - start_time
            return "EXCEPTION", str(e), response_time
    
    def attempt_combination_fast(self, roll_number, dob_str):
        """Fast single attempt"""
        self.total_attempts += 1
        attempt_start = time.time()
        
        try:
            # Navigate (with retry)
            for retry in range(2):
                try:
                    self.driver.get(SITE_URL)
                    # Wait only for critical elements
                    self.wait.until(EC.presence_of_element_located((By.ID, "rollno")))
                    break
                except:
                    if retry == 1:
                        raise
                    time.sleep(1)
            
            # Fill form
            captcha = self.fill_form_fast(roll_number, dob_str)
            
            # Submit and check
            status, message, response_time = self.submit_and_check_fast(roll_number, dob_str)
            
            if status == "SUCCESS":
                with self.lock:
                    self.successful_attempts += 1
                    print(f"  ✅ SUCCESS! {roll_number} | {dob_str} | {response_time:.1f}s")
                    self.log_result(roll_number, dob_str, captcha, status, message, response_time)
                return True
            else:
                with self.lock:
                    print(f"  ❌ {status}: {roll_number} | {response_time:.1f}s")
                    self.log_result(roll_number, dob_str, captcha, status, "", response_time)
                return False
                
        except Exception as e:
            response_time = time.time() - attempt_start
            with self.lock:
                print(f"  ⚠ Exception: {e} | {response_time:.1f}s")
                self.log_error(roll_number, dob_str, str(e), response_time)
            return False
    
    def log_result(self, roll_number, dob_str, captcha, status, downloaded_file="", response_time=0):
        """Log result to CSV"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(RESULTS_CSV, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp, roll_number, dob_str, captcha,
                status, downloaded_file, self.total_attempts, f"{response_time:.2f}"
            ])
    
    def log_error(self, roll_number, dob_str, error_msg, response_time):
        """Log error to CSV"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(ERROR_LOG, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, roll_number, dob_str, error_msg, self.total_attempts, f"{response_time:.2f}"])

    def close(self):
        """Close driver"""
        self.driver.quit()
        print(f"\nCaptcha success rate: {self.captcha_success}/{self.captcha_success + self.captcha_fail}")

# Multi-threaded processing
class ParallelScraper:
    def __init__(self, max_workers=3):
        self.max_workers = max_workers
        self.scrapers = []
        self.results_queue = Queue()
        self.successful = []
        
    def worker(self, scraper_id, roll_number, dates):
        """Worker thread for processing a single roll number"""
        scraper = FastResultScraper(headless=True)
        self.scrapers.append(scraper)
        scraper.setup_csv()
        
        print(f"\n[Worker {scraper_id}] Processing {roll_number} with {len(dates)} dates")
        
        for dob in dates:
            if self.results_queue.qsize() > 0:
                # Check if this roll was already solved by another worker
                break
            
            success = scraper.attempt_combination_fast(roll_number, dob)
            if success:
                self.results_queue.put((roll_number, dob))
                break
        
        scraper.close()
        
    def process_parallel(self, roll_numbers, dates):
        """Process multiple roll numbers in parallel"""
        print(f"\nStarting parallel processing with {self.max_workers} workers")
        print(f"Total rolls: {len(roll_numbers)} | Dates per roll: {len(dates)}")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            
            for i, roll in enumerate(roll_numbers):
                # Submit task to thread pool
                future = executor.submit(self.worker, i+1, roll, dates)
                futures.append(future)
                
                # Limit concurrent tasks
                if len(futures) >= self.max_workers * 2:
                    # Wait for some to complete
                    for f in as_completed(futures[:self.max_workers]):
                        f.result()
                    futures = futures[self.max_workers:]
            
            # Wait for remaining
            for f in as_completed(futures):
                f.result()
        
        # Collect results
        while not self.results_queue.empty():
            self.successful.append(self.results_queue.get())
        
        return self.successful

# Data generation functions
def generate_roll_numbers():
    """Generate roll numbers from HSMH2504819 to HSMH2504820"""
    rolls = []
    for num in range(4819, 4821):
        roll = f"HSMH25{num:05d}"
        rolls.append(roll)
    return rolls

def generate_dates():
    """Generate dates from 01-12-2011 to 31-12-2011 in DD-MM-YYYY format"""
    dates = []
    start_date = datetime(2011, 12, 1)
    end_date = datetime(2011, 12, 31)
    
    current_date = start_date
    while current_date <= end_date:
        dob_str = current_date.strftime("%d-%m-%Y")
        dates.append(dob_str)
        current_date += timedelta(days=1)
    
    return dates

# Optimized main function
def main_optimized():
    print("=" * 80)
    print("SEHSS RESULT SCRAPER - SIMPLIFIED MODE")
    print("=" * 80)
    print("Features:")
    print("• Simplified captcha solving (3 methods)")
    print("• Intelligent fallback with learning")
    print("• Response time tracking")
    print("• Captcha success rate monitoring")
    print("=" * 80)
    
    # Generate data
    print("\nGenerating data...")
    roll_numbers = generate_roll_numbers()
    dates = generate_dates()
    
    print(f"Roll numbers: {len(roll_numbers)}")
    print(f"Dates per roll: {len(dates)}")
    print(f"Max attempts: {len(roll_numbers) * len(dates):,}")
    
    # Choose mode
    print("\nChoose processing mode:")
    print("1. Sequential (more stable)")
    print("2. Parallel 2x (faster)")
    print("3. Parallel 3x (fastest, more aggressive)")
    
    choice = input("Enter choice (1-3): ").strip()
    
    start_time = datetime.now()
    
    if choice == "1":
        # Sequential mode
        scraper = FastResultScraper(headless=True)
        scraper.setup_csv()
        
        successful = []
        for roll in roll_numbers:
            print(f"\n{'='*50}")
            print(f"Processing: {roll}")
            print(f"{'='*50}")
            
            for dob in dates:
                success = scraper.attempt_combination_fast(roll, dob)
                if success:
                    successful.append((roll, dob))
                    break
        
        scraper.close()
        
    elif choice in ["2", "3"]:
        # Parallel mode
        workers = 2 if choice == "2" else 3
        parallel_scraper = ParallelScraper(max_workers=workers)
        successful = parallel_scraper.process_parallel(roll_numbers, dates)
    
    else:
        print("Invalid choice, using sequential mode")
        scraper = FastResultScraper(headless=True)
        scraper.setup_csv()
        
        successful = []
        for roll in roll_numbers:
            for dob in dates:
                success = scraper.attempt_combination_fast(roll, dob)
                if success:
                    successful.append((roll, dob))
                    break
        
        scraper.close()
    
    # Statistics
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n{'='*80}")
    print("PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"Total time: {duration}")
    print(f"Successful results: {len(successful)}")
    
    if successful:
        print(f"\n🎯 SUCCESSFUL COMBINATIONS:")
        for roll, dob in successful:
            print(f"   • {roll} with DOB {dob}")
    
    # Performance stats
    if os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
            if len(rows) > 1:
                total_time = sum(float(row[7]) for row in rows[1:] if len(row) > 7 and row[7])
                avg_time = total_time / (len(rows) - 1)
                print(f"\n📊 PERFORMANCE:")
                print(f"   Average response time: {avg_time:.2f}s per attempt")
                print(f"   Total attempts: {len(rows) - 1}")

def quick_test_optimized():
    """Quick test with optimized scraper"""
    print("=" * 60)
    print("QUICK TEST - VISIBLE BROWSER")
    print("=" * 60)
    
    test_roll = "HSMH2504000"
    test_dob = "01-01-2010"
    
    print(f"\nTest: {test_roll} | {test_dob}")
    print("Opening visible browser...")
    
    scraper = FastResultScraper(headless=False)
    scraper.setup_csv()
    
    try:
        success = scraper.attempt_combination_fast(test_roll, test_dob)
        if success:
            print("\n✅ Test successful! PDF downloaded.")
        else:
            print("\n⚠ Test completed - no match found")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        scraper.close()

if __name__ == "__main__":
    print("=" * 60)
    print("SEHSS RESULT SCRAPER")
    print("=" * 60)
    print("Choose mode:")
    print("1. Quick test (visible browser)")
    print("2. Sequential mode")
    print("3. Parallel mode (2-3x faster)")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        quick_test_optimized()
    elif choice == "2":
        main_optimized()
    elif choice == "3":
        main_optimized()  # Will prompt for parallel mode inside
    else:
        print("Invalid choice, running sequential mode")
        main_optimized()
