import os
import re
import time
import io
import queue
import threading
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
import pytesseract

SITE_URL = "https://iaptexam.examtime.co.in/SEHSS/student_result.php"
# SITE_URL = "https://localhost:8443/SEHSS/student_result.php"  # For testing with local server
ZONES = ["east", "west", "north"]
INCLUDE_SINGLE_INPUT = False
SINGLE_INPUT_FILE = os.path.join("sehss-results", "sehss-input.txt")
ROLL_NUMBER = "HSWB2510256"
DOB_INPUT = "24-05-2009"  # DD-MM-YYYY
MOBILE_NO = ""  # Optional, leave empty if not required
BASE_DOWNLOAD_DIR = "sehss-results"
MAX_ATTEMPTS = 6
WAIT_TIMEOUT = 15
MANUAL_FALLBACK = False
INPUT_FILE_TEMPLATE = os.path.join(BASE_DOWNLOAD_DIR, "sehss-input-{zone}.txt")
STOP_ON_SUCCESS = False
SKIP_EXISTING_PDFS = True
MAX_WORKERS = 30
WORKERS = max(1, min(MAX_WORKERS, os.cpu_count() or 2))
DRIVER_START_RETRIES = 3
DRIVER_START_DELAY = 2
QUEUE_MAXSIZE = 500
PAGE_LOAD_STRATEGY = "eager"
BLOCK_IMAGES = True

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)



def normalize_dob(dob_str):
    dt = datetime.strptime(dob_str, "%d-%m-%Y")
    return dt.strftime("%Y-%m-%d")

def wait_for_new_pdf(download_dir, seen_files, timeout=15):
    start = time.time()
    while time.time() - start < timeout:
        names = os.listdir(download_dir)
        if any(name.endswith(".crdownload") for name in names):
            time.sleep(0.5)
            continue
        for name in names:
            if name.lower().endswith(".pdf") and name not in seen_files:
                return os.path.join(download_dir, name)
        time.sleep(0.5)
    return None


def iter_guesses_from_file(path):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            yield parts[0], parts[1]


def configure_driver(download_dir):
    chrome_options = Options()
    chrome_options.page_load_strategy = PAGE_LOAD_STRATEGY
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    if BLOCK_IMAGES:
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")

    prefs = {
        "download.default_directory": os.path.abspath(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_settings.popups": 0,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)

    return webdriver.Chrome(options=chrome_options)


def preprocess_image(image):
    image = image.convert("L")
    image = ImageOps.invert(image)
    image = image.resize((image.width * 2, image.height * 2), Image.Resampling.LANCZOS)
    image = image.filter(ImageFilter.MedianFilter(size=3))
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2.5)
    image = image.point(lambda p: 255 if p > 120 else 0)
    return image


def solve_captcha(driver, out_dir=None, suffix=""):
    captcha_img = driver.find_element(By.XPATH, "//img[contains(@src, 'roll_no_captcha.php')]")
    captcha_png = captcha_img.screenshot_as_png
    image = Image.open(io.BytesIO(captcha_png))
    processed = preprocess_image(image)
    config = r"--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789"
    text = pytesseract.image_to_string(processed, config=config)
    text = re.sub(r"[^0-9]", "", text)
    return text


def process_guess(driver, wait, roll_number, dob_input, download_dir):
    clean_dob = dob_input.replace("-", "")
    target_name = f"{roll_number}_{clean_dob}.pdf"
    target_path = os.path.join(download_dir, target_name)
    if SKIP_EXISTING_PDFS and os.path.exists(target_path):
        print(f"Skipping existing: {target_path}")
        return "skipped"

    dob = normalize_dob(dob_input)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        driver.get(SITE_URL)
        wait.until(EC.presence_of_element_located((By.ID, "rollno")))

        roll_el = driver.find_element(By.ID, "rollno")
        roll_el.clear()
        roll_el.send_keys(roll_number)

        dob_el = driver.find_element(By.ID, "dob")
        dob_el.clear()
        driver.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
            "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
            dob_el,
            dob,
        )

        if MOBILE_NO:
            mobile = driver.find_element(By.ID, "mobileno")
            mobile.clear()
            mobile.send_keys(MOBILE_NO)

        captcha_text = solve_captcha(driver)
        if not captcha_text or len(captcha_text) < 4:
            print(f"{roll_number} {dob_input} attempt {attempt}: captcha OCR failed.")
            if MANUAL_FALLBACK:
                captcha_text = input("Enter captcha: ").strip()
            if not captcha_text:
                print(f"{roll_number} {dob_input} attempt {attempt}: no captcha provided, retrying...")
                continue

        captcha_el = driver.find_element(By.ID, "captcha")
        captcha_el.clear()
        captcha_el.send_keys(captcha_text)
        seen = set(os.listdir(download_dir))
        driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()

        downloaded = wait_for_new_pdf(download_dir, seen, timeout=WAIT_TIMEOUT)
        if downloaded:
            if os.path.exists(target_path):
                timestamp = datetime.now().strftime("%H%M%S")
                target_name = f"{roll_number}_{clean_dob}_{timestamp}.pdf"
                target_path = os.path.join(download_dir, target_name)
            os.rename(downloaded, target_path)
            print(f"Downloaded: {target_path}")
            return "success"

        print(f"{roll_number} {dob_input} attempt {attempt}: no PDF downloaded.")
        if MANUAL_FALLBACK:
            manual_text = input("Enter captcha for retry (blank to skip): ").strip()
            if manual_text:
                driver.find_element(By.ID, "captcha").clear()
                driver.find_element(By.ID, "captcha").send_keys(manual_text)
                seen = set(os.listdir(download_dir))
                driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
                downloaded = wait_for_new_pdf(download_dir, seen, timeout=WAIT_TIMEOUT)
                if downloaded:
                    if os.path.exists(target_path):
                        timestamp = datetime.now().strftime("%H%M%S")
                        target_name = f"{roll_number}_{clean_dob}_{timestamp}.pdf"
                        target_path = os.path.join(download_dir, target_name)
                    os.rename(downloaded, target_path)
                    print(f"Downloaded: {target_path}")
                    return "success"
        print(f"{roll_number} {dob_input} attempt {attempt}: retrying...")

    print(f"{roll_number} {dob_input}: no PDF downloaded after retries.")
    return "failed"


def append_line(path, line, lock):
    with lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def start_driver_with_retries(download_dir):
    last_exc = None
    for attempt in range(1, DRIVER_START_RETRIES + 1):
        try:
            return configure_driver(download_dir)
        except WebDriverException as exc:
            last_exc = exc
            print(f"ChromeDriver start failed (attempt {attempt}/{DRIVER_START_RETRIES}): {exc}")
            time.sleep(DRIVER_START_DELAY)
    raise last_exc


def worker(worker_id, task_queue, stop_event, result_lock, download_dir, success_path, failed_path):
    try:
        driver = start_driver_with_retries(download_dir)
    except WebDriverException:
        print(f"[worker {worker_id}] failed to start ChromeDriver after retries, exiting.")
        return
    wait = WebDriverWait(driver, 10)
    try:
        while True:
            item = task_queue.get()
            try:
                if item is None:
                    return
                if stop_event.is_set():
                    continue
                roll_number, dob_input = item
                print(f"[worker {worker_id}] processing {roll_number} {dob_input}")
                try:
                    status = process_guess(driver, wait, roll_number, dob_input, download_dir)
                except WebDriverException as exc:
                    print(f"[worker {worker_id}] WebDriver error, restarting driver: {exc}")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    try:
                        driver = start_driver_with_retries(download_dir)
                        wait = WebDriverWait(driver, 10)
                        status = process_guess(driver, wait, roll_number, dob_input, download_dir)
                    except WebDriverException as retry_exc:
                        print(f"[worker {worker_id}] retry failed: {retry_exc}")
                        status = "failed"
                if status == "success" or status == "skipped":
                    append_line(success_path, f"{roll_number} {dob_input}", result_lock)
                elif status == "failed":
                    append_line(failed_path, f"{roll_number} {dob_input}", result_lock)
                if STOP_ON_SUCCESS and status in {"success", "skipped"}:
                    stop_event.set()
            finally:
                task_queue.task_done()
    finally:
        driver.quit()


def run_zone(zone, input_path):
    zone_dir = os.path.join(BASE_DOWNLOAD_DIR, zone)
    ensure_dir(zone_dir)
    success_path = os.path.join(zone_dir, f"sehss-success-{zone}.txt")
    failed_path = os.path.join(zone_dir, f"sehss-failed-{zone}.txt")

    input_exists = input_path and os.path.exists(input_path)
    if input_exists and os.path.getsize(input_path) == 0:
        print(f"[{zone}] input file is empty, skipping: {input_path}")
        return
    task_queue = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()
    result_lock = threading.Lock()
    threads = []
    for idx in range(WORKERS):
        thread = threading.Thread(
            target=worker,
            args=(idx, task_queue, stop_event, result_lock, zone_dir, success_path, failed_path),
            daemon=True,
        )
        thread.start()
        threads.append(thread)

    if input_exists:
        temp_path = input_path + ".processing"
        os.replace(input_path, temp_path)
        open(input_path, "w", encoding="utf-8").close()
        for item in iter_guesses_from_file(temp_path):
            if stop_event.is_set():
                with open(input_path, "a", encoding="utf-8") as f:
                    f.write(f"{item[0]} {item[1]}\n")
                continue
            task_queue.put(item)
        os.remove(temp_path)
    else:
        task_queue.put((ROLL_NUMBER, DOB_INPUT))

    for _ in threads:
        task_queue.put(None)

    task_queue.join()
    for thread in threads:
        thread.join()


def main():
    zones_to_run = []
    if INCLUDE_SINGLE_INPUT and os.path.exists(SINGLE_INPUT_FILE):
        zones_to_run.append(("manual", SINGLE_INPUT_FILE))

    for zone in ZONES:
        zones_to_run.append((zone, INPUT_FILE_TEMPLATE.format(zone=zone)))

    for zone, input_path in zones_to_run:
        if input_path and not os.path.exists(input_path):
            print(f"[{zone}] input file not found, skipping: {input_path}")
            continue
        print(f"[{zone}] starting with input file: {input_path or '(single)'}")
        run_zone(zone, input_path)


if __name__ == "__main__":
    main()
