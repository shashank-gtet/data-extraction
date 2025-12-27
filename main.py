from bs4 import BeautifulSoup
import requests
import re
import threading
import pandas as pd
from pandas.errors import EmptyDataError
from concurrent.futures import ThreadPoolExecutor
import os
import csv
import json
from datetime import datetime
import sys

url1 = "https://iapt.manageexam.com/Student/Login"
url2 = "https://iapt.manageexam.com/Student/Application/ScoreCard"
url3 = "https://iapt.manageexam.com/Student/Application/JSscorecard"
logfile = "scrape_log.csv"
html_dump_dir = "scorecard_html"
log_fields = [
    "regno",
    "dob",
    "login_ok",
    "login_status",
    "scorecard_status",
    "email",
    "details",
    "scores",
    "scores_count",
    "scorecard_html_path",
    "name_mismatch",
    "retry_count",
    "all_scores_na",
]
output_fields = []
max_login_attempts = 2
max_missing_score_retries = 2
issue_file = "Scrape issue.csv"
issue_outputfile = "scrape_issue_scored.csv"
detail_keys = {
    "regno",
    "Name",
    "Mobile",
    "Email",
    "name",
    "parent_name",
    "gender",
    "class_standard",
    "roll_no",
    "date_of_birth",
    "exam_center",
}
def extract_student_details(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    def get_label_value(label_text):
        label_element = soup.find("h4", string=lambda text: text and label_text in text)
        if label_element:
            sibling = label_element.find_next("label")
            return sibling.text.strip() if sibling else None
        return None
    details = {
        "name": get_label_value("Name"),
        "parent_name": get_label_value("Father/Mother"),
        "gender": get_label_value("Gender"),
        "class_standard": get_label_value("Class/Standard"),
        "roll_no": get_label_value("Roll No."),
        "date_of_birth": get_label_value("Date of Birth"),
        "exam_center": get_label_value("Appeared at centre"),
    }
    soup = BeautifulSoup(html_content, 'html.parser')
    soup = soup.find('div', class_='col-sm-12', style="padding-bottom: 20px;")
    scores = {}
    if soup:
        subjects_column = soup.find("div", class_="col-sm-4") or soup.find("div", class_="col-sm-5")
        score_columns = soup.find_all("div", class_="col-sm-2")
        scores_column = score_columns[1] if len(score_columns) > 1 else (score_columns[0] if score_columns else None)
        if subjects_column and scores_column:
            subjects = []
            for label in subjects_column.find_all("label"):
                raw = label.text.strip()
                code_match = re.match(r"^[A-Z]{3,5}", raw)
                subjects.append(code_match.group(0) if code_match else raw.split('(')[0].strip())
            if not subjects:
                for heading in subjects_column.find_all(["h4", "h5"]):
                    raw = heading.text.strip()
                    code_match = re.match(r"^[A-Z]{3,5}", raw)
                    subjects.append(code_match.group(0) if code_match else raw.split('(')[0].strip())
            score_values = [
                label.text.strip() for label in scores_column.find_all("label", style="font-weight: bold;")
            ]
            scores = {subject: score for subject, score in zip(subjects, score_values)}
    combined = {**details, **scores}    
    return combined

def get_email(html_content):
    # Define patterns for extracting name, email, and mobile
    name_pattern = r'Name\s*:</label>\s*<label.*?text-primary.*?>(.*?)</label>'
    mobile_pattern = r'Mobile No\s*:</label>\s*(?:<label.*?>.*?</label>\s*)*?<label.*?text-primary.*?>(\d{10})</label>'
    email_pattern = r'Email Id\s*:</label>\s*<label.*?text-primary.*?>(.*?)</label>'

    # Search for matches
    name_match = re.search(name_pattern, html_content)
    mobile_match = re.search(mobile_pattern, html_content)
    email_match = re.search(email_pattern, html_content)

    # Extract and display results
    extracted_details = {
        "Name": name_match.group(1).strip() if name_match else "Not Found",
        "Mobile": mobile_match.group(1).strip() if mobile_match else "Not Found",
        "Email": email_match.group(1).strip() if email_match else "Not Found",
    }
    return extracted_details

def normalize_name(value):
    if not value:
        return ""
    return re.sub(r"[^A-Z0-9]", "", value.upper())

def extract_scores_only(data):
    return {k: v for k, v in data.items() if k not in detail_keys}

def login(regno, dob):
    session = requests.Session()
    try:
        response = session.get(url1)
        soup = BeautifulSoup(response.content, 'html.parser')
        token = soup.find("input", {"name": "__RequestVerificationToken"})['value']
        data = {
            "__RequestVerificationToken": token,
            "RegNo": regno,
            "DOB": dob,
        }
        response = session.post(url1, data=data)
    except requests.RequestException:
        return False, {
            "login_ok": False,
            "login_status": None,
            "scorecard_status": None,
            "email": {},
            "details": {},
            "scores": {},
        }
    post_status = response.status_code
    if response.url == url1:
        return False, {
            "login_ok": False,
            "login_status": post_status,
            "scorecard_status": None,
            "email": {},
            "details": {},
            "scores": {},
        }

    email = get_email(response.text)
    response = session.get(url2)
    scorecard_html = response.text
    student_details = extract_student_details(scorecard_html)
    response = session.get(url3)
    if response.status_code == 200:
        js_html = response.text
        js_details = extract_student_details(js_html)
        if js_details and extract_scores_only(js_details):
            scorecard_html = js_html
            student_details = js_details
    should_retry = False
    if email.get("Name") != "Not Found" and student_details.get("name"):
        if normalize_name(email["Name"]) != normalize_name(student_details["name"]):
            should_retry = True
            session = requests.Session()
            response = session.get(url1)
            soup = BeautifulSoup(response.content, 'html.parser')
            token = soup.find("input", {"name": "__RequestVerificationToken"})['value']
            data = {
                "__RequestVerificationToken": token,
                "RegNo": regno,
                "DOB": dob,
            }
            response = session.post(url1, data=data)
            post_status = response.status_code
            if response.url != url1:
                email = get_email(response.text)
                response = session.get(url2)
                scorecard_html = response.text
                student_details = extract_student_details(scorecard_html)
    name_mismatch = False
    if email.get("Name") != "Not Found" and student_details.get("name"):
        name_mismatch = normalize_name(email["Name"]) != normalize_name(student_details["name"])
    scores_only = extract_scores_only(student_details)
    details_only = {k: v for k, v in student_details.items() if k not in scores_only}
    regno_dict = {"regno": regno}
    return {**regno_dict, **email, **student_details}, {
        "login_ok": True,
        "login_status": post_status,
        "scorecard_status": response.status_code,
        "email": email,
        "details": details_only,
        "scores": scores_only,
        "scorecard_html": scorecard_html,
        "name_mismatch": name_mismatch,
    }

def append_log_row(row):
    fieldnames = list(log_fields)
    write_header = not os.path.exists(logfile) or os.path.getsize(logfile) == 0
    if not write_header:
        with open(logfile, newline="") as f:
            reader = csv.reader(f)
            existing_header = next(reader, [])
        if existing_header != fieldnames:
            with open(logfile, newline="") as f:
                existing_rows = list(csv.DictReader(f, restkey="__extra__"))
            merged_header = existing_header + [f for f in fieldnames if f not in existing_header]
            with open(logfile, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=merged_header, extrasaction="ignore")
                writer.writeheader()
                for existing in existing_rows:
                    existing.pop("__extra__", None)
                    writer.writerow(existing)
            fieldnames = merged_header
    with open(logfile, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)

def normalize_dob(raw_dob):
    if not raw_dob or pd.isna(raw_dob):
        return None
    if isinstance(raw_dob, (int, float)):
        raw_dob = str(raw_dob)
    raw_dob = str(raw_dob).strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(raw_dob, fmt)
            return parsed.strftime("%d/%m/%Y")
        except ValueError:
            continue
    parts = re.split(r"[/-]", raw_dob)
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        day, month, year = parts
        return f"{int(day):02d}/{int(month):02d}/{year}"
    return None

def scrape(regno, dob):
    result = None
    log_data = None
    for attempt in range(1, max_login_attempts + 1):
        result, log_data = login(regno, dob)
        if log_data["login_ok"]:
            break
    all_scores_na = False
    if log_data["login_ok"] and log_data["scores"]:
        all_scores_na = all(str(v).strip().upper() == "NA" for v in log_data["scores"].values())
    html_path = ""
    if log_data["login_ok"] and (not log_data["scores"] or log_data.get("name_mismatch") or all_scores_na):
        os.makedirs(html_dump_dir, exist_ok=True)
        html_path = os.path.join(html_dump_dir, f"{regno}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(log_data.get("scorecard_html", ""))
    if log_data.get("name_mismatch"):
        result = None
    log_row = {
        "regno": regno,
        "dob": dob,
        "login_ok": log_data["login_ok"],
        "login_status": log_data["login_status"],
        "scorecard_status": log_data["scorecard_status"],
        "email": json.dumps(log_data["email"], ensure_ascii=True),
        "details": json.dumps(log_data["details"], ensure_ascii=True),
        "scores": json.dumps(log_data["scores"], ensure_ascii=True),
        "scores_count": len(log_data["scores"]),
        "scorecard_html_path": html_path,
        "name_mismatch": log_data.get("name_mismatch", False),
        "retry_count": attempt,
        "all_scores_na": all_scores_na,
    }
    with lock:
        append_log_row(log_row)
    return result

def scrape_with_dob_guess(regno):
    last_log_data = None
    attempts = 0
    for yyyy in [2011, 2012]:
        for mm in range(1, 13):
            for dd in range(1, 32):
                try:
                    datetime(yyyy, mm, dd)
                except ValueError:
                    continue
                attempts += 1
                dob = f"{dd:02d}/{mm:02d}/{yyyy}"
                result, log_data = login(regno, dob)
                last_log_data = log_data
                if log_data["login_ok"]:
                    all_scores_na = False
                    if log_data["scores"]:
                        all_scores_na = all(str(v).strip().upper() == "NA" for v in log_data["scores"].values())
                    html_path = ""
                    if not log_data["scores"] or log_data.get("name_mismatch") or all_scores_na:
                        os.makedirs(html_dump_dir, exist_ok=True)
                        html_path = os.path.join(html_dump_dir, f"{regno}.html")
                        with open(html_path, "w", encoding="utf-8") as f:
                            f.write(log_data.get("scorecard_html", ""))
                    if log_data.get("name_mismatch"):
                        result = None
                    log_row = {
                        "regno": regno,
                        "dob": dob,
                        "login_ok": log_data["login_ok"],
                        "login_status": log_data["login_status"],
                        "scorecard_status": log_data["scorecard_status"],
                        "email": json.dumps(log_data["email"], ensure_ascii=True),
                        "details": json.dumps(log_data["details"], ensure_ascii=True),
                        "scores": json.dumps(log_data["scores"], ensure_ascii=True),
                        "scores_count": len(log_data["scores"]),
                        "scorecard_html_path": html_path,
                        "name_mismatch": log_data.get("name_mismatch", False),
                        "retry_count": attempts,
                        "all_scores_na": all_scores_na,
                    }
                    with lock:
                        append_log_row(log_row)
                    return result, dob
    fallback = last_log_data or {
        "login_ok": False,
        "login_status": None,
        "scorecard_status": None,
        "email": {},
        "details": {},
        "scores": {},
    }
    log_row = {
        "regno": regno,
        "dob": "",
        "login_ok": fallback["login_ok"],
        "login_status": fallback["login_status"],
        "scorecard_status": fallback["scorecard_status"],
        "email": json.dumps(fallback["email"], ensure_ascii=True),
        "details": json.dumps(fallback["details"], ensure_ascii=True),
        "scores": json.dumps(fallback["scores"], ensure_ascii=True),
        "scores_count": len(fallback["scores"]),
        "scorecard_html_path": "",
        "name_mismatch": False,
        "retry_count": attempts,
        "all_scores_na": False,
    }
    with lock:
        append_log_row(log_row)
    return None, ""
lock = threading.Lock()
outputfile = "main_scored.csv"
existing_regnos = set()
def add_result(regno, dob, base_row):
    with lock:
        if regno in existing_regnos:
            print(f"Skipping {regno} as it already exists in the file.")
            return
    print(f"Scraping result for {regno} with dob {dob}")
    result = scrape(regno, dob)
    with lock:
        write_header = not os.path.exists(outputfile) or os.path.getsize(outputfile) == 0
        combined = dict(base_row)
        combined["regno"] = regno
        combined["dob_used"] = dob
        combined["scrape_ok"] = bool(result)
        if result:
            combined.update(result)
            scores = {k: v for k, v in result.items() if k not in detail_keys}
            if scores:
                score_parts = []
                for key in sorted(scores.keys()):
                    value = str(scores[key]).strip()
                    if value:
                        score_parts.append(f"{key}={value}")
                combined["score"] = "; ".join(score_parts)
            else:
                combined["score"] = ""
            existing_regnos.add(regno)
            print(f"Added result for {regno}")
            print(result)
        else:
            print(f"Could not find result for {regno}")
        with open(outputfile, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow(combined)

def add_result_bruteforce(regno, base_row):
    with lock:
        if regno in existing_regnos:
            print(f"Skipping {regno} as it already exists in the file.")
            return
    print(f"Scraping result for {regno} with DOB guessing")
    result, dob_used = scrape_with_dob_guess(regno)
    with lock:
        write_header = not os.path.exists(outputfile) or os.path.getsize(outputfile) == 0
        combined = dict(base_row)
        combined["regno"] = regno
        combined["dob_used"] = dob_used
        combined["scrape_ok"] = bool(result)
        if result:
            combined.update(result)
            scores = {k: v for k, v in result.items() if k not in detail_keys}
            if scores:
                score_parts = []
                for key in sorted(scores.keys()):
                    value = str(scores[key]).strip()
                    if value:
                        score_parts.append(f"{key}={value}")
                combined["score"] = "; ".join(score_parts)
            else:
                combined["score"] = ""
            existing_regnos.add(regno)
            print(f"Added result for {regno}")
            print(result)
        else:
            print(f"Could not find result for {regno}")
        with open(outputfile, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow(combined)

def retry_missing_scores():
    if not os.path.exists(outputfile) or os.path.getsize(outputfile) == 0:
        return
    updated_rows = []
    with open(outputfile, newline="") as f:
        reader = csv.DictReader(f, restkey="__extra__")
        fieldnames = reader.fieldnames or []
        for row in reader:
            row.pop("__extra__", None)
            score = (row.get("score") or "").strip()
            if score:
                updated_rows.append(row)
                continue
            regno = (row.get("Enrollment Number") or row.get("regno") or "").strip()
            dob = normalize_dob(row.get("DOB") or row.get("dob_used") or "")
            if not regno or not dob:
                updated_rows.append(row)
                continue
            result = None
            for _ in range(max_missing_score_retries):
                result = scrape(regno, dob)
                if result:
                    scores = {k: v for k, v in result.items() if k not in detail_keys}
                    if scores:
                        score_parts = []
                        for key in sorted(scores.keys()):
                            value = str(scores[key]).strip()
                            if value:
                                score_parts.append(f"{key}={value}")
                        row["score"] = "; ".join(score_parts)
                        row["scrape_ok"] = True
                        row["regno"] = regno
                        row["dob_used"] = dob
                        for key, value in result.items():
                            if key in row:
                                row[key] = value
                        break
            updated_rows.append(row)
    with open(outputfile, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in updated_rows:
            writer.writerow(row)
                
if __name__ == "__main__":
    max_workers = int(input("Enter the number of workers: "))
    use_bruteforce = "--issue" in sys.argv
    if use_bruteforce:
        outputfile = issue_outputfile
    if os.path.exists(outputfile) and os.path.getsize(outputfile) > 0:
        with open(outputfile, newline="") as f:
            reader = csv.DictReader(f, restkey="__extra__")
            for row in reader:
                regno = row.get("regno")
                if regno:
                    existing_regnos.add(regno)
    source_file = issue_file if use_bruteforce else "main.csv"
    with open(source_file, newline="") as f:
        reader = csv.DictReader(f)
        base_rows = []
        for idx, row in enumerate(reader, start=1):
            cleaned = {(k or "").strip(): (v or "").strip() for k, v in row.items()}
            cleaned["source_row"] = str(idx)
            base_rows.append(cleaned)
    if not base_rows:
        raise ValueError("main.csv must include data rows.")
    if "Enrollment Number" not in base_rows[0] or "DOB" not in base_rows[0]:
        raise ValueError("main.csv must include Enrollment Number and DOB columns.")
    output_fields = list(base_rows[0].keys()) + [
        "regno",
        "dob_used",
        "scrape_ok",
        "score",
        "Name",
        "Mobile",
        "Email",
        "name",
        "parent_name",
        "gender",
        "class_standard",
        "roll_no",
        "date_of_birth",
        "exam_center",
    ]
    records = []
    for row in base_rows:
        regno = str(row.get("Enrollment Number", "")).strip()
        dob = normalize_dob(row.get("DOB", ""))
        if not regno or regno == "nan" or not dob:
            combined = dict(row)
            combined["regno"] = regno
            combined["dob_used"] = dob or ""
            combined["scrape_ok"] = False
            with lock:
                with open(outputfile, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
                    if not os.path.exists(outputfile) or os.path.getsize(outputfile) == 0:
                        writer.writeheader()
                    writer.writerow(combined)
            continue
        records.append((regno, dob, row))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for regno, dob, row in records:
            if use_bruteforce:
                futures.append(executor.submit(add_result_bruteforce, regno, row))
            else:
                futures.append(executor.submit(add_result, regno, dob, row))
        for future in futures:
            future.result()
    if not use_bruteforce:
        retry_missing_scores()
    
