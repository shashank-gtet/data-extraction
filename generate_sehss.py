import os
import time
from datetime import datetime, date, timedelta

# ================= CONFIG =================
ZONE = "central"

EAST_ROLLS = [
    "HSAS2500177",
    "HSOD2505560",
    "HSOD2505650",
    "HSWB2508759",
    "HSWB2510195",
    "HSWB2510201",
    "HSWB2510204",
    "HSWB2510208",
    "HSWB2510209",
    "HSWB2510211",
    "HSWB2510215",
    "HSWB2510217",
    "HSWB2510218",
    "HSWB2510220",
    "HSWB2510225",
    "HSWB2510254",
    "HSWB2510256",
    "HSWB2510274",
    "HSWB2510287",
    "HSWB2510330",
    "HSWB2510333",
    "HSWB2510340",
    "HSWB2510369",
    "HSWB2510427",
    "HSWB2510443",
]

WEST_ROLLS = [
    "HSGJ2501799",
    "HSGJ2501868",
    "HSMH2503761",
    "HSMH2503762",
    "HSMH2503881",
    "HSMH2503966",
    "HSMH2504051",
    "HSMH2504129",
    "HSMH2504213",
    "HSMH2504642",
    "HSMH2504654",
    "HSMH2504668",
    "HSMH2504747",
    "HSMH2504892",
    "HSMH2505048",
    "HSMH2505227",
    "HSRJ2506267",
    "HSRJ2506322",
    "HSRJ2506331",
    "HSRJ2506592",
    "HSRJ2506634",
    "HSRJ2506670",
    "HSRJ2506850",
    "HSRJ2510528",
    "HSRJ2510529",
]

NORTH_ROLLS = [
    "HSCH2500870",
    "HSCH2500891",
    "HSCH2500892",
    "HSCH2500896",
    "HSCH2500916",
    "HSCH2500920",
    "HSCH2500965",
    "HSCH2505730",
    "HSDL2501123",
    "HSHR2500909",
    "HSHR2502031",
    "HSHR2502032",
    "HSHR2502036",
    "HSHR2502098",
    "HSHR2502128",
    "HSHR2502289",
    "HSHR2502474",
    "HSHR2502494",
    "HSPB2505735",
    "HSPB2505771",
    "HSPB2505773",
    "HSPB2505777",
    "HSPB2505818",
    "HSPB2506004",
    "HSPB2506030",
]

SOUTH_ROLLS = [
    "HSKA2503016",
    "HSKA2503051",
    "HSKA2503065",
    "HSKA2503112",
    "HSKA2503285",
    "HSKA2503288",
    "HSKA2503327",
    "HSKA2503331",
    "HSKA2503334",
    "HSKA2503336",
    "HSKL2503407",
    "HSKL2503689",
    "HSTG2507186",
    "HSTG2507195",
    "HSTG2507269",
    "HSTN2506135",
    "HSTN2506151",
    "HSTN2507488",
    "HSTN2507501",
    "HSTN2507719",
    "HSTN2507799",
    "HSTN2507846",
    "HSTN2508244",
    "HSTN2508248",
    "HSTN2508370",
]

CENTRAL_ROLLS = [
    "HSBR2500487",
    "HSBR2500573",
    "HSBR2500609",
    "HSBR2500624",
    "HSBR2501441",
    "HSBR2506192",
    "HSBR2506329",
    "HSCG2500685",
    "HSMP2505288",
    "HSMP2505443",
    "HSMP2505457",
    "HSMP2505467",
    "HSUP2509133",
    "HSUP2509171",
    "HSUP2509252",
    "HSUP2509359",
    "HSUP2509642",
    "HSUP2509824",
    "HSUP2509885",
    "HSUP2510074",
    "HSUP2510075",
    "HSUP2510099",
    "HSUP2510145",
    "HSUP2510152",
    "HSUP2510188",
]

ROLLS_BY_ZONE = {
    "east": EAST_ROLLS,
    "west": WEST_ROLLS,
    "north": NORTH_ROLLS,
    "south": SOUTH_ROLLS,
    "central": CENTRAL_ROLLS,
}

DOB_START = date(2008, 1, 1)
DOB_END = date(2013, 12, 31)

OUTPUT_PATH = os.path.join("sehss-results", f"sehss-input-{ZONE}.txt")
LOG_EVERY = 1000
# =========================================


def generate_roll_numbers(zone):
    for roll in ROLLS_BY_ZONE.get(zone, []):
        yield roll


def generate_dobs(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current.strftime("%d-%m-%Y")
        current += timedelta(days=1)


def write_guess_file(roll_numbers, dobs, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    dobs_list = list(dobs)  # reuse DOBs for every roll
    start_time = time.time()
    count = 0

    with open(path, "w", encoding="utf-8") as f:
        for roll in roll_numbers:
            for dob in dobs_list:
                f.write(f"{roll} {dob}\n")
                count += 1

                if count % LOG_EVERY == 0:
                    elapsed = time.time() - start_time
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] "
                        f"Wrote {count} lines in {elapsed:.1f}s"
                    )


def main():
    print("Starting guess generation...")
    roll_numbers = generate_roll_numbers(ZONE)
    dobs = generate_dobs(DOB_START, DOB_END)
    write_guess_file(roll_numbers, dobs, OUTPUT_PATH)
    print(f"Done! Output written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
