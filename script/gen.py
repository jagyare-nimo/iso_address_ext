import argparse
import pandas as pd
import numpy as np
import uuid
import random
import string
from pathlib import Path

# Constants for postcode generation
_UK_OUTWARDS = ["SW1A", "EC1A", "W1A", "M1", "B33", "CR2", "DN55"]
_UK_INWARDS = ["1AA", "2BB", "3CC", "4DD", "5EE", "6FF"]

def _random_uk_postcode():
    return f"{random.choice(_UK_OUTWARDS)} {random.choice(_UK_INWARDS)}"


def _random_ca_postcode():
    letters = string.ascii_uppercase
    digits = string.digits
    return (
        f"{random.choice(letters)}{random.choice(digits)}{random.choice(letters)} "
        f"{random.choice(digits)}{random.choice(letters)}{random.choice(digits)}"
    )


def _random_us_postcode():
    return f"{np.random.randint(10000, 99999)}"


def _random_de_postcode():
    return f"{np.random.randint(10000, 99999)}"


def _random_fr_postcode():
    return f"{np.random.randint(10000, 99999)}"


def generate_raw_data(n: int, output_file: Path):
    """
    Generate mixed-country raw address data in Excel with columns:
    ID, ADDRESSLINE1, ADDRESSLINE2, ADDRESSLINE3
    """
    # Street templates
    street_names = [
        "Maple Street", "Cedar Lane", "Pine Avenue", "Birch Road", "Elm Drive",
        "Wellington Street", "Granville Avenue", "Yonge Boulevard", "Queen's Avenue",
        "King's Road", "Station Road", "High Street", "Victoria Terrace",
        "Saint-Catherine O", "17th Avenue NW"
    ]
    # Country-specific lists
    us_states = ["CA", "TX", "WA", "MA", "FL", "NY", "IL", "PA", "OH", "GA"]
    ca_provinces = ["ON", "BC", "QC", "AB", "MB"]
    uk_cities = ["Cambridge", "Manchester", "Bristol", "Edinburgh", "London"]
    de_cities = ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"]
    fr_cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]
    country_codes = ["US", "CA", "UK", "DE", "FR"]

    ids = [str(uuid.uuid4()) for _ in range(n)]
    house_nums = np.random.randint(1, 2000, size=n)
    streets = np.random.choice(street_names, size=n)
    # Compose ADDRESSLINE1
    line1 = [f"{h} {s}" for h, s in zip(house_nums, streets)]

    # Randomly assign a country code for each row
    country_pick = np.random.choice(country_codes, size=n)

    line2 = []
    line3 = []
    for idx, ctry in enumerate(country_pick):
        if ctry == "US":
            city = np.random.choice(["Springfield", "Austin", "Seattle", "Boston", "Miami"])
            state = np.random.choice(us_states)
            pcode = _random_us_postcode()
            line2.append(f"{city}, {state} {pcode}")
        elif ctry == "CA":
            city = np.random.choice(["Ottawa", "Vancouver", "Toronto", "Edmonton", "Montréal"])
            prov = np.random.choice(ca_provinces)
            pcode = _random_ca_postcode()
            line2.append(f"{city}, {prov} {pcode}")
        elif ctry == "UK":
            city = np.random.choice(uk_cities)
            pcode = _random_uk_postcode()
            line2.append(f"{city} {pcode}")
        elif ctry == "DE":
            city = np.random.choice(de_cities)
            pcode = _random_de_postcode()
            line2.append(f"{city} {pcode}")
        else:  # FR
            city = np.random.choice(fr_cities)
            pcode = _random_fr_postcode()
            line2.append(f"{city} {pcode}")
        # ADDRESSLINE3 is simply the country code
        line3.append(ctry)

    df = pd.DataFrame({
        "ID": ids,
        "ADDRESSLINE1": line1,
        "ADDRESSLINE2": line2,
        "ADDRESSLINE3": line3
    })

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_file, index=False)
    print(f"Generated {n:,} rows → {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate raw mixed-country address Excel test data"
    )
    parser.add_argument(
        "--count", "-c", type=int, default=100,
        help="Number of rows to generate"
    )
    parser.add_argument(
        "--out", "-o", type=str,
        default="raw_addresses.xlsx",
        help="Output XLSX file path"
    )
    args = parser.parse_args()
    generate_raw_data(args.count, Path(args.out))
