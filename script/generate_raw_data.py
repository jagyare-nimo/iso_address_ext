import argparse
import pandas as pd
import numpy as np
import uuid
import random
import string
from pathlib import Path

# UK postcode parts
_UK_OUTWARDS = ["SW1A", "EC1A", "W1A", "M1", "B33", "CR2", "DN55"]
_UK_INWARDS = ["1AA", "2BB", "3CC", "4DD", "5EE", "6FF"]


def _random_uk_postcode() -> str:
    return f"{random.choice(_UK_OUTWARDS)} {random.choice(_UK_INWARDS)}"


def _random_ca_postcode() -> str:
    L, D = string.ascii_uppercase, string.digits
    return f"{random.choice(L)}{random.choice(D)}{random.choice(L)} {random.choice(D)}{random.choice(L)}{random.choice(D)}"


def _random_us_postcode() -> str:
    return f"{np.random.randint(10000, 99999)}"


def _random_de_postcode() -> str:
    return f"{np.random.randint(10000, 99999)}"


def _random_fr_postcode() -> str:
    return f"{np.random.randint(10000, 99999)}"


def generate_mixed_addresses(n: int, output_file: Path):
    street_names = [
        "Maple Street", "Cedar Lane", "Pine Avenue", "Birch Road", "Elm Drive",
        "Wellington Street", "Granville Avenue", "Yonge Boulevard", "Queen's Avenue",
        "King's Road", "Station Road", "High Street", "Victoria Terrace",
        "Saint-Catherine O", "17th Avenue NW"
    ]
    us_states = ["CA", "TX", "WA", "MA", "FL", "NY", "IL", "PA", "OH", "GA"]
    ca_provs = ["ON", "BC", "QC", "AB", "MB"]
    uk_cities = ["Cambridge", "Manchester", "Bristol", "Edinburgh", "London"]
    de_cities = ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"]
    fr_cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]
    country_codes = ["US", "CA", "UK", "DE", "FR"]

    # Prepare columns
    ids = [str(uuid.uuid4()) for _ in range(n)]
    nums = np.random.randint(1, 2000, size=n)
    streets = np.random.choice(street_names, size=n)
    line1 = [f"{h} {s}" for h, s in zip(nums, streets)]
    picks = np.random.choice(country_codes, size=n)

    line2, line3 = [], []
    for ctry in picks:
        if ctry == "US":
            city = random.choice(["Springfield", "Austin", "Seattle", "Boston", "Miami"])
            state = random.choice(us_states)
            pc = _random_us_postcode()
            # **line2**: CITY, STATE POSTCODE
            line2.append(f"{city}, {state} {pc}")
        elif ctry == "CA":
            city = random.choice(["Ottawa", "Vancouver", "Toronto", "Edmonton", "Montréal"])
            prov = random.choice(ca_provs)
            pc = _random_ca_postcode()
            # **line2**: CITY, PROVINCE POSTCODE
            line2.append(f"{city}, {prov} {pc}")
        elif ctry == "UK":
            city = random.choice(uk_cities)
            pc = _random_uk_postcode()
            # **line2**: CITY, POSTCODE  (no province)
            line2.append(f"{city}, {pc}")
        elif ctry == "DE":
            city = random.choice(de_cities)
            pc = _random_de_postcode()
            # **line2**: CITY, POSTCODE
            line2.append(f"{city}, {pc}")
        else:  # FR
            city = random.choice(fr_cities)
            pc = _random_fr_postcode()
            # **line2**: CITY, POSTCODE
            line2.append(f"{city}, {pc}")

        # **line3** is always the two‐letter country code
        line3.append(ctry)

    # build dataframe
    df = pd.DataFrame({
        "ID": ids,
        "ADDRESSLINE1": line1,
        "ADDRESSLINE2": line2,
        "ADDRESSLINE3": line3,
    })

    # write
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_file, index=False)
    print(f"Generated {n:,} addresses → {output_file}")


def main():
    p = argparse.ArgumentParser(description="Generate mixed-address test data.")
    p.add_argument("--count", "-c", type=int, default=1000,
                   help="Number of rows to generate")
    p.add_argument("--out", "-o", type=str,
                   default="../resources/ici_sheets/raw/raw_input.xlsx",
                   help="Output Excel path")
    args = p.parse_args()

    out = Path(args.out)
    generate_mixed_addresses(args.count, out)


if __name__ == "__main__":
    main()
