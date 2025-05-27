import argparse
import pandas as pd
import numpy as np
import uuid
from pathlib import Path


def generate_mixed_addresses(n: int, output_file: Path):
    street_names = [
        "Maple Street", "Cedar Lane", "Pine Avenue", "Birch Road", "Elm Drive",
        "Wellington Street", "Granville Avenue", "Yon-ge Boulevard", "Queen's Avenue",
        "King's Road", "Station Road", "High Street", "Victoria Terrace",
        "Saint-Catherine O", "17th Avenue NW"
    ]

    us_states = ["CA", "TX", "WA", "MA", "FL", "NY", "IL", "PA", "OH", "GA"]
    ca_province = ["ON", "BC", "QC", "AB", "MB"]
    uk_cities = ["Cambridge", "Manchester", "Bristol", "Edinburgh", "London"]
    de_cities = ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"]
    fr_cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]
    countries = ["US", "CA", "UK", "DE", "FR"]

    ids = [str(uuid.uuid4()) for _ in range(n)]
    house_nums = np.random.randint(1, 2000, size=n)
    streets = np.random.choice(street_names, size=n)
    line1 = [f"{num} {st}" for num, st in zip(house_nums, streets)]

    country_pick = np.random.choice(countries, size=n)

    line2 = []
    line3 = []
    for ctry in country_pick:
        if ctry == "US":
            city = np.random.choice(["Springfield", "Austin", "Seattle", "Boston", "Miami"])
            state = np.random.choice(us_states)
            pcode = f"{np.random.randint(10000, 99999)}"
        elif ctry == "CA":
            city = np.random.choice(["Ottawa", "Vancouver", "Toronto", "Edmonton", "Montréal"])
            state = np.random.choice(ca_province)
            l1 = np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))
            d1 = np.random.randint(0, 10)
            l2 = np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))
            d2 = np.random.randint(0, 10)
            l3 = np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))
            d3 = np.random.randint(0, 10)
            pcode = f"{l1}{d1}{l2} {d2}{l3}{d3}"
        elif ctry == "UK":
            city = np.random.choice(uk_cities)
            state = ""
            out = f"{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.randint(1, 9)}"
            inward = f"{np.random.randint(0, 9)}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}"
            pcode = f"{out} {inward}"
        elif ctry == "DE":
            city = np.random.choice(de_cities)
            state = ""
            pcode = f"{np.random.randint(10000, 99999)}"
        else:  # FR
            city = np.random.choice(fr_cities)
            state = ""
            pcode = f"{np.random.randint(10000, 99999)}"

        line2.append(f"{city}, {state} {pcode}".strip())
        line3.append(ctry)

    df = pd.DataFrame({
        "ID": ids,
        "ADDRESSLINE1": line1,
        "ADDRESSLINE2": line2,
        "ADDRESSLINE3": line3
    })

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_file, index=False)
    print(f"Generated {n:,} rows -> {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate mixed-address Excel test data for DeepParse pipeline."
    )
    parser.add_argument(
        "--count", type=int, default=500000,
        help="Number of rows to generate (default: 500,000)"
    )
    parser.add_argument(
        "--out", type=str,
        default="resources/ici_sheets/raw/raw_input_1m.xlsx",
        help="Output Excel file path"
    )

    args = parser.parse_args()

    out_path = Path(args.out)
    generate_mixed_addresses(args.count, out_path)


if __name__ == "__main__":
    main()
