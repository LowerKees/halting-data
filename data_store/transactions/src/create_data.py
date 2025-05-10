import csv
import itertools
import pathlib
import sys
import uuid

from faker import Faker
from faker.providers import bank, currency, python

fake = Faker()
fake.add_provider(bank)
fake.add_provider(python)
fake.add_provider(currency)


def main(file_path: pathlib.Path) -> None:
    field_names = ["id", "amt", "from", "to", "dts"]
    invalid_field_names = field_names + ["curr"]

    if file_path.name == "invalid.csv":
        # Take existing transaction ids from the valid csv file
        with open(file_path.parent / "valid.csv", "rt") as valid:
            reader = csv.DictReader(valid, fieldnames=invalid_field_names)
            first_five_records = []
            for row in list(reader)[1:6]:  # skip header row
                first_five_records.append(row)

        # Add additional column
        with open(file_path, mode="wt+", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=invalid_field_names)
            writer.writeheader()

            # add duplicate transaction ids
            for idx in range(0, 5):
                row = {}
                row[invalid_field_names[0]] = first_five_records[idx]["id"]
                row[invalid_field_names[1]] = fake.pyfloat(
                    min_value=-1_000_000.0, max_value=5_000_000.0
                )
                row[invalid_field_names[2]] = fake.iban()
                row[invalid_field_names[3]] = fake.iban()
                row[invalid_field_names[4]] = fake.date_time_this_decade()
                row[invalid_field_names[5]] = fake.currency_code()
                writer.writerow(rowdict=row)

            # add five duplicate records
            for r in first_five_records:
                r["curr"] = None
                writer.writerow(rowdict=r)

            # Insert five illegal domain values for amt (over 5 mln)
            for idx in range(1, 6):
                row = {}
                row[invalid_field_names[0]] = uuid.uuid4()
                row[invalid_field_names[1]] = fake.pyfloat(
                    min_value=5_000_000.0, max_value=6_000_000.0
                )
                row[invalid_field_names[2]] = fake.iban()
                row[invalid_field_names[3]] = fake.iban()
                row[invalid_field_names[4]] = fake.date_time_this_decade()
                row[invalid_field_names[5]] = fake.currency_code()
                writer.writerow(rowdict=row)

            # Add body to the invalid file
            for idx in range(15, 100_000):
                row = {}
                row[invalid_field_names[0]] = uuid.uuid4()
                row[invalid_field_names[1]] = fake.pyfloat(
                    min_value=-1_000_000, max_value=5_000_000.0
                )
                row[invalid_field_names[2]] = fake.iban()
                row[invalid_field_names[3]] = fake.iban()
                row[invalid_field_names[4]] = fake.date_time_this_decade()
                row[invalid_field_names[5]] = fake.currency_code()
                writer.writerow(rowdict=row)

    if file_path.name == "valid.csv":
        with open(file_path, mode="wt", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()

            for idx in range(1, 100_001):
                row = {}
                row[field_names[0]] = uuid.uuid4()
                row[field_names[1]] = fake.pyfloat(
                    min_value=-1_000_000.0, max_value=5_000_000.0
                )
                row[field_names[2]] = fake.iban()
                row[field_names[3]] = fake.iban()
                row[field_names[4]] = fake.date_time_this_decade()
                writer.writerow(rowdict=row)


if __name__ == "__main__":
    file_path = pathlib.Path(sys.argv[1])
    if not file_path.exists():
        raise FileNotFoundError(f"File not found at {sys.argv[1]}")

    main(file_path=file_path)
