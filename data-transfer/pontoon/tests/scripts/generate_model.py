import csv
import random
import uuid
from datetime import datetime, timedelta
import argparse

def generate_synthetic_data(file_path, num_records, num_customers, start_date, end_date):
    """Generates synthetic data and writes it to a CSV file."""
    # Parse date range
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Calculate total days in the range
    total_days = (end_date - start_date).days + 1
    
    if total_days <= 0:
        raise ValueError("End date must be after start date.")

    # Open the CSV file for writing
    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write header row
        writer.writerow(["id", "created_at", "updated_at", "customer_id", "name", "email", "score", "notes"])

        # Generate synthetic records
        for _ in range(num_records):
            # Generate unique ID
            record_id = str(uuid.uuid4())

            # Generate timestamps
            day_offset = random.randint(0, total_days - 1)
            created_date = start_date + timedelta(days=day_offset)
            created_time = created_date + timedelta(
                seconds=random.randint(0, 24 * 60 * 60 - 1)
            )

            updated_time = created_time + timedelta(
                seconds=random.randint(0, 60 * 60 * 24)  # Update within the same day or slightly later
            )

            # Generate other fields
            customer_id = f"Customer{random.randint(1, num_customers)}"
            name = f"User{random.randint(1, 10000)}"
            email = f"{name.lower()}@example.com"
            score = random.randint(0, 100)
            notes = f"This is a note for {name}."

            # Write record to CSV
            writer.writerow([
                record_id,
                created_time.isoformat(),
                updated_time.isoformat(),
                customer_id,
                name,
                email,
                score,
                notes,
            ])

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Generate synthetic data and save to CSV.")
    parser.add_argument("file_path", type=str, help="File path to save the CSV.")
    parser.add_argument("num_records", type=int, help="Number of records to generate.")
    parser.add_argument("num_customers", type=int, help="Number of customers to generate.")
    parser.add_argument("start_date", type=str, help="Start date (YYYY-MM-DD).")
    parser.add_argument("end_date", type=str, help="End date (YYYY-MM-DD).")

    # Parse arguments
    args = parser.parse_args()

    # Generate data
    generate_synthetic_data(
        args.file_path,
        args.num_records,
        args.num_customers,
        args.start_date,
        args.end_date,
    )
