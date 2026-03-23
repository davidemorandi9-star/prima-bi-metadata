import sqlite3
from datetime import datetime

# Simple ANSI colors for Windows-compatible output
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"

def print_header(title):
    print(f"\n{BLUE}=== {title} ==={RESET}")

def print_row(row, columns):
    for col, val in zip(columns, row):
        print(f"{GREEN}{col}{RESET}: {val}")
    print("-" * 40)

def main():
    conn = sqlite3.connect("metadata.db")
    cur = conn.cursor()

    # Show tables
    print_header("TABLES")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cur.fetchall()]
    print(tables)

    # Show metadata content
    print_header("METADATA CONTENT")
    cur.execute("PRAGMA table_info(metadata);")
    columns = [col[1] for col in cur.fetchall()]

    cur.execute("SELECT * FROM metadata;")
    rows = cur.fetchall()

    if not rows:
        print(f"{YELLOW}No rows found in metadata table.{RESET}")
    else:
        for row in rows:
            print_row(row, columns)

    conn.close()

if __name__ == "__main__":
    main()
