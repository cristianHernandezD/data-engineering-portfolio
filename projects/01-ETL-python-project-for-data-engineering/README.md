# Project 03: Python Project for Data Engineering

This project is part of the **IBM Data Engineering Professional Certificate**.  
The objective was to create an ETL (Extract, Transform, Load) pipeline using **Python** and **SQLite3**.

---

## Project Overview
- **Extract:** Data was collected via **Web Scraping** from a Wikipedia page archived on the Wayback Machine.
- **Transform:** Data was cleaned and converted into multiple currencies (USD, GBP, EUR, INR).
- **Load:** The final dataset was saved as both a CSV file and into a SQLite database.

---

## Tools & Libraries
- **Python**  
- **BeautifulSoup** (HTML parsing & scraping)  
- **Requests** (HTTP requests)  
- **Pandas** & **NumPy** (Data manipulation)  
- **SQLite3** (Database storage)

---

## Files Included
- `code/banks_project.py` – Complete ETL pipeline code.
- `data/exchange_rate.csv` – Exchange rate values used for transformation.
- `data/Largest_banks_data.csv` – Output CSV file after ETL.
- `images/screenshot_etl.png` – Screenshot showing execution and results.
- `README.md` – Documentation of the project.

---

## Key Features
- Web scraping implementation using BeautifulSoup.
- Currency conversion with exchange rates from CSV.
- Loading data into both CSV and SQLite formats.
- Querying database to validate stored data.

---

## How It Works
1. **Extract:** Scrape bank names and market capitalization (USD) from Wikipedia.
2. **Transform:** Convert market capitalization to GBP, EUR, and INR using exchange rates.
3. **Load:** Save final dataset into:
   - A **CSV file** (`Largest_banks_data.csv`)
   - A **SQLite database** (`Banks.db`)

---

## Example Queries Executed
- Display all data:  
  ```sql
  SELECT * FROM Largest_banks;
  ```
- Average market cap in GBP:  
  ```sql
  SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
  ```
- Top 5 banks:  
  ```sql
  SELECT Name FROM Largest_banks LIMIT 5;
  ```

---

## Notes
- This repository is for **documentation and portfolio purposes only**.
- Original runtime environment is not included.
