import psycopg2
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from time import sleep
import os
import csv
from abstract_extractor import AbstractExtractor, extract_abstract, extract_all_abstracts


# ============ DATABASE CONFIGURATION ============
DB_HOST = "postgres.0ussama.dev"
DB_PORT = 5432
DB_NAME = "openalex_db"
DB_USER = "openalex_app"
DB_PASSWORD = '{yszMR*[v"T`,z#31aAe9z1%8'

# SQL QUERY - Add your query here
SQL_QUERY = "SELECT work_id, doi FROM fact_works" 

# ============ SELENIUM CONFIGURATION ============
path_to_driver = r"C:\Users\HP\Desktop\Etudes\Python\web_scrapping\chromedriver.exe"
options = webdriver.ChromeOptions()
options.add_argument("--incognito")
options.add_argument("--headless")
options.add_argument("webdriver.chrome.driver=" + path_to_driver)
driver = webdriver.Chrome(options)

# ============ CREATE PAGES FOLDER ============
pages_folder = "pages"
if not os.path.exists(pages_folder):
    os.makedirs(pages_folder)
    print(f"Created folder: {pages_folder}")

# ============ CREATE CSV FILE FOR FAILED LINKS ============
errors_csv_file = open("failed_links.csv", "w", newline="", encoding="utf-8")
errors_csv_writer = csv.writer(errors_csv_file)
errors_csv_writer.writerow(["work_id", "doi", "error_message"])
print("Created failed_links.csv for error tracking")

# ============ CONNECT TO DATABASE ============
try:
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode='require'
    )
    cursor = connection.cursor()
    print("Connected to PostgreSQL database successfully")
except Exception as e:
    print(f"Error connecting to database: {e}")
    driver.quit()
    exit()

# ============ EXECUTE SQL QUERY ============
try:
    cursor.execute(SQL_QUERY)
    results = cursor.fetchall()
    print(f"Retrieved {len(results)} records from database")
except Exception as e:
    print(f"Error executing SQL query: {e}")
    cursor.close()
    connection.close()
    driver.quit()
    exit()

# ============ SCRAPE DOI PAGES ============
for row in results:
    work_id = row[0]  # First column should be the ID
    doi = row[1]  # Second column should be the DOI identifier
    
    try:
        # Construct full DOI URL if not already a complete URL
        if doi.startswith("http"):
            doi_link = doi
        else:
            doi_link = f"https://doi.org/{doi}"
        
        print(f"Scraping DOI for work ID: {work_id}")
        driver.get(doi_link)
        driver.maximize_window()
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        # Get the HTML code
        html_content = driver.page_source
        abstract = extract_abstract(html_content)

        if abstract is None:
            print(f"No abstract found for work ID {work_id}")
            continue
        # Save to file
        file_path = os.path.join(pages_folder, f"abstract_{work_id}.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(abstract)
        
        print(f"Saved: {file_path}")
    
    except Exception as e:
        print(f"Error scraping work ID {work_id}: {e}")
        # Write failed link to CSV
        errors_csv_writer.writerow([work_id, doi])

# ============ CLEANUP ============
cursor.close()
connection.close()
errors_csv_file.close()
driver.quit()
print("Scraping completed!")
print("Failed links saved to failed_links.csv")