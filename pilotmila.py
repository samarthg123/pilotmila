import requests
import pdfplumber
import pandas as pd
import os
from time import sleep
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from io import BytesIO
import warnings

# just so terminal looks cleaner, csv unaffected
warnings.filterwarnings("ignore", message="Could not get FontBBox")

# constants for the timeframe of the study

START_YEAR = 2004 #1995 (actual start date)
END_YEAR = 2006 #2015 (actual end date)
OUTPUT_FOLDER = "downloads"
RESULTS_CSV = "law_review_prelim_results.csv"

# implementation for general url pattern for duke law review journals

BASE_URL = "https://scholarship.law.duke.edu/dlj/vol{volume}/iss{issue}/{article}/"

# simple functions

def download_pdf(url):
    """download PDF from URL and return info"""
    try:
        print(f" downloading: {url}")
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f" error: page not found ({response.status_code})")
            return None
        
        # using bs4 to find the actual pdf link

        soup = BeautifulSoup(response.text, "html.parser")
        pdf_link = None
        for a in soup.find_all("a", href=True):
            if "viewcontent.cgi" in a["href"]:
                pdf_link = a["href"]
                break
        if not pdf_link:
            print(" no pdf link found on page.")
            return None
        
        pdf_url = urljoin(url, pdf_link)
        print(f" downloading pdf: {pdf_url}")

        pdf_response = requests.get(pdf_url, timeout = 30)
        if pdf_response.status_code == 200 and 'application/pdf' in pdf_response.headers.get('Content-Type', ''):
            return pdf_response.content
        else:
            print(f" error: pdf not returned ({pdf_response.status_code})")
            return None
        
    except Exception as e:
        print(f" Error: {e}")
        return None
    

def extract_pdf_metadata(pdf_content): #since most of the files from lawcommonsreview are PDFs
    """extract pages and word count from PDF bytes"""
    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            num_pages = len(pdf.pages)

            # all pages
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + " "

            # word estimation
            word_count = len(full_text.split())

            #title from first page
            first_page_text = pdf.pages[0].extract_text() if pdf.pages else ""
            title = extract_title_from_text(first_page_text)

            return {
                'pages': num_pages,
                'words': word_count,
                'title': title
            }
    except Exception as e:
        print(f" PDF parsing error: {e}")
        return None

def extract_title_from_text(text):
    """attempt to extract article title from first page text"""
    if not text:
        return "unknown title"
    
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    for line in lines[:5]:
        if len(line) > 20 and len(line) < 200:
            return line
    
    return lines[0] if lines else "unknown title"

def scrape_duke_law_journal(start_year=1995, end_year=2015):
    #duke law journal started 1951 and usually 6 issues per year/multiple artciles per issue
    print(f" scraping duke law journal ({start_year}---{end_year})")
    print(f" saving downloads to: {OUTPUT_FOLDER}/\n")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    results = []

    # volume calc
    # vol 1 = 1951 (when duke law journal began) ==> Vol X = 1951 + (X-1), hence year Y, vol = (Y - 1951) + 1

    for year in range(start_year, end_year + 1):
        volume = year - 1951 + 1
        print(f"\n Year {year} (Volume {volume})")

        for issue in range (1, 7):
            print(f" Issue {issue}:")

            # trying for articles 1-20 since most issues have <20 articles
            articles_found = 0
            consecutive_failures = 0

            for article_num in range(1, 21):
                # stopping if hit at least 3 404 errors
                if consecutive_failures >= 3:
                    break

                url = BASE_URL.format(volume=volume, issue=issue, article=article_num)

                # attempt to download

                pdf_content = download_pdf(url)

                if pdf_content:
                    metadata = extract_pdf_metadata(pdf_content)

                    if metadata:
                        # saving purposes
                        filename = f"duke_{year}_vol{volume}_iss{issue}_art{article_num}.pdf"
                        filepath = os.path.join(OUTPUT_FOLDER, filename)
                        with open(filepath, "wb") as f:
                            f.write(pdf_content)

                        # storing results

                        results.append ({
                            'journal':'Duke Law Journal',
                            'year': year,
                            'volume': volume,
                            'issue': issue,
                            'article': article_num,
                            'title': metadata['title'],
                            'pages': metadata['pages'],
                            'words': metadata['words'],
                            'url': url,
                            'filename': filename

                        })

                        print(f" Article {article_num}: {metadata['pages']} pages")
                        articles_found += 1
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                else:
                    consecutive_failures += 1
                
                sleep (0.5)

            if articles_found == 0:
                print(f" No articles found")
                break 

    # the break above allows for moving onto the next year in case there are no articles in for ex. issue 1

    return results

def save_results(results):

    df = pd.DataFrame(results)
    df.to_csv(RESULTS_CSV, index=False)
    print(f"\n Saved {len(results)} articles to {RESULTS_CSV}")

    # summarizing the data

    print(f"\n Summary:")
    print(f"Total articles: {len(results)}")
    print(f"Year range: {df['year'].min()} - {df['year'].max()}")
    print(f"Average pages: {df['pages'].mean():.1f}") # just to look cleaner
    print(f"Average words: {df['words'].mean():.0f}")

    # comparison timeline before/after 2005

    before_2005 = df[df['year'] < 2005]
    after_2005 = df[df['year'] >= 2005]

    print(f"\nBefore 2005: {len(before_2005)} articles, avg {before_2005['pages'].mean():.1f} pages")
    print(f"After 2005: {len(after_2005)} articles, avg {after_2005['pages'].mean():.1f} pages")

    return df


# main function

def main():
    print("=" * 60)
    print("law review length analysis")
    print("=" * 60)

    # scrape articles
    results = scrape_duke_law_journal(START_YEAR, END_YEAR)

    if not results:
        print("no articles found. check url pattern.")
        return
    
    df = save_results(results)

    print(f"\n next steps:")
    print(f"1. review {RESULTS_CSV} for data quality")
    print(f"2. run citation scraper to see if that affects hypothesis")
    print(f"3. analyze correlation between length and citations")

if __name__ == "__main__":
    main()