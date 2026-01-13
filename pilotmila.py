import requests
import pdfplumber
import pandas as pd
import os
import json
from time import sleep
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from io import BytesIO
import warnings
from typing import Dict, Any
from pdf2image import convert_from_bytes
import pytesseract

# just so terminal looks cleaner, csv unaffected
warnings.filterwarnings("ignore", message="Could not get FontBBox")
warnings.filterwarnings("ignore", message="Could not get FontBBox from font descriptor")
warnings.filterwarnings("ignore", message="Not adding object")
warnings.filterwarnings("ignore", category=UserWarning)

# constants for the timeframe of the study
START_YEAR = 2004 #1995 (actual start date)
END_YEAR = 2004 #2015 (actual end date)
OUTPUT_FOLDER = "downloads"
RESULTS_CSV = "law_review_prelim_results.csv"

# implementation for general url pattern for duke law review journals
BASE_URL = "https://scholarship.law.duke.edu/dlj/vol{volume}/iss{issue}/{article}/"
DUKE_JOURNAL_START_YEAR = 1951

# pepperdine law review URL and journal start date
PEPPERDINE_BASE_URL = "https://digitalcommons.pepperdine.edu/plr/vol{volume}/iss{issue}/{article}/"
PEPPERDINE_JOURNAL_START_YEAR = 1974 # plr start year similar to duke 1951

# simple functions

def download_pdf(url):
    """download PDF from URL and return info (now includes author metadata)""" # <--- MODIFIED DOCSTRING
    try:
        print(f" downloading: {url}")
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f" error: page not found ({response.status_code})")
            return None, None, None, None 
        
        # using bs4 to find the actual pdf link
        soup = BeautifulSoup(response.text, "html.parser")
        
        # title extraction from html (existing logic)
        page_title = None
        citation_meta = soup.find("meta", {"name": "citation_title"})
        if citation_meta:
            page_title = citation_meta.get("content")
        else:
            title_elem = soup.find("h1") or soup.find("h2")
            if title_elem:
                page_title = title_elem.get_text(strip=True)
        
        # author metadata extraction (works for both Duke and Pepperdine)
        authors = []
        
        # Method 1: Try citation_author meta tags (Duke)
        author_meta_tags = soup.find_all("meta", {"name": "citation_author"})
        authors = [tag.get("content") for tag in author_meta_tags if tag.get("content")]
        
        # Method 2: If no meta tags found, try Pepperdine HTML structure
        # Look for author links in the "Authors" section
        if not authors:
            author_links = soup.find_all("a", href=True)
            seen_authors = set()
            for link in author_links:
                href = link.get("href", "")
                if "author=" in href or "q=author" in href:
                    author_name = link.get_text(strip=True)
                    if author_name and author_name not in seen_authors:
                        authors.append(author_name)
                        seen_authors.add(author_name)
        
        author_count = len(authors)
        is_multi_author = author_count > 1
        authors_string = " and ".join(authors) if authors else "Unknown Author"
        
        # print what we found
        if author_count > 0:
            print(f"  Authors found: {authors_string} (count: {author_count})")
        
        # pdf link extraction (existing logic)
        pdf_link = None
        for a in soup.find_all("a", href=True):
            if "viewcontent.cgi" in a["href"]:
                pdf_link = a["href"]
                break
        if not pdf_link:
            print(" no pdf link found on page.")
            return None, None, None, None
        
        pdf_url = urljoin(url, pdf_link)
        print(f" downloading pdf: {pdf_url}")

        pdf_response = requests.get(pdf_url, timeout=30)
        if pdf_response.status_code == 200 and 'application/pdf' in pdf_response.headers.get('Content-Type', ''):
            return pdf_response.content, page_title, authors_string, is_multi_author # modified return to include authors_string and is_multi_author
        else:
            print(f" error: pdf not returned ({pdf_response.status_code})")
            return None, None, None, None
        
    except Exception as e:
        print(f" Error: {e}")
        return None, None, None, None


def ocr_scanned_pdf(pdf_content):
    # OCR for scanned pdfs using pdf2image + pytesseract
    # converts PDF to images then extracts text using OCR
   
    try:
        print(f" running OCR (this may take a few seconds...")
        
        # convert PDF bytes to images
        images = convert_from_bytes(pdf_content, poppler_path='/opt/homebrew/bin')
        
        full_text = ""
        for page_num, image in enumerate(images, 1):
            print(f" Processing page {page_num}/{len(images)}...")
            # OCR each page
            text = pytesseract.image_to_string(image)
            full_text += text + " "
        
        # calculate metrics
        word_count = len(full_text.split())
        char_count_total = len(full_text)
        char_count_no_space = len(''.join(c for c in full_text if not c.isspace()))
        
        return {
            'words': word_count,
            'char_count_total': char_count_total,
            'char_count_no_space': char_count_no_space,
            'ocr_used': True
        }
    except Exception as e:
        print(f" OCR failed: {e}")
        return None


def extract_pdf_metadata(pdf_content):
    
    # extracts word count and character counts from PDF bytes.
    
    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            
            # internal debugging
            num_pages = len(pdf.pages)
            
            # all pages
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + " "

            # for debugging below
            if num_pages > 0 and not full_text.strip():
                print(f" WARNING: PDF has {num_pages} pages, but NO TEXT was extracted.")
                print(" article is likely scanned (image-only) - will attempt OCR.")

            # word estimation
            word_count = len(full_text.split())

            # char count total
            total_char_count = len(full_text)
            
            # char count no spaces whitespace excluded
            char_count_no_space = len(''.join(c for c in full_text if not c.isspace()))

            # title from first page
            first_page_text = pdf.pages[0].extract_text() if pdf.pages else ""
            title = extract_title_from_text(first_page_text)

            return {
                'words': word_count,
                'char_count_total': total_char_count,
                'char_count_no_space': char_count_no_space,
                'title': title,
                'ocr_used': False
            }
    except Exception as e:
        print(f" PDF parsing FATAL error: {e}")
        return None


def extract_title_from_text(text):
    # attempt to extract article title from first page text
    if not text:
        return "unknown title"
    
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    for line in lines[:5]:
        if len(line) > 20 and len(line) < 200:
            return line
    
    return lines[0] if lines else "unknown title"


def scrape_law_journal(journal_name, base_url, journal_start_year, start_year, end_year): # changed input params here
    # generic scraping function for digital common journals to work for both 
    print(f" scraping {journal_name} ({start_year}---{end_year})") 
    print(f" saving downloads to: {OUTPUT_FOLDER}/\n")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    results = []

    for year in range(start_year, end_year + 1):
        # volume calc based on specific journal start year
        volume = year - journal_start_year + 1 # logic for general law journal scraper for consistency between duke and pepperdine
        print(f"\n Year {year} (Volume {volume})")

        for issue in range(1, 7):
            print(f" Issue {issue}:")

            articles_found = 0
            consecutive_failures = 0

            for article_num in range(1, 21):
                if consecutive_failures >= 3:
                    break

                # use the provided base_url
                url = base_url.format(volume=volume, issue=issue, article=article_num) # more generic

                # now includes author data
                pdf_content, page_title, authors_string, is_multi_author = download_pdf(url) # modified func call

                if pdf_content:
                    metadata = extract_pdf_metadata(pdf_content)

                    if metadata:
                        # if pdf extraction fails, uses html title
                        if metadata['title'] == "unknown title" and page_title:
                            metadata['title'] = page_title
                        
                        # OCR handling for scanned PDFs
                        if metadata['words'] == 0:
                            print(f"  Scanned PDF detected 0 words - trying OCR...")
                            ocr_result = ocr_scanned_pdf(pdf_content)
                            
                            if ocr_result and ocr_result['words'] > 0:
                                # updates metadata with OCR results with accurate words
                                metadata['words'] = ocr_result['words']
                                metadata['char_count_total'] = ocr_result['char_count_total']
                                metadata['char_count_no_space'] = ocr_result['char_count_no_space']
                                metadata['ocr_used'] = True
                                print(f" OCR successful: {metadata['words']} words extracted")
                            else:
                                print(f" OCR failed - skipping article {article_num}")
                                consecutive_failures += 1
                                continue
                        
                        # saving purposes
                        filename = f"{journal_name.replace(' ', '_').lower()}_{year}_vol{volume}_iss{issue}_art{article_num}.pdf" # modified file name
                        filepath = os.path.join(OUTPUT_FOLDER, filename)
                        with open(filepath, "wb") as f:
                            f.write(pdf_content)

                        # storing results (uses char counts and omits pages)
                        results.append({
                            'journal': journal_name, 
                            'year': year,
                            'volume': volume,
                            'issue': issue,
                            'article': article_num,
                            'title': metadata['title'],
                            'authors': authors_string,
                            'multi_author': is_multi_author,
                            'words': metadata['words'],
                            'char_count_total': metadata['char_count_total'],
                            'char_count_no_space': metadata['char_count_no_space'],
                            'ocr_used': metadata.get('ocr_used', False),
                            'url': url,
                            'filename': filename
                        })

                        # success print msg to verify works
                        ocr_marker = " [OCR]" if metadata.get('ocr_used') else ""
                        author_marker = " [Multi-Author]" if is_multi_author else "" # new print marker for multi-authorship
                        print(f" Article {article_num}: {metadata['words']} words / {metadata['char_count_no_space']} chars{ocr_marker}{author_marker}") # <--- MODIFIED PRINT
                        articles_found += 1
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                else:
                    consecutive_failures += 1
                
                sleep(0.5)

            if articles_found == 0:
                print(f" no articles found")
                break

    return results

# author metadata generation and analysis


def generate_author_metadata_json(df, output_filename='author_metadata.json'):
    # Creates structured JSON of author metadata for trend analysis (solo vs. multi author)
    author_metadata = {
        'metadata': {
            'total_articles': len(df),
            'analysis_period': f"{int(df['year'].min())}-{int(df['year'].max())}",
            'journals': list(df['journal'].unique())
        },
        'authorship_summary': {
            'multi_author_count': int(df['multi_author'].sum()),
            'single_author_count': int((~df['multi_author']).sum()),
            'multi_author_percentage': float((df['multi_author'].sum() / len(df)) * 100),
        },
        'authorship_by_year': {},
        'authorship_by_journal': {},
        'articles': []
    }
    
    # Breakdown by year
    for year in sorted(df['year'].unique()):
        year_data = df[df['year'] == year]
        multi_count = year_data['multi_author'].sum()
        author_metadata['authorship_by_year'][int(year)] = {
            'total_articles': len(year_data),
            'multi_author': int(multi_count),
            'single_author': int(len(year_data) - multi_count),
            'multi_author_percentage': float((multi_count / len(year_data)) * 100) if len(year_data) > 0 else 0,
            'avg_words_multi': float(year_data[year_data['multi_author']]['words'].mean()) if multi_count > 0 else None,
            'avg_words_single': float(year_data[~year_data['multi_author']]['words'].mean()) if (len(year_data) - multi_count) > 0 else None,
        }
    
    # Breakdown by journal
    for journal in df['journal'].unique():
        journal_data = df[df['journal'] == journal]
        multi_count = journal_data['multi_author'].sum()
        author_metadata['authorship_by_journal'][journal] = {
            'total_articles': len(journal_data),
            'multi_author': int(multi_count),
            'single_author': int(len(journal_data) - multi_count),
            'multi_author_percentage': float((multi_count / len(journal_data)) * 100) if len(journal_data) > 0 else 0,
            'avg_words_multi': float(journal_data[journal_data['multi_author']]['words'].mean()) if multi_count > 0 else None,
            'avg_words_single': float(journal_data[~journal_data['multi_author']]['words'].mean()) if (len(journal_data) - multi_count) > 0 else None,
        }
    
    # Individual article records
    for _, row in df.iterrows():
        article = {
            'title': row['title'],
            'authors': row['authors'],
            'author_count': 2 if row['multi_author'] else 1,
            'is_multi_author': bool(row['multi_author']),
            'journal': row['journal'],
            'year': int(row['year']),
            'volume': int(row['volume']),
            'issue': int(row['issue']),
            'article_number': int(row['article']),
            'words': int(row['words']),
            'char_count_no_space': int(row['char_count_no_space']),
            'ocr_used': bool(row['ocr_used']),
            'url': row['url']
        }
        author_metadata['articles'].append(article)
    
    # Save to JSON
    with open(output_filename, 'w') as f:
        json.dump(author_metadata, f, indent=2)
    
    print(f"\n✓ Author metadata saved to {output_filename}")
    return author_metadata


def analyze_authorship_trends(df):
    # trend analysis between multi and solo authors
    print("\n" + "="*60)
    print("Authorship Trend Analysis")
    print("="*60)
    
    # Overall stats
    multi_author_df = df[df['multi_author']]
    single_author_df = df[~df['multi_author']]
    
    print(f"\nOVERALL STATISTICS:")
    print(f"  Total articles: {len(df)}")
    print(f"  Multi-author articles: {len(multi_author_df)} ({len(multi_author_df)/len(df)*100:.1f}%)")
    print(f"  Single-author articles: {len(single_author_df)} ({len(single_author_df)/len(df)*100:.1f}%)")
    
    # Word count comparison
    print(f"\nWORD COUNT COMPARISON:")
    print(f"  Multi-author avg: {multi_author_df['words'].mean():.0f} words")
    print(f"  Single-author avg: {single_author_df['words'].mean():.0f} words")
    print(f"  Difference: {multi_author_df['words'].mean() - single_author_df['words'].mean():.0f} words")
    
    # Character count comparison
    print(f"\nCHARACTER COUNT COMPARISON:")
    print(f"  Multi-author avg: {multi_author_df['char_count_no_space'].mean():.0f} chars")
    print(f"  Single-author avg: {single_author_df['char_count_no_space'].mean():.0f} chars")
    
    # Year-over-year trend
    print(f"\nMULTI-AUTHORSHIP TREND BY YEAR:")
    for year in sorted(df['year'].unique()):
        year_data = df[df['year'] == year]
        multi_pct = (year_data['multi_author'].sum() / len(year_data)) * 100
        print(f"  {int(year)}: {multi_pct:.1f}% multi-author")
    
    # Journal comparison
    print(f"\nMULTI-AUTHORSHIP BY JOURNAL:")
    for journal in df['journal'].unique():
        journal_data = df[df['journal'] == journal]
        multi_pct = (journal_data['multi_author'].sum() / len(journal_data)) * 100
        print(f"  {journal}: {multi_pct:.1f}% multi-author")


def save_results(results):

    df = pd.DataFrame(results)
    df.to_csv(RESULTS_CSV, index=False)
    df.to_json(RESULTS_CSV.replace('.csv', '.json'), orient='records', indent=2)
    
    print(f"\n Saved {len(results)} articles to {RESULTS_CSV}")
    print(f" Also saved to {RESULTS_CSV.replace('.csv', '.json')}")

    # summarizing the data
    print(f"\n Summary:")
    print(f"total articles: {len(results)}")
    print(f"year range: {df['year'].min()} - {df['year'].max()}")
    print(f"average words: {df['words'].mean():.0f}")
    print(f"average characters (total): {df['char_count_total'].mean():.0f}")
    print(f"average characters (no space): {df['char_count_no_space'].mean():.0f}")
    
    # OCR stats
    ocr_count = df['ocr_used'].sum()
    print(f"\nOCR stats:")
    print(f" articles processed with OCR: {ocr_count}")
    print(f" articles with direct text extraction: {len(df) - ocr_count}")
    
    # Author counts
    multi_author_count = df['multi_author'].sum()
    print(f"\nAuthor Stats:")
    print(f" articles with multiple authors: {multi_author_count}")
    print(f" articles with single author: {len(df) - multi_author_count}")
    
    # comparison timeline before/after 2005
    before_2005 = df[df['year'] < 2005]
    after_2005 = df[df['year'] >= 2005]

    print(f"\nbefore 2005: {len(before_2005)} articles")
    print(f" average words: {before_2005['words'].mean():.0f}")
    print(f" average chars (no space): {before_2005['char_count_no_space'].mean():.0f}")
    
    print(f"\nafter 2005: {len(after_2005)} articles")
    print(f" average words: {after_2005['words'].mean():.0f}")
    print(f" average chars (no space): {after_2005['char_count_no_space'].mean():.0f}")

    # generate author metadata JSON and analysis
    print("\n" + "="*60)
    generate_author_metadata_json(df)
    analyze_authorship_trends(df)
    print("="*60)

    return df


# main function
def main():
    print("=" * 60)
    print("law review length analysis (with OCR)")
    print("=" * 60)
    
    # 1. scrape Duke law journal
    duke_results = scrape_law_journal(
        journal_name='Duke Law Journal',
        base_url=BASE_URL,
        journal_start_year=DUKE_JOURNAL_START_YEAR,
        start_year=START_YEAR,
        end_year=END_YEAR
    )

    # 2. scrape Pepperdine law journal (new)
    pepperdine_results = scrape_law_journal(
        journal_name='Pepperdine Law Review',
        base_url=PEPPERDINE_BASE_URL,
        journal_start_year=PEPPERDINE_JOURNAL_START_YEAR,
        start_year=START_YEAR,
        end_year=END_YEAR
    )
    
    # combine results into csv
    results = duke_results + pepperdine_results

    if not results:
        print("no articles found. check url patterns and start years.")
        return
    
    df = save_results(results)

    print(f"\n next steps:")
    print(f"1. review {RESULTS_CSV} for data quality (now quantified by words/chars and author data)")
    print(f"2. resolve Poppler/OCR installation and path configuration.") 
    print(f"3. run citation scraper to see if that affects hypothesis")
    print(f"4. analyze correlation between length and citations, also considering multi-author trends.") 


if __name__ == "__main__":
    main()
