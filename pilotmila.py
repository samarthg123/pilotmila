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
import warnings

warnings.filterwarnings("ignore", message="Could not get FontBBox")
warnings.filterwarnings("ignore", message="Could not get FontBBox from font descriptor")
warnings.filterwarnings("ignore", message="Not adding object")
warnings.filterwarnings("ignore", category=UserWarning)

# constants
START_YEAR = 2004 # 1995 actual
END_YEAR = 2004 # 2015 actual
OUTPUT_FOLDER = "downloads"
RESULTS_CSV = "law_review_prelim_results.csv"
FLAGGED_ISSUES_JSON = "flagged_issues.json"

# URLs
BASE_URL = "https://scholarship.law.duke.edu/dlj/vol{volume}/iss{issue}/{article}/"
DUKE_JOURNAL_START_YEAR = 1951

PEPPERDINE_BASE_URL = "https://digitalcommons.pepperdine.edu/plr/vol{volume}/iss{issue}/{article}/"
PEPPERDINE_JOURNAL_START_YEAR = 1974

# part 1) pdf downloading and metadata extraction

def download_pdf(url):
    # downloads PDFs from URL and extract metadata (title, authors)
    try:
        print(f" downloading: {url}")
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            print(f" error: page not found ({response.status_code})")
            return None, None, None, None
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # title extraction
        page_title = None
        citation_meta = soup.find("meta", {"name": "citation_title"})
        if citation_meta:
            page_title = citation_meta.get("content")
        else:
            title_elem = soup.find("h1") or soup.find("h2")
            if title_elem:
                page_title = title_elem.get_text(strip=True)
        
        # author extraction
        authors = []
        author_meta_tags = soup.find_all("meta", {"name": "citation_author"})
        authors = [tag.get("content") for tag in author_meta_tags if tag.get("content")]
        
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
        
        if author_count > 0:
            print(f"  Authors found: {authors_string} (count: {author_count})")
        
        # PDF link extraction
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
            return pdf_response.content, page_title, authors_string, is_multi_author
        else:
            print(f" error: pdf not returned ({pdf_response.status_code})")
            return None, None, None, None
        
    except Exception as e:
        print(f" Error: {e}")
        return None, None, None, None


# part 2) text extraction & footnote/article classification funcs


def find_footnote_separator(pdf_content):
    # detects the horizontal line discussed that acts as a separator in law reviews
    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            main_text = ""
            footnotes_text = ""
            separator_found = False
            separator_y = None
            
            for page_num, page in enumerate(pdf.pages):
                page_height = page.height
                page_width = page.width
                
                # extracts all horizontal lines on the page
                horizontal_lines = []
                
                # gets lines (edges from line objects)
                if hasattr(page, 'lines') and page.lines:
                    for line in page.lines:
                        try:
                            x0, top, x1, bottom = line
                            # checks if line is horizontal (y-coordinates are same)
                            if abs(top - bottom) < 2:  # allows small tolerance for different law review variations?
                                # checks if line spans most of page width (typical for separators)
                                line_width = abs(x1 - x0)
                                if line_width > page_width * 0.5:  # line covers >50% of page
                                    horizontal_lines.append({
                                        'y': top,
                                        'x0': min(x0, x1),
                                        'x1': max(x0, x1),
                                        'width': line_width
                                    })
                        except (ValueError, TypeError):
                            pass
                
                # gets rects (can also be separator lines)
                if hasattr(page, 'rects') and page.rects:
                    for rect in page.rects:
                        try:
                            x0, top, x1, bottom = rect
                            height = abs(bottom - top)
                            width = abs(x1 - x0)
                            # very thin horizontal rectangle = separator line
                            if height < 2 and width > page_width * 0.5:
                                horizontal_lines.append({
                                    'y': top,
                                    'x0': min(x0, x1),
                                    'x1': max(x0, x1),
                                    'width': width
                                })
                        except (ValueError, TypeError):
                            pass
                
                # finds the separator line (typically in middle of page)
                # filters lines that are likely separators
                # -> not at very top (title area)
                # -> not at very bottom (page break area)
                # -> reasonably long
                separator_candidates = [
                    line for line in horizontal_lines
                    if page_height * 0.2 < line['y'] < page_height * 0.85
                ]
                
                # chooses the line closest to middle of page (most likely separator)
                if separator_candidates:
                    separator_line = min(
                        separator_candidates,
                        key=lambda x: abs(x['y'] - (page_height * 0.6))
                    )
                    separator_y = separator_line['y']
                    separator_found = True
                
                # extracts text and split by separator
                page_text = page.extract_text() or ""
                
                if separator_found and separator_y is not None:
                    # uses pdfplumber's cropping for text separation above/below line
                    # text above separator = main text
                    main_crop = page.crop((0, 0, page_width, separator_y))
                    main_text += (main_crop.extract_text() or "") + " "
                    
                    # text below separator = footnotes
                    footnote_crop = page.crop((0, separator_y, page_width, page_height))
                    footnotes_text += (footnote_crop.extract_text() or "") + " "
                else:
                    # no separator found, treat entire page as main text
                    main_text += page_text + " "
            
            return (
                main_text.strip(),
                footnotes_text.strip(),
                separator_y,
                separator_found
            )
    except Exception as e:
        print(f" Error finding separator: {e}")
        return "", "", None, False


def classify_article_type(df_row):
    # classifies article as 'Note', 'Comment', 'Article', or 'Essay'
    word_count = df_row['words']
    is_multi_author = df_row['multi_author']
    
    # general trends:
    # notes/comments: 8k-15k words, 1 author
    # articles/essays: 15k+ words, can be multi-author
    # multi-author papers cannot be notes (those are student work)
    
    if is_multi_author:
        return 'Article' if word_count > 12000 else 'Essay'
    if word_count < 10000:
        return 'Note'
    elif word_count < 15000:
        return 'Comment'
    else:
        return 'Article'


def extract_pdf_text_and_metadata(pdf_content):
    # extracts text word char counts etc and flagging mechanism

    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            num_pages = len(pdf.pages)
        
        # detects footnote separator
        main_text, footnotes_text, separator_y, has_separator = find_footnote_separator(pdf_content)
        
        # full text is main + footnotes
        full_text = main_text + " " + footnotes_text
        
        # finds metrics
        main_word_count = len(main_text.split())
        footnote_word_count = len(footnotes_text.split())
        total_word_count = len(full_text.split())
        
        total_char_count = len(full_text)
        char_count_no_space = len(''.join(c for c in full_text if not c.isspace()))
        
        # flags suspicious cases for manual review
        flags = []
        
        # detects scanned PDFs (many pages but almost no text extracted)
        if num_pages > 3 and total_word_count < 100:
            flags.append("scanned_pdf_detected")
        
        # detects table of contents (very high line count relative to words, lots of page numbers)
        if "contents" in main_text.lower() or "table of" in main_text.lower():
            if main_text.count('\n') > total_word_count * 0.3:  # many line breaks = TOC format
                flags.append("likely_table_of_contents")
        
        # detects front matter / introductory pages (very short and intro-like)
        intro_markers = ["editor", "foreword", "preface", "introduction to", "note from"]
        if total_word_count < 800 and any(marker in main_text.lower() for marker in intro_markers):
            flags.append("likely_front_matter")
        
        return {
            'words': total_word_count,
            'main_text_words': main_word_count,
            'footnote_words': footnote_word_count,
            'char_count_total': total_char_count,
            'char_count_no_space': char_count_no_space,
            'pages': num_pages,
            'main_text': main_text,
            'footnotes_text': footnotes_text,
            'has_footnote_separator': has_separator,
            'separator_position': separator_y,
            'ocr_used': False,
            'flags': flags,
            'text_extracted': full_text.strip(),
            'title': 'unknown title'
        }
    except Exception as e:
        print(f" PDF parsing error: {e}")
        return None


def extract_title_from_text(text):
    # extracts title from first page review
    if not text:
        return "unknown title"
    
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    for line in lines[:5]:
        if len(line) > 20 and len(line) < 200:
            return line
    
    return lines[0] if lines else "unknown title"


# part 3) scraping and metadata collection

def scrape_law_journal(journal_name, base_url, journal_start_year, start_year, end_year):

    print(f" scraping {journal_name} ({start_year}---{end_year})") 
    print(f" saving downloads to: {OUTPUT_FOLDER}/\n")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    results = []
    flagged_articles = []

    for year in range(start_year, end_year + 1):
        volume = year - journal_start_year + 1
        print(f"\n Year {year} (Volume {volume})")

        for issue in range(1, 7):
            print(f" Issue {issue}:")

            articles_found = 0
            consecutive_failures = 0

            for article_num in range(1, 21):
                if consecutive_failures >= 3:
                    break

                url = base_url.format(volume=volume, issue=issue, article=article_num)
                pdf_content, page_title, authors_string, is_multi_author = download_pdf(url)

                if pdf_content:
                    metadata = extract_pdf_text_and_metadata(pdf_content)

                    if metadata:
                        if metadata['title'] == "unknown title" and page_title:
                            metadata['title'] = page_title
                        
                        # handles flagged articles
                        if metadata['flags']:
                            flagged_articles.append({
                                'journal': journal_name,
                                'year': year,
                                'volume': volume,
                                'issue': issue,
                                'article': article_num,
                                'url': url,
                                'flags': metadata['flags'],
                                'word_count': metadata['words'],
                                'pages': metadata['pages']
                            })
                            print(f"  ⚠️  Article {article_num} flagged: {metadata['flags']}")
                            consecutive_failures += 1
                            continue
                        
                        filename = f"{journal_name.replace(' ', '_').lower()}_{year}_vol{volume}_iss{issue}_art{article_num}.pdf"
                        filepath = os.path.join(OUTPUT_FOLDER, filename)
                        with open(filepath, "wb") as f:
                            f.write(pdf_content)

                        # creates article record
                        article_record = {
                            'journal': journal_name,
                            'year': year,
                            'volume': volume,
                            'issue': issue,
                            'article': article_num,
                            'title': metadata['title'],
                            'authors': authors_string,
                            'multi_author': is_multi_author,
                            'words': metadata['words'],
                            'main_text_words': metadata['main_text_words'],
                            'footnote_words': metadata['footnote_words'],
                            'char_count_total': metadata['char_count_total'],
                            'char_count_no_space': metadata['char_count_no_space'],
                            'pages': metadata['pages'],
                            'has_footnote_separator': metadata['has_footnote_separator'],
                            'ocr_used': metadata['ocr_used'],
                            'url': url,
                            'filename': filename
                        }
                        
                        results.append(article_record)

                        author_marker = " [Multi-Author]" if is_multi_author else ""
                        print(f" Article {article_num}: {metadata['words']} words / {metadata['pages']} pages{author_marker}")
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

    # saves flagged issues
    if flagged_articles:
        with open(FLAGGED_ISSUES_JSON, 'w') as f:
            json.dump(flagged_articles, f, indent=2)
        print(f"\n⚠️  Flagged {len(flagged_articles)} articles - see {FLAGGED_ISSUES_JSON}")

    return results


# part 4) analysis

def generate_enhanced_metadata_json(df, output_filename='author_metadata.json'):
    # makes JSON with all info we need for classification
    # adds article type classification
    df['article_type'] = df.apply(classify_article_type, axis=1)
    
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
        'article_type_summary': {},
        'footnote_analysis': {
            'articles_with_separators': int(df['has_footnote_separator'].sum()),
            'articles_without_separators': int((~df['has_footnote_separator']).sum()),
            'avg_main_text_words': float(df['main_text_words'].mean()),
            'avg_footnote_words': float(df['footnote_words'].mean()),
            'main_to_footnote_ratio': float(df['main_text_words'].sum() / df['footnote_words'].sum()) if df['footnote_words'].sum() > 0 else None,
        },
        'authorship_by_year': {},
        'authorship_by_journal': {},
        'articles': []
    }
    
    # article type breakdown
    for atype in df['article_type'].unique():
        type_data = df[df['article_type'] == atype]
        author_metadata['article_type_summary'][atype] = {
            'count': len(type_data),
            'avg_words': float(type_data['words'].mean()),
            'avg_pages': float(type_data['pages'].mean()),
            'multi_author_pct': float((type_data['multi_author'].sum() / len(type_data)) * 100)
        }
    
    # breakdown by year
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
    
    # breakdown by journal
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
    
    # individual articles
    for _, row in df.iterrows():
        article = {
            'title': row['title'],
            'authors': row['authors'],
            'author_count': 2 if row['multi_author'] else 1,
            'is_multi_author': bool(row['multi_author']),
            'article_type': row['article_type'],
            'journal': row['journal'],
            'year': int(row['year']),
            'volume': int(row['volume']),
            'issue': int(row['issue']),
            'article_number': int(row['article']),
            'words': int(row['words']),
            'main_text_words': int(row['main_text_words']),
            'footnote_words': int(row['footnote_words']),
            'footnote_ratio': float(row['footnote_words'] / row['words']) if row['words'] > 0 else 0,
            'has_footnote_separator': bool(row['has_footnote_separator']),
            'pages': int(row['pages']),
            'char_count_no_space': int(row['char_count_no_space']),
            'ocr_used': bool(row['ocr_used']),
            'url': row['url']
        }
        author_metadata['articles'].append(article)
    
    with open(output_filename, 'w') as f:
        json.dump(author_metadata, f, indent=2)
    
    print(f"\n✓ Enhanced metadata saved to {output_filename}")
    return author_metadata


def analyze_authorship_and_types(df):
    # analyze authorship and article type trends
    print("\n" + "="*60)
    print("AUTHORSHIP & ARTICLE TYPE ANALYSIS")
    print("="*60)
    
    df['article_type'] = df.apply(classify_article_type, axis=1)
    
    # overall stats
    multi_author_df = df[df['multi_author']]
    single_author_df = df[~df['multi_author']]
    
    print(f"\nOVERALL STATISTICS:")
    print(f"  Total articles: {len(df)}")
    print(f"  Multi-author: {len(multi_author_df)} ({len(multi_author_df)/len(df)*100:.1f}%)")
    print(f"  Single-author: {len(single_author_df)} ({len(single_author_df)/len(df)*100:.1f}%)")
    
    print(f"\nARTICLE TYPE BREAKDOWN:")
    for atype in sorted(df['article_type'].unique()):
        type_data = df[df['article_type'] == atype]
        pct = len(type_data) / len(df) * 100
        avg_words = type_data['words'].mean()
        print(f"  {atype}: {len(type_data)} ({pct:.1f}%) - avg {avg_words:.0f} words")
    
    print(f"\nWORD COUNT COMPARISON:")
    print(f"  Multi-author avg: {multi_author_df['words'].mean():.0f} words")
    print(f"  Single-author avg: {single_author_df['words'].mean():.0f} words")


def save_results(results):
    # save results and generate info for us
    df = pd.DataFrame(results)
    df.to_csv(RESULTS_CSV, index=False)
    df.to_json(RESULTS_CSV.replace('.csv', '.json'), orient='records', indent=2)
    
    print(f"\n✓ Saved {len(results)} articles to {RESULTS_CSV}")
    print(f"\nSUMMARY STATISTICS:")
    print(f"  Total articles: {len(results)}")
    print(f"  Year range: {int(df['year'].min())} - {int(df['year'].max())}")
    print(f"  Average words: {df['words'].mean():.0f}")
    print(f"  Average pages: {df['pages'].mean():.1f}")
    print(f"  Multi-author articles: {int(df['multi_author'].sum())}")
    
    # enhanced metadata
    print("\n" + "="*60)
    generate_enhanced_metadata_json(df)
    analyze_authorship_and_types(df)
    print("="*60)
    
    return df


# main func

def main():
    print("=" * 60)
    print("law review analysis (v2 - enhanced)")
    print("=" * 60)
    
    duke_results = scrape_law_journal(
        journal_name='Duke Law Journal',
        base_url=BASE_URL,
        journal_start_year=DUKE_JOURNAL_START_YEAR,
        start_year=START_YEAR,
        end_year=END_YEAR
    )

    pepperdine_results = scrape_law_journal(
        journal_name='Pepperdine Law Review',
        base_url=PEPPERDINE_BASE_URL,
        journal_start_year=PEPPERDINE_JOURNAL_START_YEAR,
        start_year=START_YEAR,
        end_year=END_YEAR
    )
    
    results = duke_results + pepperdine_results

    if not results:
        print("no articles found. check url patterns and start years.")
        return
    
    df = save_results(results)

    print(f"\n📋 NEXT STEPS:")
    print(f"1. Review {FLAGGED_ISSUES_JSON} for flagged articles")
    print(f"2. Review {RESULTS_CSV} for data quality")
    print(f"3. TBD")


if __name__ == "__main__":
    main()
