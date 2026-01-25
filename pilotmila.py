import requests
import pdfplumber
import pandas as pd
import os
import json
import re
from time import sleep
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from io import BytesIO
import warnings
from typing import Dict, Any, Tuple
from datetime import datetime

warnings.filterwarnings("ignore", message="Could not get FontBBox")
warnings.filterwarnings("ignore", message="Could not get FontBBox from font descriptor")
warnings.filterwarnings("ignore", message="Not adding object")
warnings.filterwarnings("ignore", category=UserWarning)

# constants
START_YEAR = 2004 # 1995 original
END_YEAR = 2004 # 2015 original
OUTPUT_FOLDER = "downloads"
RESULTS_CSV = "law_review_prelim_results.csv"
FLAGGED_ISSUES_JSON = "flagged_issues.json"
CLASSIFICATION_LOG = "classification_log.json"

# URLs
BASE_URL = "https://scholarship.law.duke.edu/dlj/vol{volume}/iss{issue}/{article}/"
DUKE_JOURNAL_START_YEAR = 1951

PEPPERDINE_BASE_URL = "https://digitalcommons.pepperdine.edu/plr/vol{volume}/iss{issue}/{article}/"
PEPPERDINE_JOURNAL_START_YEAR = 1974

# classification pipeline updated with LLM fallback

class LawReviewClassifier:
    # LLM fallback with new methodology - Dr. Or CS
    
    # labels defs
    LABELS = {
        'Unlabeled', 'Article', 'Essay', 'Article_OR_Essay',
        'Note', 'Comment', 'Note_OR_Comment', 'Miscellaneous', 'ERROR'
    }
    
    # case sensitive KWs
    SECTION_KEYWORDS = {
        'ARTICLE': ['ARTICLE', 'ARTICLES'],
        'ESSAY': ['ESSAY', 'ESSAYS'],
        'NOTE': ['NOTE', 'NOTES'],
        'COMMENT': ['COMMENT', 'COMMENTS'],
        'STUDENT_CONTRIBUTION': ['STUDENT CONTRIBUTION', 'STUDENT CONTRIBUTIONS']
    }
    
    def __init__(self):
        self.classification_log = []
    
    def classify(self, paper_data: Dict[str, Any], publication_year: int) -> Dict[str, Any]:
        # main pipeline with final label return

        result = {
            'label': 'Unlabeled',
            'steps': [],
            'confidence': 0.0,
            'errors': []
        }
        
        # directs to re-processing filter for shorter pieces <= 3 pages
        result = self._step_a_preprocessing(paper_data, result)
        if result['label'] != 'Unlabeled':
            return result
        
        # section header keyword lookup within first three pages
        result = self._step_b_keyword_lookup(paper_data, result)
        
        # checks if student author or law professor
        result = self._step_c_student_check(paper_data, result, publication_year)
        
        # length-based validation
        result = self._step_d_validation(result)
        
        # LLM fallback for ERROR
        if result['label'] == 'ERROR':
            result = self._step_e_llm_fallback(paper_data, result, publication_year)
        
        self.classification_log.append({
            'title': paper_data.get('title', 'Unknown'),
            'year': publication_year,
            'result': result
        })
        
        return result
    
    def _step_a_preprocessing(self, paper_data: Dict, result: Dict) -> Dict:
        # filters short papers (read above)
        pages = paper_data.get('pages', 0)
        
        if pages <= 3:
            result['label'] = 'Misc'
            result['steps'].append({
                'step': 'A',
                'name': 'Pre-Processing Filter',
                'rule': f'Pages ({pages}) ≤ 3',
                'action': 'Label Misc'
            })
        else:
            result['steps'].append({
                'step': 'A',
                'name': 'Pre-Processing Filter',
                'rule': f'Pages ({pages}) > 3',
                'action': 'Continue to next step'
            })
        
        return result
    
    def _step_b_keyword_lookup(self, paper_data: Dict, result: Dict) -> Dict:
        # check KWs from from section headers first 3 pgs
        text = paper_data.get('main_text', '')
        
        # estimate: ~250-300 words per page for extraction first three pgs
        first_3_pages = ' '.join(text.split()[:1000])
        
        # KW search
        found_keywords = {category: [] for category in self.SECTION_KEYWORDS}
        
        for category, keywords in self.SECTION_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text[:3000]:  # actual search
                    found_keywords[category].append(keyword)
        
        # count KW types found
        categories_found = [cat for cat, kws in found_keywords.items() if kws]
        
        decision_logic_result = self._apply_decision_logic(categories_found)
        
        if decision_logic_result != 'Continue':
            result['label'] = decision_logic_result
            result['steps'].append({
                'step': 'B',
                'name': 'Section Header Keyword Lookup',
                'keywords_found': found_keywords,
                'categories': categories_found,
                'action': f'Label {decision_logic_result}'
            })
        else:
            result['steps'].append({
                'step': 'B',
                'name': 'Section Header Keyword Lookup',
                'keywords_found': found_keywords,
                'categories': categories_found,
                'action': 'No definitive keywords. Continue to Step C'
            })
        
        return result
    
    def _apply_decision_logic(self, categories: list) -> str:
        if not categories:
            return 'Continue'
        
        # identifies the diff categories for the metadata we want to analyze
        if len(categories) == 1:
            cat = categories[0]
            if cat == 'ARTICLE':
                return 'Article'
            elif cat == 'ESSAY':
                return 'Essay'
            elif cat == 'NOTE':
                return 'Note'
            elif cat == 'COMMENT':
                return 'Comment'
            elif cat == 'STUDENT_CONTRIBUTION':
                return 'Note_OR_Comment'
        
        # two categories
        if len(categories) == 2:
            cats_set = set(categories)
            if cats_set == {'ARTICLE', 'ESSAY'}:
                return 'Article_OR_Essay'
            elif cats_set == {'NOTE', 'COMMENT'}:
                return 'Note_OR_Comment'
            else:
                return 'ERROR'  # other combos
        
        # if > two categories
        return 'ERROR'
    
    def _step_c_student_check(self, paper_data: Dict, result: Dict, 
                              publication_year: int) -> Dict:
        # checks student authorship, wc <= 20,000 AND if contains KWs c/o xxxx OR J.D Candidate

        if result['label'] != 'Unlabeled':
            return result
        
        authors = paper_data.get('authors', '')
        word_count = paper_data.get('words', 0)
        
        jd_candidate_match = 'J.D. Candidate' in authors
        class_year_match = False
        
        if not jd_candidate_match:
            class_pattern = r'Class of (\d{4})'
            class_matches = re.findall(class_pattern, authors)
            for class_year_str in class_matches:
                try:
                    class_year = int(class_year_str)
                    if abs(class_year - publication_year) <= 3:
                        class_year_match = True
                        break
                except ValueError:
                    pass
        
        criterion_1_met = jd_candidate_match or class_year_match
        criterion_2_met = word_count <= 20000
        both_criteria_met = criterion_1_met and criterion_2_met
        
        step_info = {
            'step': 'C',
            'name': 'Student Authorship Check',
            'criterion_1_jd_candidate': jd_candidate_match,
            'criterion_1_class_year': class_year_match,
            'criterion_1_met': criterion_1_met,
            'criterion_2_word_count': word_count,
            'criterion_2_met': criterion_2_met,
            'both_met': both_criteria_met
        }
        
        if both_criteria_met:
            result['label'] = 'Note_OR_Comment'
            step_info['action'] = 'both criteria met. label Note_OR_Comment'
        else:
            step_info['action'] = 'criteria not fully met. continue to final step'
        
        result['steps'].append(step_info)
        return result
    
    def _step_d_validation(self, result: Dict) -> Dict:
        # final step: validate and refine labels based on WC thresholds

        word_count = result.get('word_count', 0)  # will be added by classify()
        current_label = result['label']
        
        # length-based assignments if still unlabeled
        validation_table = {
            'Article': (lambda wc: wc > 15000, 'Article (confirmed)', 'ERROR'),
            'Essay': (lambda wc: wc < 20000, 'Essay (confirmed)', 'ERROR'),
            'Article_OR_Essay': (lambda wc: wc > 15000, 'Article', 'Essay'),
            'Note': (lambda wc: wc < 18000, 'Note (confirmed)', 'ERROR'),
            'Comment': (lambda wc: wc < 10000, 'Comment (confirmed)', 'ERROR'),
            'Note_OR_Comment': (lambda wc: ('Comment' if wc < 10000 else ('Note' if 10000 <= wc <= 20000 else 'ERROR')), None, None),
            'Unlabeled': (lambda wc: wc > 18000, 'Article', 'ERROR'),
            'Miscellaneous': (lambda wc: True, 'Miscellaneous', 'Miscellaneous'),
        }
        
        if current_label in validation_table:
            if current_label == 'Note_OR_Comment':
                condition_fn, _, _ = validation_table[current_label]
                final_label = condition_fn(word_count)
            else:
                condition_fn, label_if_true, label_if_false = validation_table[current_label]
                final_label = label_if_true if condition_fn(word_count) else label_if_false
            
            result['steps'].append({
                'step': 'D',
                'name': 'Validation (Length-Based Refinement)',
                'current_label': current_label,
                'word_count': word_count,
                'condition_met': condition_fn(word_count) if current_label != 'Note_OR_Comment' else None,
                'final_label': final_label
            })
            
            result['label'] = final_label
        else:
            result['steps'].append({
                'step': 'D',
                'name': 'Validation (Length-Based Refinement)',
                'note': f'Unknown label: {current_label}'
            })
        
        return result
    
    def _step_e_llm_fallback(self, paper_data: Dict, result: Dict, 
                            publication_year: int) -> Dict:
        # still working on perfecting this - need Claude Sonnet 4.5 API key
        
        result['steps'].append({
            'step': 'E',
            'name': 'LLM Fallback (TODO)',
            'note': 'needs Claude Sonnet 4.5 API key. placeholder for now.',
            'status': 'PENDING_API_KEY'
        })
        
        result['label'] = 'ERROR'  # Still ERROR pending LLM implementation
        result['requires_manual_review'] = True
        
        return result

# metadata extraction section

def download_pdf(url):
    # gets metadata on authors, etc.
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

# text extraction and footnote detection

def find_footnote_separator(pdf_content):
    # detects horizontal line separator
    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            main_text = ""
            footnotes_text = ""
            separator_found = False
            separator_y = None
            
            for page_num, page in enumerate(pdf.pages):
                page_height = page.height
                page_width = page.width
                
                horizontal_lines = []
                
                # extracts lines
                if hasattr(page, 'lines') and page.lines:
                    for line in page.lines:
                        try:
                            x0, top, x1, bottom = line
                            if abs(top - bottom) < 2:
                                line_width = abs(x1 - x0)
                                if line_width > page_width * 0.5:
                                    horizontal_lines.append({
                                        'y': top,
                                        'x0': min(x0, x1),
                                        'x1': max(x0, x1),
                                        'width': line_width
                                    })
                        except (ValueError, TypeError):
                            pass
                
                # extracts rects
                if hasattr(page, 'rects') and page.rects:
                    for rect in page.rects:
                        try:
                            x0, top, x1, bottom = rect
                            height = abs(bottom - top)
                            width = abs(x1 - x0)
                            if height < 2 and width > page_width * 0.5:
                                horizontal_lines.append({
                                    'y': top,
                                    'x0': min(x0, x1),
                                    'x1': max(x0, x1),
                                    'width': width
                                })
                        except (ValueError, TypeError):
                            pass
                
                # finds the separators
                separator_candidates = [
                    line for line in horizontal_lines
                    if page_height * 0.2 < line['y'] < page_height * 0.85
                ]
                
                if separator_candidates:
                    separator_line = min(
                        separator_candidates,
                        key=lambda x: abs(x['y'] - (page_height * 0.6))
                    )
                    separator_y = separator_line['y']
                    separator_found = True
                
                page_text = page.extract_text() or ""
                
                if separator_found and separator_y is not None:
                    main_crop = page.crop((0, 0, page_width, separator_y))
                    main_text += (main_crop.extract_text() or "") + " "
                    
                    footnote_crop = page.crop((0, separator_y, page_width, page_height))
                    footnotes_text += (footnote_crop.extract_text() or "") + " "
                else:
                    main_text += page_text + " "
            
            return (
                main_text.strip(),
                footnotes_text.strip(),
                separator_y,
                separator_found
            )
    except Exception as e:
        print(f" error finding separator: {e}")
        return "", "", None, False


def extract_pdf_text_and_metadata(pdf_content):
    # gives metadata based on extraction from pdfplumber
    try:
        with pdfplumber.open(BytesIO(pdf_content)) as pdf:
            num_pages = len(pdf.pages)
        
        main_text, footnotes_text, separator_y, has_separator = find_footnote_separator(pdf_content)
        full_text = main_text + " " + footnotes_text
        
        main_word_count = len(main_text.split())
        footnote_word_count = len(footnotes_text.split())
        total_word_count = len(full_text.split())
        
        total_char_count = len(full_text)
        char_count_no_space = len(''.join(c for c in full_text if not c.isspace()))
        
        # to flag manual cases - for review
        flags = []
        
        if num_pages > 3 and total_word_count < 100:
            flags.append("scanned_pdf_detected")
        
        if "contents" in main_text.lower() or "table of" in main_text.lower():
            if main_text.count('\n') > total_word_count * 0.3:
                flags.append("likely_table_of_contents")
        
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

# scraping and metadata collection

def scrape_law_journal(journal_name, base_url, journal_start_year, start_year, end_year):
    # scrapes with new classification pipeline
    
    print(f" scraping {journal_name} ({start_year}---{end_year})") 
    print(f" saving downloads to: {OUTPUT_FOLDER}/\n")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    results = []
    flagged_articles = []
    classifier = LawReviewClassifier()

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
                        
                        # classification pipeline
                        paper_data = {
                            'title': metadata['title'],
                            'authors': authors_string,
                            'words': metadata['words'],
                            'pages': metadata['pages'],
                            'main_text': metadata['main_text']
                        }
                        
                        classification_result = classifier.classify(paper_data, year)
                        
                        filename = f"{journal_name.replace(' ', '_').lower()}_{year}_vol{volume}_iss{issue}_art{article_num}.pdf"
                        filepath = os.path.join(OUTPUT_FOLDER, filename)
                        with open(filepath, "wb") as f:
                            f.write(pdf_content)

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
                            'classification_label': classification_result['label'],
                            'classification_steps': classification_result['steps'],
                            'requires_manual_review': classification_result.get('requires_manual_review', False),
                            'url': url,
                            'filename': filename
                        }
                        
                        results.append(article_record)

                        author_marker = " [Multi-Author]" if is_multi_author else ""
                        label_marker = f" → {classification_result['label']}"
                        print(f" Article {article_num}: {metadata['words']} words / {metadata['pages']} pages{author_marker}{label_marker}")
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

    if flagged_articles:
        with open(FLAGGED_ISSUES_JSON, 'w') as f:
            json.dump(flagged_articles, f, indent=2)
        print(f"\n⚠️  Flagged {len(flagged_articles)} articles - see {FLAGGED_ISSUES_JSON}")

    # Save classification log
    with open(CLASSIFICATION_LOG, 'w') as f:
        json.dump(classifier.classification_log, f, indent=2)
    print(f"✓ Classification log saved to {CLASSIFICATION_LOG}")

    return results

# analysis and results BELOW

def save_results(results):
    # gives us summaries of the metadata found
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
    
    # classification results
    print(f"\nCLASSIFICATION RESULTS:")
    label_counts = df['classification_label'].value_counts()
    for label, count in label_counts.items():
        pct = (count / len(df)) * 100
        print(f"  {label}: {count} ({pct:.1f}%)")
    
    # keeps track of ERROR labels
    error_count = (df['classification_label'] == 'ERROR').sum()
    manual_review_count = df['requires_manual_review'].sum()
    print(f"\nQUALITY METRICS:")
    print(f"  ERROR labels: {error_count}")
    print(f"  Requiring manual review: {manual_review_count}")
    
    return df

def main():
    print("=" * 60)
    print("law review analysis (v3 - enhanced classification pipeline)")
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
    print(f"1. review {CLASSIFICATION_LOG} for detailed classification trace")
    print(f"2. review {RESULTS_CSV} for articles requiring manual review")
    print(f"3. implement LLM fallback (step E/final step) with Claude Sonnet 4.5 API key")
    print(f"4. discuss with Dr. Or CS about next steps, if implementation approach is accurate")


if __name__ == "__main__":
    main()
