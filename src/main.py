"""Apify Actor entry point: Jooble Job Scraper (Python, requests_html + BeautifulSoup).

This Actor fetches job listings from Jooble using query parameters for `ukw` (keyword)
and `rgns` (region), paginates via `p` parameter, and outputs structured dataset items.
"""

from __future__ import annotations

import asyncio
import random
import json
import re
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Set

from apify import Actor  # pyright: ignore[reportMissingImports]
from bs4 import BeautifulSoup
import httpx

# Stealth headers / random user-agents
try:
    # Minimal, defensive import in case environment lacks stealthkit
    from stealthkit import user_agent as sk_user_agent
    from stealthkit.headers import build_headers as sk_build_headers
except Exception:  # pragma: no cover - fallback if stealthkit not available
    sk_user_agent = None
    sk_build_headers = None


# Optional Chrome TLS impersonation client (stealth TLS/JA3)
try:
    from curl_cffi import requests as curl_requests  # type: ignore
except Exception:  # pragma: no cover - fallback if curl_cffi not available
    curl_requests = None

def build_stealth_headers(referer: Optional[str] = None) -> Dict[str, str]:
    """Return randomized, browser-like headers using stealthkit if available.

    Falls back to a simple rotating User-Agent list if stealthkit is unavailable.
    """
    if sk_build_headers and sk_user_agent:
        ua = sk_user_agent.random()
        headers = sk_build_headers(user_agent=ua)
    else:
        # Use latest, most common User-Agents from real browsers
        fallback_uas = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
        ]
        ua = random.choice(fallback_uas)
        
        # More realistic header order and values matching real browsers
        headers = {
            'User-Agent': ua,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Chromium";v="131", "Not_A Brand";v="24", "Google Chrome";v="131"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
        }
    
    if referer:
        headers['Referer'] = referer
        # When we have a referer, it means we're navigating within the site
        headers['Sec-Fetch-Site'] = 'same-origin'
    
    return headers


def absolute_url(base: str, href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    return urllib.parse.urljoin(base, href)


def select_first_text(soup: BeautifulSoup, selectors: Iterable[str]) -> Optional[str]:
    for selector in selectors:
        el = soup.select_one(selector)
        if el and el.get_text(strip=True):
            return el.get_text(strip=True)
    return None


def select_first_html(soup: BeautifulSoup, selectors: Iterable[str]) -> Optional[str]:
    for selector in selectors:
        el = soup.select_one(selector)
        if el:
            return str(el)
    return None


def extract_jobs_from_ld_json(page_soup: BeautifulSoup) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    
    # First, try to find JSON-LD structured data
    for script in page_soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string or '')
        except Exception:
            continue
        if not data:
            continue
        candidates = []
        if isinstance(data, dict):
            candidates = [data]
        elif isinstance(data, list):
            candidates = data
        for obj in candidates:
            try:
                typ = obj.get('@type') if isinstance(obj, dict) else None
                if isinstance(typ, list):
                    is_job = any(t.lower() == 'jobposting' for t in typ if isinstance(t, str))
                else:
                    is_job = isinstance(typ, str) and typ.lower() == 'jobposting'
                if not is_job:
                    continue
                title = obj.get('title') or obj.get('name')
                hiring_org = obj.get('hiringOrganization') or {}
                company = hiring_org.get('name') if isinstance(hiring_org, dict) else None
                job_loc = obj.get('jobLocation') or {}
                location = None
                if isinstance(job_loc, dict):
                    addr = job_loc.get('address') or {}
                    if isinstance(addr, dict):
                        location = addr.get('addressLocality') or addr.get('addressRegion') or addr.get('addressCountry')
                descr = obj.get('description')
                date_posted = obj.get('datePosted') or obj.get('datePublished')
                salary = None
                comp = obj.get('baseSalary') or {}
                if isinstance(comp, dict):
                    val = comp.get('value')
                    if isinstance(val, dict):
                        amount = val.get('value')
                        unit = val.get('unitText')
                        if amount:
                            salary = f"{amount} {unit or ''}".strip()
                url = obj.get('url')
                item: Dict[str, Any] = {
                    'job_title': title,
                    'company': company,
                    'location': location,
                    'date_posted': date_posted,
                    'job_type': None,
                    'job_url': url,
                    'description_text': BeautifulSoup(descr, 'lxml').get_text(strip=True) if isinstance(descr, str) else None,
                    'description_html': descr if isinstance(descr, str) else None,
                    'salary': salary,
                }
                if item['job_title'] or item['job_url']:
                    jobs.append(item)
            except Exception:
                continue
    
    # Second, try to extract from inline JavaScript variables (common pattern for SPAs)
    if not jobs:
        for script in page_soup.find_all('script'):
            if not script.string:
                continue
            try:
                script_text = script.string
                # Look for common JS variable patterns that contain job data
                # Pattern: var jobs = [...]; or window.__INITIAL_STATE__ = {...};
                patterns = [
                    r'var\s+jobs\s*=\s*(\[.*?\]);',
                    r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});',
                    r'window\.__DATA__\s*=\s*(\{.*?\});',
                    r'"jobs"\s*:\s*(\[.*?\])',
                    r'"vacancies"\s*:\s*(\[.*?\])',
                    r'"results"\s*:\s*(\[.*?\])',
                ]
                
                for pattern in patterns:
                    matches = re.findall(pattern, script_text, re.DOTALL)
                    for match in matches:
                        try:
                            data = json.loads(match)
                            if isinstance(data, list):
                                job_list = data
                            elif isinstance(data, dict):
                                # Try to find jobs array in the dict
                                job_list = data.get('jobs') or data.get('vacancies') or data.get('results') or []
                            else:
                                continue
                                
                            for job_data in job_list:
                                if not isinstance(job_data, dict):
                                    continue
                                item = {
                                    'job_title': job_data.get('title') or job_data.get('name') or job_data.get('position'),
                                    'company': job_data.get('company') or job_data.get('employer') or job_data.get('organization'),
                                    'location': job_data.get('location') or job_data.get('city') or job_data.get('address'),
                                    'date_posted': job_data.get('datePosted') or job_data.get('created_at') or job_data.get('published'),
                                    'job_type': job_data.get('employmentType') or job_data.get('type') or job_data.get('schedule'),
                                    'job_url': job_data.get('url') or job_data.get('link') or job_data.get('href'),
                                    'description_text': job_data.get('description') or job_data.get('summary'),
                                    'description_html': None,
                                    'salary': job_data.get('salary') or job_data.get('wage') or job_data.get('compensation'),
                                }
                                if item['job_title'] or item['job_url']:
                                    jobs.append(item)
                        except (ValueError, json.JSONDecodeError):
                            continue
                            
            except Exception as e:
                Actor.log.debug(f'Error extracting from inline JS: {e}')
                continue
                
    return jobs


def extract_jobs_from_links(page_soup: BeautifulSoup, page_url: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    # Comprehensive link selectors for Jooble
    link_selectors = [
        'a.job_card_link',
        'a[class*="job_card_link" i]',
        'a._8w9Ce2.tUC4Fj._6i4Nb0.wtCvxI.job_card_link',
        'a[href*="/job/"]',
        'a[href*="/jdp/"]',
        'a[href*="/jd/"]',
        'a[href*="/j/"]',
        'a[href*="redirect?"]',
        # Additional Jooble patterns
        'a[href*="/redirect?"]',
        'a[href*="/search/"]',
        'a[href*="/vacancy/"]',
        'a[href*="/position/"]',
        # Generic job-related links
        'a[href*="job"]',
        'a[href*="vacancy"]',
        'a[href*="position"]',
    ]
    seen_hrefs: Set[str] = set()
    
    # First pass: try specific selectors
    for sel in link_selectors:
        for a in page_soup.select(sel):
            href = a.get('href')
            if not href:
                continue
            abs_url = absolute_url(page_url, href)
            if not abs_url or abs_url in seen_hrefs:
                continue
            title = a.get_text(strip=True) or None
            if not title or len(title) < 3:  # Skip very short titles
                continue
                
            # Try to find nearby company/location
            parent = a.parent
            company = None
            location = None
            salary = None
            description_text = None
            try:
                if parent:
                    # Look up the tree a few levels for common fields
                    container = parent
                    for _ in range(4):  # Increased depth
                        if not container:
                            break
                        if not company:
                            company = select_first_text(container, [
                                'span[class*="company" i]',
                                'div[class*="company" i]',
                                'a[data-qa*="company" i]',
                                'span[class*="employer" i]',
                                'div[class*="employer" i]',
                            ])
                        if not location:
                            location = select_first_text(container, [
                                'span[class*="location" i]',
                                'div[class*="location" i]',
                                'span[data-qa*="location" i]',
                                'span[class*="city" i]',
                                'div[class*="city" i]',
                            ])
                        if not salary:
                            salary = select_first_text(container, [
                                'span[class*="salary" i]',
                                'div[class*="salary" i]',
                                'span[data-qa*="salary" i]',
                                'span[class*="wage" i]',
                                'div[class*="wage" i]',
                            ])
                        if not description_text:
                            desc_html = select_first_html(container, [
                                'div[class*="description" i]',
                                'div[class*="desc" i]',
                                'div[data-qa*="vacancy-snippet" i]',
                                'div[class*="snippet" i]',
                            ])
                            if desc_html:
                                description_text = BeautifulSoup(desc_html, 'lxml').get_text(strip=True)
                        container = container.parent
            except Exception:
                pass

            items.append({
                'job_title': title,
                'company': company,
                'location': location,
                'date_posted': None,
                'job_type': None,
                'job_url': abs_url,
                'description_text': description_text,
                'description_html': None,
                'salary': salary,
            })
            seen_hrefs.add(abs_url)
    
    # Second pass: scan all links for job-related patterns if we found nothing
    if not items:
        Actor.log.info('No specific job links found, scanning all links for job patterns...')
        for a in page_soup.find_all('a', href=True):
            href = a.get('href')
            if not href:
                continue
            # Look for job-related patterns in URLs
            if any(pattern in href.lower() for pattern in ['job', 'vacancy', 'position', 'career', 'employment']):
                abs_url = absolute_url(page_url, href)
                if not abs_url or abs_url in seen_hrefs:
                    continue
                title = a.get_text(strip=True) or None
                if not title or len(title) < 3:
                    continue
                    
                items.append({
                    'job_title': title,
                    'company': None,
                    'location': None,
                    'date_posted': None,
                    'job_type': None,
                    'job_url': abs_url,
                    'description_text': None,
                    'description_html': None,
                    'salary': None,
                })
                seen_hrefs.add(abs_url)
                # Log first few found links for debugging
                if len(items) <= 5:
                    Actor.log.info(f'Found link via pattern scan: {title[:50]} -> {abs_url[:100]}')
    
    Actor.log.info(f'extract_jobs_from_links found {len(items)} items total')
    return items


async def try_ajax_endpoints(session: httpx.AsyncClient, base_url: str, keyword: str, region: str) -> List[Dict[str, Any]]:
    """Try to find AJAX endpoints that return job data in JSON format."""
    jobs = []
    
    # Common AJAX patterns for job sites
    ajax_patterns = [
        f"/api/jobs?keyword={urllib.parse.quote(keyword)}&region={urllib.parse.quote(region)}",
        f"/api/search?q={urllib.parse.quote(keyword)}&location={urllib.parse.quote(region)}",
        f"/api/vacancies?search={urllib.parse.quote(keyword)}&location={urllib.parse.quote(region)}",
        f"/search/jobs?keyword={urllib.parse.quote(keyword)}&region={urllib.parse.quote(region)}",
        f"/jobs/search?q={urllib.parse.quote(keyword)}&loc={urllib.parse.quote(region)}",
    ]
    
    headers = build_stealth_headers()
    headers.update({
        'Accept': 'application/json, text/plain, */*',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': base_url,
    })
    
    for pattern in ajax_patterns:
        try:
            ajax_url = urllib.parse.urljoin(base_url, pattern)
            Actor.log.info(f'Trying AJAX endpoint: {ajax_url}')
            
            resp = await session.get(ajax_url, headers=headers, timeout=15)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if isinstance(data, dict) and 'jobs' in data:
                        jobs_data = data['jobs']
                    elif isinstance(data, list):
                        jobs_data = data
                    else:
                        continue
                        
                    for job_data in jobs_data:
                        if isinstance(job_data, dict):
                            job_item = {
                                'job_title': job_data.get('title') or job_data.get('name') or job_data.get('position'),
                                'company': job_data.get('company') or job_data.get('employer') or job_data.get('organization'),
                                'location': job_data.get('location') or job_data.get('city') or job_data.get('address'),
                                'date_posted': job_data.get('datePosted') or job_data.get('created_at') or job_data.get('published'),
                                'job_type': job_data.get('employmentType') or job_data.get('type') or job_data.get('schedule'),
                                'job_url': job_data.get('url') or job_data.get('link') or job_data.get('href'),
                                'description_text': job_data.get('description') or job_data.get('summary'),
                                'description_html': None,
                                'salary': job_data.get('salary') or job_data.get('wage') or job_data.get('compensation'),
                            }
                            
                            if job_item['job_title'] or job_item['job_url']:
                                jobs.append(job_item)
                                
                except (ValueError, KeyError) as e:
                    Actor.log.debug(f'Failed to parse JSON from {ajax_url}: {e}')
                    continue
                    
        except Exception as e:
            Actor.log.debug(f'AJAX request failed for {pattern}: {e}')
            continue
            
    return jobs


def find_pagination_links(page_soup: BeautifulSoup, current_url: str) -> List[str]:
    """Find pagination links for next pages."""
    next_links = []
    
    # Common pagination selectors
    pagination_selectors = [
        'a[aria-label="Next"]',
        'a[aria-label="next"]',
        'a.next',
        'a[class*="next" i]',
        'a[class*="pagination" i]',
        'a[href*="p="]',
        'a[href*="page="]',
        'a[href*="pagenum="]',
    ]
    
    for selector in pagination_selectors:
        for link in page_soup.select(selector):
            href = link.get('href')
            if href:
                abs_url = absolute_url(current_url, href)
                if abs_url and abs_url not in next_links:
                    next_links.append(abs_url)
    
    return next_links


def extract_job_blocks(page_soup: BeautifulSoup) -> List[BeautifulSoup]:
    """Try multiple strategies to identify job listing blocks on Jooble search pages."""
    candidates = []
    # Common structures observed across locales; keep broad fallbacks
    strategies = [
        'article',
        'div[data-test="job"]',
        'div[class*="job" i]',
        'div[class*="result" i]',
        'li[class*="job" i]',
        'li[class*="result" i]',
        'article[class*="job" i]',
        'div[id^="job_" i]',
        'section[role="main"] article',
        'div[data-qa*="vacancy" i]',
        # broader anchors possibly wrapped in list containers
        'div[data-qa*="vacancy-item" i]',
        'div[class*="vacancy" i]',
        # Jooble-specific patterns from actual HTML structure
        'div[class*="card" i]',
        'div[class*="listing" i]',
        'div[class*="item" i]',
        # Look for divs containing job_card_link anchors
        'div:has(> a.job_card_link)',
        'div:has(> a[href*="redirect"])',
        # More generic container patterns
        'li',
        'article',
        'section > div',
    ]
    for css in strategies:
        try:
            found = page_soup.select(css)
            if found:
                candidates.extend(found)
                Actor.log.debug(f'Strategy "{css}" found {len(found)} elements')
        except Exception as e:
            # Some selectors like :has() might not work in older BeautifulSoup
            Actor.log.debug(f'Strategy "{css}" failed: {e}')
            continue
    
    # De-duplicate while preserving order
    seen: Set[int] = set()
    unique_blocks: List[BeautifulSoup] = []
    for block in candidates:
        key = id(block)
        if key not in seen:
            seen.add(key)
            unique_blocks.append(block)
    Actor.log.info(f'extract_job_blocks found {len(unique_blocks)} unique blocks')
    return unique_blocks


def parse_job_block(block: BeautifulSoup, page_url: str) -> Optional[Dict[str, Any]]:
    """Parse a single job block and return a structured dict or None if incomplete."""
    # Title and URL
    title_el = block.select_one('div[class*="position" i], a[href*="/job/"]')
    if not title_el:
        # Sometimes title anchor is nested
        title_el = block.select_one('a[href*="/job/"]')
    if not title_el:
        # Additional Jooble variants, including obfuscated multi-class link
        title_el = block.select_one(
            'a.job_card_link, '
            'a[class*="job_card_link" i], '
            'a._8w9Ce2.tUC4Fj._6i4Nb0.wtCvxI.job_card_link, '
            'a[data-qa*="vacancy-title" i], '
            'h2 a, h3 a, '
            'a[href*="/jdp/"], a[href*="/jd/"], a[href*="/j/"], a[href*="redirect?"]'
        )

    job_title = title_el.get_text(strip=True) if title_el else None
    href = title_el.get('href') if title_el else None
    job_url = absolute_url(page_url, href)

    # Company, location, date, salary
    company = select_first_text(block, [
        'span[class*="company-name" i]',
        'div[data-test="company_name"]',
        'div[class*="company" i]',
        'a[data-qa*="vacancy-company-name" i]',
        'span[data-qa*="company" i]',
    ])
    location = select_first_text(block, [
        'span[class*="location" i]',
        'div[class*="location" i]',
        'span[data-qa*="location" i]',
    ])
    date_posted = select_first_text(block, [
        'span[class*="date" i]',
        'time',
    ])
    salary = select_first_text(block, [
        'div[class*="salary" i]',
        'span[class*="salary" i]',
        'span[data-qa*="salary" i]',
    ])

    # Description text/html
    description_html = select_first_html(block, [
        'div[class*="description" i]',
        'div[class*="desc" i]',
        'div[data-qa*="vacancy-snippet" i]',
    ])
    description_text = None
    if description_html:
        # Parse again to clean text
        description_text = BeautifulSoup(description_html, 'lxml').get_text(strip=True)

    # Job type heuristic: look for tags inside description or chips
    job_type = select_first_text(block, [
        'div[class*="description" i] span[class*="type" i]',
        'span[class*="type" i]',
        'a[class*="tag" i]',
        'span[class*="tag" i]',
        'span[data-qa*="employment" i]',
        'span[data-qa*="schedule" i]',
    ])

    if not job_title and not job_url:
        return None

    return {
        'job_title': job_title,
        'company': company,
        'location': location,
        'date_posted': date_posted,
        'job_type': job_type,
        'job_url': job_url,
        'description_text': description_text,
        'description_html': description_html,
        'salary': salary,
    }


async def fetch_search_page(session: httpx.AsyncClient, url: str, referer: Optional[str], effective_proxy: Optional[str]) -> Optional[str]:
    headers = build_stealth_headers(referer=referer)
    
    # Realistic cookies that browsers typically send
    cookies = {
        'cookiesAccepted': '1',
    }

    # Retry with different strategies to avoid 403
    max_attempts = 5
    backoff_base = 2.0
    
    for attempt in range(1, max_attempts + 1):
        try:
            # Longer delay on retries to avoid rate limiting
            if attempt > 1:
                delay = backoff_base * (2 ** (attempt - 2)) + random.uniform(1.0, 3.0)
                Actor.log.info(f'Waiting {delay:.1f}s before retry {attempt}...')
                await asyncio.sleep(delay)
            else:
                # Small initial delay to simulate human behavior
                await asyncio.sleep(random.uniform(1.0, 2.5))
            
            # Rotate headers on each retry to avoid fingerprinting
            if attempt > 1:
                headers = build_stealth_headers(referer=referer)

            resp = await session.get(url, headers=headers, cookies=cookies, timeout=30.0, follow_redirects=True)
            status = resp.status_code

            html_text = resp.text
            
            # Log response for debugging
            Actor.log.info(f'Response status: {status}, HTML length: {len(html_text)} chars')
            
            # Check if we got blocked
            if status == 403:
                Actor.log.warning(f'Got 403 Forbidden on attempt {attempt}. Trying different approach...')
                
                # First, try a curl_cffi Chrome TLS impersonation fetch (sync within thread)
                if curl_requests is not None:
                    try:
                        def do_curl_fetch() -> Optional[str]:
                            proxies = None
                            if effective_proxy:
                                proxies = {"http": effective_proxy, "https": effective_proxy}
                            r = curl_requests.get(
                                url,
                                headers=headers,
                                cookies=cookies,
                                impersonate="chrome",
                                proxies=proxies,
                                timeout=30,
                                allow_redirects=True,
                            )
                            if r.status_code == 200 and len(r.text) > 1000:
                                return r.text
                            return None
                        alt_html = await asyncio.to_thread(do_curl_fetch)
                        if alt_html:
                            Actor.log.info('curl_cffi impersonation succeeded after 403.')
                            return alt_html
                    except Exception as e:
                        Actor.log.debug(f'curl_cffi impersonation failed: {e}')

                # On 403 without success, retry with rotated headers
                if attempt < max_attempts:
                    continue
                Actor.log.error(f'Still getting 403 after {max_attempts} attempts. Jooble is blocking this request.')
                Actor.log.error('SOLUTION: Use Apify Proxy with residential group (apifyProxyGroups: ["RESIDENTIAL"]) to bypass anti-bot protection.')
                return None
            
            # Check for successful response with actual content
            if status == 200 and len(html_text) > 5000:
                # Look for job-related content indicators
                html_lower = html_text.lower()
                has_content = any(keyword in html_lower for keyword in ['job', 'vacancy', 'position', 'employment', 'career'])
                
                if has_content or len(html_text) > 10000:
                    return html_text
                else:
                    Actor.log.warning(f'Got 200 but HTML seems to be a block page (no job keywords found)')
                    if attempt < max_attempts:
                        continue
                    
            # Other status codes
            if status in {200, 301, 302}:
                return html_text
                
        except Exception as e:
            Actor.log.warning(f'Fetch error on attempt {attempt} for {url}: {e}')
            if attempt >= max_attempts:
                Actor.log.error(f'Failed to fetch {url} after {max_attempts} attempts')

    return None


async def main() -> None:
    """Jooble job scraper main entry."""
    async with Actor:
        actor_input = await Actor.get_input() or {}

        keyword: str = (actor_input.get('keyword') or actor_input.get('ukw') or '').strip()
        # Region is optional for Jooble; empty means global or site default
        region: str = (actor_input.get('region') or actor_input.get('rgns') or '').strip()
        max_pages: int = int(actor_input.get('max_pages') or 1)
        start_url: Optional[str] = (actor_input.get('startUrl') or '').strip() or None
        max_jobs: int = int(actor_input.get('maxJobs') or 0)
        
        # Get proxy configuration
        proxy_config = actor_input.get('proxyConfiguration')
        proxy_url = None
        effective_proxy: Optional[str] = None
        
        if proxy_config:
            use_apify_proxy = proxy_config.get('useApifyProxy', False)
            if use_apify_proxy:
                # Get Apify proxy URL from the Actor configuration
                proxy_url = Actor.create_proxy_configuration(actor_proxy_input=proxy_config)
                if proxy_url:
                    Actor.log.info('Using Apify Proxy to avoid IP blocking')
            elif proxy_config.get('proxyUrls'):
                # Use custom proxy if provided
                proxy_urls = proxy_config.get('proxyUrls', [])
                if proxy_urls:
                    proxy_url = random.choice(proxy_urls)
                    Actor.log.info(f'Using custom proxy')

        if not start_url and not keyword:
            Actor.log.info('Provide either "startUrl" or "keyword" (ukw). Exiting...')
            await Actor.exit()

        def page_url(page_number: int) -> str:
            # If startUrl provided, preserve its query params and just update p
            if start_url:
                parsed = urllib.parse.urlparse(start_url)
                q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
                q['p'] = [str(page_number)]
                new_query = urllib.parse.urlencode({k: v[0] if isinstance(v, list) else v for k, v in q.items()})
                return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', new_query, ''))
            # Build URL with optional region. Jooble accepts empty rgns.
            params = {
                'ukw': keyword,
                'p': str(page_number),
            }
            # Do NOT send empty rgns to avoid anti-bot heuristics; omit when not provided
            if region:
                params['rgns'] = region
            return 'https://jooble.org/SearchResult?' + urllib.parse.urlencode(params)

        seen_urls: Set[str] = set()
        total_pushed = 0
        referer_url: Optional[str] = None

        # Use HTTP/2 to better mimic modern browsers, with realistic timeout settings
        # Configure proxy if provided
        client_kwargs = {
            'http2': True,
            'timeout': 30.0,
            'follow_redirects': True,
        }
        
        if proxy_url:
            # For Apify proxy, we need to get the actual proxy URL string
            if hasattr(proxy_url, 'new_url'):
                # ProxyConfiguration object - get a new URL for each session
                proxy_str = await proxy_url.new_url()
                client_kwargs['proxies'] = proxy_str
                effective_proxy = proxy_str
                Actor.log.info(f'Configured session with Apify Proxy')
            elif isinstance(proxy_url, str):
                client_kwargs['proxies'] = proxy_url
                effective_proxy = proxy_url
                Actor.log.info(f'Configured session with custom proxy')
        
        async with httpx.AsyncClient(**client_kwargs) as session:
            # First, try AJAX endpoints if we have keyword and region
            if keyword and not start_url:
                Actor.log.info('Attempting AJAX endpoint detection...')
                ajax_jobs = await try_ajax_endpoints(session, 'https://jooble.org', keyword, region)
                for item in ajax_jobs:
                    job_url = item.get('job_url')
                    if job_url and job_url not in seen_urls:
                        item['source_url'] = 'ajax_endpoint'
                        item['page_number'] = 0
                        await Actor.push_data(item)
                        total_pushed += 1
                        seen_urls.add(job_url)
                
                if ajax_jobs:
                    Actor.log.info(f'Found {len(ajax_jobs)} jobs via AJAX endpoints')

            for page in range(1, max_pages + 1):
                url = page_url(page)
                Actor.log.info(f'Scraping search page {page}: {url}')

                html = await fetch_search_page(session, url, referer=referer_url, effective_proxy=effective_proxy)
                referer_url = url
                if not html:
                    Actor.log.warning(f'Empty HTML for {url}, stopping pagination.')
                    break

                # Debug: Log HTML stats
                Actor.log.info(f'HTML length: {len(html)} chars')
                html_lower = html.lower()
                has_job = 'job' in html_lower
                has_redirect = 'redirect' in html_lower
                has_vacancy = 'vacancy' in html_lower
                has_position = 'position' in html_lower
                Actor.log.info(f'HTML contains: job={has_job}, redirect={has_redirect}, vacancy={has_vacancy}, position={has_position}')
                
                # Save HTML sample for debugging (first page only)
                if page == 1:
                    try:
                        await Actor.set_value('debug_html_sample', html[:50000], content_type='text/html')
                        Actor.log.info('Saved HTML sample to key-value store (debug_html_sample)')
                    except Exception as e:
                        Actor.log.debug(f'Failed to save HTML sample: {e}')

                soup = BeautifulSoup(html, 'lxml')
                
                # Debug: Count all links
                all_links = soup.find_all('a', href=True)
                Actor.log.info(f'Found {len(all_links)} total links on page')
                redirect_links = [a for a in all_links if 'redirect' in a.get('href', '').lower()]
                job_links = [a for a in all_links if any(p in a.get('href', '').lower() for p in ['job', 'vacancy', 'position'])]
                Actor.log.info(f'Found {len(redirect_links)} redirect links, {len(job_links)} job-related links')
                blocks = extract_job_blocks(soup)
                if not blocks:
                    # Log what we found for debugging
                    Actor.log.warning(f'No job blocks found. Total links: {len(all_links)}, Redirect links: {len(redirect_links)}, Job links: {len(job_links)}')

                collected_any = False

                new_items_on_page = 0
                for block in blocks:
                    item = parse_job_block(block, page_url=url)
                    if not item:
                        continue
                    job_url = item.get('job_url')
                    if job_url and job_url in seen_urls:
                        continue

                    # Enrich with metadata
                    item['source_url'] = url
                    item['page_number'] = page
                    await Actor.push_data(item)
                    total_pushed += 1
                    new_items_on_page += 1
                    if job_url:
                        seen_urls.add(job_url)
                    collected_any = True

                if not collected_any:
                    # Try JSON-LD fallback
                    ld_jobs = extract_jobs_from_ld_json(soup)
                    for item in ld_jobs:
                        job_url = item.get('job_url')
                        if job_url and job_url in seen_urls:
                            continue
                        item['source_url'] = url
                        item['page_number'] = page
                        await Actor.push_data(item)
                        total_pushed += 1
                        new_items_on_page += 1
                        if job_url:
                            seen_urls.add(job_url)
                        collected_any = True

                if not collected_any:
                    # As a last resort, scan anchors directly for job links
                    link_jobs = extract_jobs_from_links(soup, page_url=url)
                    for item in link_jobs:
                        job_url = item.get('job_url')
                        if job_url and job_url in seen_urls:
                            continue
                        item['source_url'] = url
                        item['page_number'] = page
                        await Actor.push_data(item)
                        total_pushed += 1
                        new_items_on_page += 1
                        if job_url:
                            seen_urls.add(job_url)
                    if new_items_on_page == 0:
                        Actor.log.info('No job blocks, JSON-LD, or anchor-based jobs detected on page, ending.')
                        break

                Actor.log.info(f'Page {page}: pushed {new_items_on_page} new items (total {total_pushed}).')

                if max_jobs > 0 and total_pushed >= max_jobs:
                    Actor.log.info(f'Reached maxJobs limit ({max_jobs}). Stopping.')
                    break

                if new_items_on_page == 0:
                    Actor.log.info('No new items on page; stopping pagination early.')
                    break

        Actor.log.info(f'Scrape complete. Total items: {total_pushed}.')