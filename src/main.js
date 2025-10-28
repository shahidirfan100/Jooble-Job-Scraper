import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';

// ───────────────────────────────────────────────────────────────
// 1️⃣  Utility & Config
// ───────────────────────────────────────────────────────────────
const BASE_URL = 'https://jooble.org';
const MAX_RETRIES = 3;

// Common modern UAs for stealth rotation
const UAS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1',
];
function randomUA() {
    return UAS[Math.floor(Math.random() * UAS.length)];
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function extractJobLinks(pageUrl, $) {
    const set = new Set();
    $('a[href*="/desc/"], a[data-qa="vacancy-serp__vacancy-title"], a[class*="job-link"], a[class*="link position-link"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;
        const abs = href.startsWith('http') ? href : new URL(href, pageUrl).href;
        set.add(abs);
    });
    return [...set];
}

// ───────────────────────────────────────────────────────────────
// 2️⃣  Main
// ───────────────────────────────────────────────────────────────
export async function main() {
    await Actor.init();
    let saved = 0;

    try {
        const input = (await Actor.getInput()) || {};
        const {
            searchQuery = 'developer',
            maxPages = 3,
            maxItems = 50,
            maxConcurrency = 3,
            slowMo = 100,
        } = input;

        const proxyConfiguration = await Actor.createProxyConfiguration({
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL'],
        });

        const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;

        // ───────────────────────────────────────────────────────────────
        // 3️⃣  PlaywrightCrawler Setup
        // ───────────────────────────────────────────────────────────────
        const crawler = new PlaywrightCrawler({
            proxyConfiguration,
            maxConcurrency,
            headless: true,
            navigationTimeoutSecs: 45,
            launchContext: {
                launchOptions: {
                    headless: true,
                    args: [
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-gpu',
                        '--disable-dev-shm-usage',
                        '--disable-infobars',
                        '--window-size=1280,720',
                    ],
                },
            },
            useSessionPool: true,
            persistCookiesPerSession: true,

            async preNavigationHooks({ page, request, session }) {
                const ua = randomUA();
                await page.setExtraHTTPHeaders({
                    'User-Agent': ua,
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                });
                await page.setUserAgent(ua);
                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                });
            },

            async requestHandler({ page, request, enqueueLinks, session }) {
                const label = request.userData?.label || 'SEARCH';
                const pageNum = request.userData?.page || 1;

                try {
                    await page.waitForLoadState('domcontentloaded', { timeout: 20000 });
                    await sleep(1000 + Math.random() * 1000);

                    // Detect Cloudflare / consent pages
                    const content = await page.content();
                    if (/before you continue to jooble|are you human|verify/i.test(content)) {
                        log.warning(`🚧 Cookie/consent wall on ${request.url}`);
                        session.markBad();
                        session.retire();
                        if ((request.userData.retries || 0) < MAX_RETRIES) {
                            await enqueueLinks({
                                urls: [request.url],
                                transformRequestFunction: (r) => {
                                    r.userData = { ...request.userData, retries: (request.userData.retries || 0) + 1 };
                                    return r;
                                },
                            });
                        }
                        return;
                    }

                    // ───── SEARCH PAGE ─────
                    if (label === 'SEARCH') {
                        const $ = await page.$eval('body', () => document.body.innerHTML);
                        const cheerio = (await import('cheerio')).load($);
                        const links = extractJobLinks(request.url, cheerio);

                        log.info(`🔎 Page ${pageNum}: Found ${links.length} job links`);

                        for (const jobUrl of links.slice(0, maxItems - saved)) {
                            await enqueueLinks({
                                urls: [jobUrl],
                                transformRequestFunction: (r) => {
                                    r.userData = { label: 'DETAIL', referer: request.url };
                                    return r;
                                },
                            });
                        }

                        // Pagination
                        if (pageNum < maxPages && saved < maxItems) {
                            const next = new URL(request.url);
                            next.searchParams.set('p', pageNum + 1);
                            await enqueueLinks({
                                urls: [next.href],
                                transformRequestFunction: (r) => {
                                    r.userData = { label: 'SEARCH', page: pageNum + 1 };
                                    return r;
                                },
                            });
                        }
                        return;
                    }

                    // ───── DETAIL PAGE ─────
                    if (label === 'DETAIL') {
                        const title = await page.textContent('h1, .job-title, .title');
                        const company = await page.textContent('.company, .employer, .company-name');
                        const location = await page.textContent('.location, .job-location');
                        const salary = await page.textContent('.salary, .compensation');
                        const description = await page.textContent('.job-description, .description, .vacancy-description, .content, main');

                        if (title) {
                            await Dataset.pushData({
                                title: title?.trim() || '',
                                company: company?.trim() || '',
                                location: location?.trim() || '',
                                salary: salary?.trim() || '',
                                description: description?.trim() || '',
                                job_url: request.url,
                                scrapedAt: new Date().toISOString(),
                            });
                            saved++;
                            log.info(`✅ Saved #${saved}: ${title}`);
                        } else {
                            log.warning(`⚠️ No title found on ${request.url}`);
                        }
                    }
                } catch (e) {
                    log.error(`❌ Handler error on ${request.url}: ${e.message}`);
                    session.markBad();
                }
            },

            failedRequestHandler({ request, error }) {
                log.error(`❌ Failed permanently ${request.url} – ${error?.message || error}`);
            },
        });

        await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);
        log.info(`🎉 Done. Scraped ${saved} job(s).`);
    } catch (err) {
        log.error('❌ Unexpected error in main():', err);
    } finally {
        await Actor.exit();
    }
}
