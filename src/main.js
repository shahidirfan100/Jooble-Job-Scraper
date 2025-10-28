import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';

const BASE_URL = 'https://jooble.org';
const MAX_RETRIES = 3;

const UAS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
];

function randomUA() {
    return UAS[Math.floor(Math.random() * UAS.length)];
}

function extractJobLinks(pageUrl, $) {
    const set = new Set();
    $('a[href*="/desc/"], a[data-qa="vacancy-serp__vacancy-title"], a[class*="job-link"], a[class*="link position-link"]').each((_, el) => {
        const href = $(el).attr('href');
        if (href) {
            const abs = href.startsWith('http') ? href : new URL(href, pageUrl).href;
            set.add(abs);
        }
    });
    return [...set];
}

export async function main() {
    await Actor.init();
    let saved = 0;

    try {
        const input = (await Actor.getInput()) || {};
        const {
            searchQuery = 'developer',
            maxPages = 3,
            maxItems = 50,
            maxConcurrency = 3
        } = input;

        const proxyConfiguration = await Actor.createProxyConfiguration({
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL']
        });

        const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;

        const crawler = new PlaywrightCrawler({
            proxyConfiguration,
            maxConcurrency,
            headless: true,
            navigationTimeoutSecs: 45,
            useSessionPool: true,
            persistCookiesPerSession: true,

            async preNavigationHooks({ page }) {
                const ua = randomUA();
                await page.setExtraHTTPHeaders({
                    'User-Agent': ua,
                    'Accept-Language': 'en-US,en;q=0.9'
                });
                await page.setUserAgent(ua);
                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                });
            },

            async requestHandler({ page, request, enqueueLinks }) {
                const label = request.userData?.label || 'SEARCH';
                const pageNum = request.userData?.page || 1;

                await page.waitForLoadState('domcontentloaded');
                const html = await page.content();

                if (/are you human|verify|before you continue/i.test(html)) {
                    log.warning(`🚧 Consent wall detected on ${request.url}`);
                    return;
                }

                if (label === 'SEARCH') {
                    const cheerio = (await import('cheerio')).load(html);
                    const links = extractJobLinks(request.url, cheerio);
                    log.info(`🔎 Page ${pageNum}: found ${links.length} jobs`);

                    for (const jobUrl of links.slice(0, maxItems - saved)) {
                        await enqueueLinks({
                            urls: [jobUrl],
                            transformRequestFunction: (r) => {
                                r.userData = { label: 'DETAIL', referer: request.url };
                                return r;
                            },
                        });
                    }

                    if (pageNum < maxPages && saved < maxItems) {
                        const nextUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}&p=${pageNum + 1}`;
                        await enqueueLinks({
                            urls: [nextUrl],
                            transformRequestFunction: (r) => {
                                r.userData = { label: 'SEARCH', page: pageNum + 1 };
                                return r;
                            },
                        });
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    const title = (await page.textContent('h1, .job-title, .title'))?.trim() || '';
                    const company = (await page.textContent('.company, .employer, .company-name'))?.trim() || '';
                    const location = (await page.textContent('.location, .job-location'))?.trim() || '';
                    const salary = (await page.textContent('.salary, .compensation'))?.trim() || '';
                    const description = (await page.textContent('.job-description, .description, .vacancy-description, .content, main'))?.trim() || '';

                    if (title) {
                        await Dataset.pushData({
                            title,
                            company,
                            location,
                            salary,
                            description,
                            job_url: request.url,
                            scrapedAt: new Date().toISOString(),
                        });
                        saved++;
                        log.info(`✅ Saved ${saved}: ${title}`);
                    }
                }
            },

            failedRequestHandler({ request, error }) {
                log.error(`❌ Failed ${request.url}: ${error.message}`);
            }
        });

        await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);
        log.info(`🎉 Done. Total jobs scraped: ${saved}`);
    } catch (e) {
        log.error(`❌ Fatal error in main(): ${e.message}`);
    } finally {
        await Actor.exit();
    }
}
