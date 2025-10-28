import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset, sleep } from 'crawlee';
import { load as loadCheerio } from 'cheerio';
import { gotScraping } from 'got-scraping';

const BASE_URL = 'https://jooble.org';
const DETAIL_BATCH = 5;
const MAX_RETRIES = 2;

// ---------- Helper Functions ----------

function randomUA() {
    const UAS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128 Safari/537.36',
    ];
    return UAS[Math.floor(Math.random() * UAS.length)];
}

function extractJobLinks(html, base) {
    const $ = loadCheerio(html);
    const links = new Set();
    $('a[href*="/desc/"], a[data-qa="vacancy-serp__vacancy-title"], a[class*="job-link"]').each((_, el) => {
        const href = $(el).attr('href');
        if (href) {
            const abs = href.startsWith('http') ? href : new URL(href, base).href;
            if (abs.includes('/desc/')) links.add(abs);
        }
    });
    return [...links];
}

async function extractJobData(page, url) {
    const job = {
        title: '',
        company: '',
        location: '',
        salary: '',
        description: '',
        job_url: url,
        scrapedAt: new Date().toISOString(),
    };

    try {
        job.title = (await page.textContent('h1, .job-title, .vacancy-title').catch(() => ''))?.trim() || '';
        job.company = (await page.textContent('.company, .employer, .company-name').catch(() => ''))?.trim() || '';
        job.location = (await page.textContent('.location, .job-location').catch(() => ''))?.trim() || '';
        job.salary = (await page.textContent('.salary, .compensation').catch(() => ''))?.trim() || '';
        job.description =
            (await page.textContent('.job-description, .vacancy-description, main, article').catch(() => ''))
                ?.trim()
                .slice(0, 4000) || '';
    } catch (err) {
        log.warning(`⚠️ Error parsing ${url}: ${err.message}`);
    }
    return job;
}

function chunk(arr, n) {
    const out = [];
    for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
    return out;
}

// Auto-fetch consent cookie (EU regions)
async function getConsentCookie() {
    try {
        const res = await gotScraping({
            url: `${BASE_URL}/SearchResult?ukw=test`,
            timeout: { request: 8000 },
        });
        const cookie = (res.headers['set-cookie'] || []).find((c) => c.includes('consent'));
        if (cookie) {
            log.info('✅ Fetched consent cookie');
            return cookie.split(';')[0];
        }
        log.info('ℹ️ No consent cookie found');
        return null;
    } catch (err) {
        log.warning('⚠️ Consent cookie fetch failed: ' + err.message);
        return null;
    }
}

// ---------- Main Execution ----------

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    searchQuery = 'developer',
    maxPages = 3,
    maxItems = 20,
    fastMode = true,
    detailConcurrency = DETAIL_BATCH,
} = input;

log.info(`🎬 Jooble scraper started | query="${searchQuery}" maxPages=${maxPages} maxItems=${maxItems}`);

const proxyConfiguration = await Actor.createProxyConfiguration({
    useApifyProxy: true,
    apifyProxyGroups: fastMode ? [] : ['RESIDENTIAL'],
});
log.info('✅ Proxy configured');

const consentCookie = await getConsentCookie();
let saved = 0;

const crawler = new PlaywrightCrawler({
    proxyConfiguration,
    navigationTimeoutSecs: 20,
    requestHandlerTimeoutSecs: 45,
    maxRequestsPerCrawl: maxPages * 20,
    useSessionPool: true,
    sessionPoolOptions: { maxPoolSize: 20, sessionOptions: { maxUsageCount: 3 } },

    launchContext: {
        launchOptions: {
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-gpu',
            ],
        },
    },

    preNavigationHooks: [
        async ({ page }) => {
            // block heavy resources
            await page.route('**/*', (route) => {
                const t = route.request().resourceType();
                if (['image', 'stylesheet', 'font', 'media'].includes(t)) route.abort();
                else route.continue();
            });

            // stealth
            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            });

            // ✅ Correct way to set UA in Playwright (via context)
            const ua = randomUA();
            await page.context().setExtraHTTPHeaders({
                'User-Agent': ua,
                'Accept-Language': 'en-US,en;q=0.9',
            });

            // consent cookie
            if (consentCookie) {
                const [name, value] = consentCookie.split('=');
                await page.context().addCookies([{ name, value, domain: 'jooble.org' }]);
            }
        },
    ],

    async requestHandler({ page, request, crawler, session }) {
        const label = request.userData?.label || 'SEARCH';
        const pageNum = request.userData?.page || 1;
        const retries = request.userData?.retries || 0;

        try {
            await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: 20000 });
        } catch (e) {
            log.warning(`⏱️ Timeout on ${request.url}`);
            session.retire();
            if (retries < MAX_RETRIES) {
                await crawler.addRequests([{ url: request.url, userData: { ...request.userData, retries: retries + 1 } }]);
            }
            return;
        }

        const html = await page.content();
        if (/captcha|verify|are you human|403 Forbidden/i.test(html)) {
            log.warning(`🚧 Blocked at ${request.url}`);
            session.retire();
            if (retries < MAX_RETRIES) {
                await crawler.addRequests([{ url: request.url, userData: { ...request.userData, retries: retries + 1 } }]);
            }
            return;
        }

        if (label === 'SEARCH') {
            const jobLinks = extractJobLinks(html, request.url);
            log.info(`🔎 Page ${pageNum}: found ${jobLinks.length} job links`);
            const limited = jobLinks.slice(0, maxItems - saved);
            const batches = chunk(limited, detailConcurrency);

            for (const batch of batches) {
                await Promise.allSettled(
                    batch.map(async (url) => {
                        const detail = await crawler.browserPool.newPage();
                        try {
                            await detail.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 });
                            const data = await extractJobData(detail, url);
                            if (data.title) {
                                await Dataset.pushData(data);
                                saved++;
                                log.info(`✅ [${saved}] ${data.title}`);
                            }
                        } catch (err) {
                            log.warning(`❌ Detail failed ${url}: ${err.message}`);
                        } finally {
                            await detail.close().catch(() => {});
                        }
                    }),
                );
                if (saved >= maxItems) return;
                await sleep(800 + Math.random() * 500);
            }

            if (pageNum < maxPages && saved < maxItems) {
                const next = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}&p=${pageNum + 1}`;
                await crawler.addRequests([{ url: next, userData: { label: 'SEARCH', page: pageNum + 1 } }]);
                log.info(`📄 Queued page ${pageNum + 1}`);
            }
        }
    },

    failedRequestHandler({ request, error }) {
        log.error(`❌ Failed ${request.url}: ${error.message}`);
    },
});

// Start crawling
const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);

log.info(`🎉 Done! Total jobs scraped: ${saved}`);
await Actor.exit();
