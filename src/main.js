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

// ---------- Warm-up cookies before crawling ----------

async function warmupCookies(proxyConfiguration) {
    try {
        const proxyUrl = proxyConfiguration.newUrl();
        const res = await gotScraping({
            url: BASE_URL,
            proxyUrl,
            timeout: { request: 8000 },
        });
        const cookies = res.headers['set-cookie'] || [];
        if (cookies.length) {
            log.info(`✅ Warm-up cookies received (${cookies.length})`);
        } else {
            log.info('ℹ️ No cookies set during warm-up');
        }
        return cookies;
    } catch (err) {
        log.warning('⚠️ Warm-up cookie fetch failed: ' + err.message);
        return [];
    }
}

// ---------- Main Execution ----------

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    searchQuery = 'developer',
    maxPages = 3,
    maxItems = 20,
    detailConcurrency = DETAIL_BATCH,
} = input;

log.info(`🎬 Jooble scraper started | query="${searchQuery}" maxPages=${maxPages} maxItems=${maxItems}`);

// Force residential proxy group to bypass 403
const proxyConfiguration = await Actor.createProxyConfiguration({
    useApifyProxy: true,
    apifyProxyGroups: ['RESIDENTIAL'],
    countryCode: 'US',
});
log.info('✅ Residential proxy configured');

const warmupCookiesList = await warmupCookies(proxyConfiguration);
let saved = 0;

const crawler = new PlaywrightCrawler({
    proxyConfiguration,
    navigationTimeoutSecs: 25,
    requestHandlerTimeoutSecs: 50,
    maxRequestsPerCrawl: maxPages * 20,
    maxConcurrency: 2,
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
            const ua = randomUA();
            const headers = {
                'User-Agent': ua,
                'Accept-Language': 'en-US,en;q=0.9',
                'Sec-CH-UA': '"Chromium";v="128", "Not:A-Brand";v="99"',
                'Sec-CH-UA-Mobile': '?0',
                'Sec-CH-UA-Platform': '"Windows"',
                'Upgrade-Insecure-Requests': '1',
                'Accept':
                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            };
            await page.context().setExtraHTTPHeaders(headers);

            await page.route('**/*', (route) => {
                const t = route.request().resourceType();
                if (['image', 'stylesheet', 'font', 'media'].includes(t)) route.abort();
                else route.continue();
            });

            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            });

            // Add cookies from warmup
            if (warmupCookiesList.length) {
                const parsed = warmupCookiesList.map((c) => {
                    const [name, value] = c.split(';')[0].split('=');
                    return { name, value, domain: 'jooble.org' };
                });
                await page.context().addCookies(parsed);
            }

            await page.setViewportSize({ width: 1366, height: 768 });
        },
    ],

    async requestHandler({ page, request, crawler, session }) {
        const label = request.userData?.label || 'SEARCH';
        const pageNum = request.userData?.page || 1;
        const retries = request.userData?.retries || 0;

        try {
            await sleep(500 + Math.random() * 1000); // human delay
            await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: 25000 });
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

const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);

log.info(`🎉 Done! Total jobs scraped: ${saved}`);
await Actor.exit();
