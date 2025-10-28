import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset, sleep } from 'crawlee';
import { load as loadCheerio } from 'cheerio';
import got from 'got-scraping';

const BASE_URL = 'https://jooble.org';
const DETAIL_BATCH = 5;
const MAX_RETRIES = 2;

// --- helpers ------------------------------------------------------
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
    const set = new Set();
    $('a[href*="/desc/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (href) set.add(href.startsWith('http') ? href : new URL(href, base).href);
    });
    return [...set];
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
            (await page.textContent('.job-description, main, article').catch(() => ''))?.trim().slice(0, 4000) || '';
    } catch {}
    return job;
}
function chunk(arr, n) {
    const out = [];
    for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
    return out;
}

// --- optional: auto-fetch consent cookie --------------------------
async function getConsentCookie() {
    try {
        const res = await got(`${BASE_URL}/SearchResult?ukw=test`, { timeout: 8000 });
        const cookie = (res.headers['set-cookie'] || []).find(c => c.includes('consent'));
        if (cookie) log.info('✅ Fetched consent cookie');
        return cookie?.split(';')[0];
    } catch {
        log.warning('⚠️ Consent cookie fetch failed');
        return null;
    }
}

// --- main ---------------------------------------------------------
await Actor.init();
const input = (await Actor.getInput()) || {};
const { searchQuery = 'developer', maxPages = 3, maxItems = 20, fastMode = true } = input;

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
            args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-blink-features=AutomationControlled'],
        },
    },

    preNavigationHooks: [
        async ({ page, session }) => {
            // resource blocking
            await page.route('**/*', route => {
                const t = route.request().resourceType();
                if (['image', 'stylesheet', 'font', 'media'].includes(t)) route.abort();
                else route.continue();
            });
            // stealth
            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {} };
            });
            // rotating UA & cookie
            const ua = randomUA();
            await page.setUserAgent(ua);
            if (consentCookie) await page.context().addCookies([{ name: 'consent', value: 'true', domain: 'jooble.org' }]);
        },
    ],

    async requestHandler({ page, request, crawler, session }) {
        const label = request.userData?.label || 'SEARCH';
        const pageNum = request.userData?.page || 1;

        try {
            await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: 20000 });
        } catch (e) {
            log.warning(`⏱️ Timeout on ${request.url}`);
            session.retire();
            throw e;
        }

        const html = await page.content();
        if (/captcha|verify|are you human|403 Forbidden/i.test(html)) {
            log.warning(`🚧 Blocked at ${request.url}`);
            session.retire();
            throw new Error('Blocked');
        }

        if (label === 'SEARCH') {
            const jobLinks = extractJobLinks(html, request.url);
            log.info(`🔎 Page ${pageNum}: ${jobLinks.length} job links`);
            const limited = jobLinks.slice(0, maxItems - saved);
            for (const group of chunk(limited, DETAIL_BATCH)) {
                await Promise.allSettled(
                    group.map(async url => {
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
                    })
                );
                await sleep(800 + Math.random() * 500);
                if (saved >= maxItems) return;
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
