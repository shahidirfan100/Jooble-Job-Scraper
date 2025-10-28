import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { load as loadCheerio } from 'cheerio';

const BASE_URL = 'https://jooble.org';
const DETAIL_BATCH_SIZE_DEFAULT = 6;     // concurrent job detail tabs
const MAX_RETRIES = 2;                   // per URL (search page) retries

// --- Helpers ----------------------------------------------------

function extractJobLinks(html, baseUrl) {
    const $ = loadCheerio(html);
    const links = new Set();
    // multiple patterns to be robust to markup changes
    $('a[href*="/desc/"], a[data-qa="vacancy-serp__vacancy-title"], a[class*="job-link"], a[class*="position-link"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;
        const abs = href.startsWith('http') ? href : new URL(href, baseUrl).href;
        if (abs.includes('/desc/')) links.add(abs);
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
        // Quick text selectors; tolerate missing nodes
        job.title = (await page.textContent('h1, .job-title, .vacancy-title').catch(() => ''))?.trim() || '';
        job.company = (await page.textContent('.company, .employer, .company-name').catch(() => ''))?.trim() || '';
        job.location = (await page.textContent('.location, .job-location').catch(() => ''))?.trim() || '';
        job.salary = (await page.textContent('.salary, .compensation').catch(() => ''))?.trim() || '';
        job.description = (await page.textContent('.job-description, .vacancy-description, main, article').catch(() => ''))?.trim().slice(0, 4000) || '';
    } catch (err) {
        log.warning(`⚠️ Error parsing ${url}: ${err.message}`);
    }
    return job;
}

function chunk(arr, size) {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
}

// --- Main -------------------------------------------------------

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    searchQuery = 'developer',
    maxPages = 3,
    maxItems = 30,
    fastMode = true,                          // true => Apify shared proxy; false => RESIDENTIAL
    detailConcurrency = DETAIL_BATCH_SIZE_DEFAULT,
    navTimeoutSecs = 15,                      // navigation timeout per page
} = input;

log.info(`🎬 Jooble scraper started | query="${searchQuery}" maxPages=${maxPages} maxItems=${maxItems}`);

const proxyConfiguration = await Actor.createProxyConfiguration({
    useApifyProxy: true,
    apifyProxyGroups: fastMode ? [] : ['RESIDENTIAL'],
});
log.info('✅ Proxy configured');

let saved = 0;

const crawler = new PlaywrightCrawler({
    proxyConfiguration,
    maxRequestsPerCrawl: Math.max(50, maxPages * 20),
    navigationTimeoutSecs: navTimeoutSecs,
    requestHandlerTimeoutSecs: 45,
    useSessionPool: true,

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

    // Must be an array in Crawlee 3.15.x
    preNavigationHooks: [
        async ({ page }) => {
            // Block heavy resources to speed up loads
            await page.route('**/*', (route) => {
                const type = route.request().resourceType();
                if (['image', 'stylesheet', 'font', 'media'].includes(type)) route.abort();
                else route.continue();
            });
            // simple stealth tweaks
            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            });
            await page.setViewportSize({ width: 1366, height: 768 });
        },
    ],

    async requestHandler(ctx) {
        const { page, request, crawler, session } = ctx;
        const label = request.userData?.label || 'SEARCH';
        const pageNum = request.userData?.page || 1;
        const retries = request.userData?.retries || 0;

        // --- Robust navigation with timeout + retry wiring
        try {
            await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: navTimeoutSecs * 1000 });
        } catch (err) {
            log.warning(`⏱️ Nav timeout on ${request.url} (try ${retries + 1}/${MAX_RETRIES + 1}): ${err.message}`);
            session.retire();
            if (retries < MAX_RETRIES) {
                await crawler.addRequests([{ url: request.url, userData: { ...request.userData, retries: retries + 1 } }]);
            } else {
                log.error(`❌ Gave up on ${request.url} after ${retries + 1} tries`);
            }
            return;
        }

        const html = await page.content();
        if (/captcha|verify|are you human|before you continue/i.test(html)) {
            log.warning(`🚧 Bot/consent wall on ${request.url}`);
            session.retire();
            if (retries < MAX_RETRIES) {
                await crawler.addRequests([{ url: request.url, userData: { ...request.userData, retries: retries + 1 } }]);
            }
            return;
        }

        // --- SEARCH PAGE ---
        if (label === 'SEARCH') {
            const jobLinks = extractJobLinks(html, request.url);
            log.info(`🔎 Page ${pageNum}: found ${jobLinks.length} job links`);

            if (!jobLinks.length) {
                log.warning(`⚠️ No jobs on ${request.url}`);
            } else {
                const limited = jobLinks.slice(0, Math.max(0, maxItems - saved));
                const batches = chunk(limited, Math.max(1, detailConcurrency));

                for (const batch of batches) {
                    // Process a batch of detail tabs in parallel
                    await Promise.allSettled(batch.map(async (jobUrl) => {
                        const detailPage = await crawler.browserPool.newPage();
                        try {
                            await detailPage.goto(jobUrl, { waitUntil: 'domcontentloaded', timeout: navTimeoutSecs * 1000 });
                            const data = await extractJobData(detailPage, jobUrl);
                            if (data.title) {
                                await Dataset.pushData(data);
                                saved++;
                                log.info(`✅ [${saved}] ${data.title}`);
                            }
                        } catch (e) {
                            log.warning(`❌ Detail failed: ${jobUrl} | ${e.message}`);
                        } finally {
                            await detailPage.close().catch(() => {});
                        }
                    }));

                    if (saved >= maxItems) {
                        log.info(`🎯 Reached maxItems (${maxItems}).`);
                        break;
                    }
                    // short human-like pause between batches
                    await Actor.sleep(600 + Math.random() * 400);
                }
            }

            // Queue next result page
            if (pageNum < maxPages && saved < maxItems) {
                const nextUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}&p=${pageNum + 1}`;
                await crawler.addRequests([{ url: nextUrl, userData: { label: 'SEARCH', page: pageNum + 1 } }]);
                log.info(`📄 Queued next page ${pageNum + 1}`);
            }
            return;
        }

        // (If you later add a queued DETAIL label path, it can go here.)
    },

    failedRequestHandler({ request, error }) {
        log.error(`❌ Failed: ${request.url} | ${error?.message || error}`);
    },
});

// Kick off
const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1, retries: 0 } }]);

log.info(`🎉 Done! Total jobs scraped: ${saved}`);
await Actor.exit();
