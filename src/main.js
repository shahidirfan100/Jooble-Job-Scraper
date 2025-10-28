import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { load as loadCheerio } from 'cheerio';

const BASE_URL = 'https://jooble.org';
const DETAIL_BATCH_SIZE = 6; // concurrent detail pages
const MAX_RETRIES = 2;

// Extract job links from a search result page
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

// Extract structured job data
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
        job.description = (
            await page.textContent('.job-description, .vacancy-description, main, article').catch(() => '')
        )
            ?.trim()
            .slice(0, 4000);
    } catch (err) {
        log.warning(`⚠️ Error extracting data on ${url}: ${err.message}`);
    }
    return job;
}

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    searchQuery = 'developer',
    maxPages = 3,
    maxItems = 30,
    fastMode = true,
    detailConcurrency = DETAIL_BATCH_SIZE,
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
    navigationTimeoutSecs: 15,
    requestHandlerTimeoutSecs: 40,
    maxRequestsPerCrawl: maxPages * 20,
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

    // ✅ FIXED: preNavigationHooks must be an array
    preNavigationHooks: [
        async ({ page }) => {
            // block heavy resources for faster load
            await page.route('**/*', (route) => {
                const type = route.request().resourceType();
                if (['image', 'stylesheet', 'font', 'media'].includes(type)) route.abort();
                else route.continue();
            });
            // stealth tweaks
            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            });
            await page.setViewportSize({ width: 1920, height: 1080 });
        },
    ],

    async requestHandler({ page, request, crawler }) {
        const label = request.userData?.label || 'SEARCH';
        const pageNum = request.userData?.page || 1;

        await page.goto(request.url, { waitUntil: 'domcontentloaded' }).catch(() => {
            throw new Error('Timeout loading page');
        });

        const html = await page.content();
        if (/captcha|verify|are you human/i.test(html)) {
            log.warning(`🚧 Bot wall detected at ${request.url}`);
            throw new Error('BotWall');
        }

        // ========== SEARCH PAGE ==========
        if (label === 'SEARCH') {
            const jobLinks = extractJobLinks(html, request.url);
            log.info(`🔎 Page ${pageNum}: found ${jobLinks.length} job links`);

            if (!jobLinks.length) {
                log.warning(`⚠️ No jobs on ${request.url}`);
                return;
            }

            // parallel batches of job details
            const limited = jobLinks.slice(0, Math.max(0, maxItems - saved));
            const batches = [];
            for (let i = 0; i < limited.length; i += detailConcurrency) {
                batches.push(limited.slice(i, i + detailConcurrency));
            }

            for (const batch of batches) {
                await Promise.allSettled(
                    batch.map(async (jobUrl) => {
                        const detailPage = await crawler.browserPool.newPage();
                        try {
                            await detailPage.goto(jobUrl, {
                                waitUntil: 'domcontentloaded',
                                timeout: 15000,
                            });
                            const data = await extractJobData(detailPage, jobUrl);
                            if (data.title) {
                                await Dataset.pushData(data);
                                saved++;
                                log.info(`✅ [${saved}] ${data.title}`);
                            }
                        } catch (err) {
                            log.warning(`❌ Detail failed: ${jobUrl} | ${err.message}`);
                        } finally {
                            await detailPage.close();
                        }
                    }),
                );
                if (saved >= maxItems) {
                    log.info(`🎯 Reached maxItems (${maxItems}), stopping early.`);
                    return;
                }
                // human-like delay between batches
                await Actor.sleep(800 + Math.random() * 400);
            }

            // paginate
            if (pageNum < maxPages && saved < maxItems) {
                const nextPage = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(
                    searchQuery,
                )}&p=${pageNum + 1}`;
                await crawler.addRequests([{ url: nextPage, userData: { label: 'SEARCH', page: pageNum + 1 } }]);
                log.info(`📄 Queued next page ${pageNum + 1}`);
            }
        }
    },

    handlePageTimeout: async ({ request, session, error }) => {
        log.warning(`⚠️ Timeout at ${request.url}, retiring session`);
        session.retire();
        throw error;
    },

    failedRequestHandler({ request, error }) {
        log.error(`❌ Failed: ${request.url} | ${error.message}`);
    },
});

const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);

log.info(`🎉 Done! Total jobs scraped: ${saved}`);
await Actor.exit();
