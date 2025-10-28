import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { load as loadCheerio } from 'cheerio';

const BASE_URL = 'https://jooble.org';
const MAX_RETRIES = 3;

const UAS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
];
const randomUA = () => UAS[Math.floor(Math.random() * UAS.length)];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

async function run() {
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

        log.info(`🎬 Jooble Playwright run started | query="${searchQuery}" pages=${maxPages} items=${maxItems} conc=${maxConcurrency}`);

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

            async preNavigationHooks({ page, request }) {
                const ua = randomUA();
                await page.setExtraHTTPHeaders({
                    'User-Agent': ua,
                    'Accept-Language': 'en-US,en;q=0.9'
                });
                await page.setUserAgent(ua);
                // Basic stealth tweak
                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                });
                log.debug(`➡️ Navigating: ${request.url}`);
            },

            async requestHandler({ page, request, enqueueLinks }) {
                const label = request.userData?.label || 'SEARCH';
                const pageNum = request.userData?.page || 1;

                await page.waitForLoadState('domcontentloaded', { timeout: 20000 });
                await sleep(400 + Math.random() * 600);

                const html = await page.content();

                // Consent / bot wall quick check
                if (/before you continue|are you human|verify|captcha/i.test(html)) {
                    log.warning(`🚧 Consent/bot wall on ${request.url}`);
                    if ((request.userData.retries || 0) < MAX_RETRIES) {
                        await enqueueLinks({
                            urls: [request.url],
                            transformRequestFunction: (r) => {
                                r.userData = { ...request.userData, retries: (request.userData.retries || 0) + 1 };
                                return r;
                            },
                        });
                    } else {
                        await Dataset.pushData({ error: 'Consent wall', url: request.url });
                    }
                    return;
                }

                // SEARCH page
                if (label === 'SEARCH') {
                    const $ = loadCheerio(html);
                    const links = extractJobLinks(request.url, $);
                    log.info(`🔎 Search page ${pageNum}: found ${links.length} job links`);

                    for (const jobUrl of links.slice(0, Math.max(0, maxItems - saved))) {
                        await enqueueLinks({
                            urls: [jobUrl],
                            transformRequestFunction: (r) => {
                                r.userData = { label: 'DETAIL', referer: request.url };
                                return r;
                            },
                        });
                    }

                    // pagination
                    if (pageNum < maxPages && saved < maxItems) {
                        const next = new URL(request.url);
                        next.searchParams.set('p', String(pageNum + 1));
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

                // DETAIL page
                if (label === 'DETAIL') {
                    const title = (await page.textContent('h1, .job-title, .title'))?.trim() || '';
                    const company = (await page.textContent('.company, .employer, .company-name'))?.trim() || '';
                    const location = (await page.textContent('.location, .job-location'))?.trim() || '';
                    const salary = (await page.textContent('.salary, .compensation'))?.trim() || '';
                    const description = (await page.textContent('.job-description, .description, .vacancy-description, .content, main'))?.trim() || '';

                    if (title) {
                        await Dataset.pushData({
                            title, company, location, salary, description,
                            job_url: request.url,
                            scrapedAt: new Date().toISOString(),
                        });
                        saved++;
                        log.info(`✅ Saved #${saved}: ${title}`);
                    } else {
                        log.warning(`⚠️ No title on ${request.url}`);
                    }
                }
            },

            failedRequestHandler({ request, error }) {
                log.error(`❌ Failed: ${request.url} – ${error?.message || error}`);
            },
        });

        await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);
        log.info(`🎉 Done. Total jobs scraped: ${saved}`);
    } catch (err) {
        log.error('❌ Fatal error in run():', err);
    } finally {
        await Actor.exit();
    }
}

/**
 * 🔴 IMPORTANT: Make sure main is actually invoked.
 * On Apify and locally, this guarantees the actor runs (no silent exit).
 */
run().catch((e) => {
    log.error('❌ Top-level run error:', e);
    process.exit(1);
});
