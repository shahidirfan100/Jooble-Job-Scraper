import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { load as loadCheerio } from 'cheerio';

const BASE_URL = 'https://jooble.org';
const MAX_RETRIES = 2;

function extractJobLinks(html, pageUrl) {
    const set = new Set();
    const hrefPattern = /href="([^"]*(?:\/desc\/|vacancy)[^"]*)"/gi;
    let match;
    while ((match = hrefPattern.exec(html)) !== null) {
        const href = match[1];
        if (href.includes('/desc/') || href.includes('vacancy')) {
            const abs = href.startsWith('http') ? href : new URL(href, pageUrl).href;
            set.add(abs);
        }
    }
    return [...set];
}

async function extractJobData(page, url) {
    const data = { title: '', company: '', location: '', description: '', job_url: url, scrapedAt: new Date().toISOString() };
    try {
        data.title = (await page.textContent('h1, .job-title, .vacancy-title').catch(() => ''))?.trim() || '';
        data.company = (await page.textContent('.company, .employer, .company-name').catch(() => ''))?.trim() || '';
        data.location = (await page.textContent('.location, .job-location').catch(() => ''))?.trim() || '';
        data.description = (
            await page.textContent('.job-description, .vacancy-description, article, main').catch(() => '')
        )
            ?.trim()
            .slice(0, 3000);
    } catch {}
    return data;
}

await Actor.init();

const input = (await Actor.getInput()) || {};
const { searchQuery = 'developer', maxPages = 3, maxItems = 20, fastMode = true } = input;
log.info(`🎬 Jooble scraper started | query="${searchQuery}" maxPages=${maxPages} maxItems=${maxItems}`);

const proxyConfiguration = await Actor.createProxyConfiguration({
    useApifyProxy: true,
    apifyProxyGroups: fastMode ? [] : ['RESIDENTIAL'],
});
log.info('✅ Proxy configured');

const crawler = new PlaywrightCrawler({
    proxyConfiguration,
    maxRequestsPerCrawl: maxPages * 20,
    navigationTimeoutSecs: 15,
    requestHandlerTimeoutSecs: 40,
    useSessionPool: true,
    launchContext: {
        launchOptions: {
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-images',
            ],
        },
    },
    async preNavigationHooks({ page, session }) {
        // block heavy resources
        await page.route('**/*', (route) => {
            const type = route.request().resourceType();
            if (['image', 'stylesheet', 'font', 'media'].includes(type)) route.abort();
            else route.continue();
        });

        await page.addInitScript(() => {
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        });
        session.userData.lastUsed = Date.now();
    },
    async requestHandler({ page, request, enqueueLinks, crawler }) {
        const label = request.userData?.label || 'SEARCH';
        const pageNum = request.userData?.page || 1;

        await page.goto(request.url, { waitUntil: 'domcontentloaded' }).catch((e) => {
            throw new Error(`Timeout navigating ${request.url}: ${e.message}`);
        });

        const html = await page.content();

        if (/captcha|verify|are you human/i.test(html)) {
            log.warning(`🚧 Bot wall on ${request.url}`);
            throw new Error('BotWall');
        }

        if (label === 'SEARCH') {
            const links = extractJobLinks(html, request.url);
            log.info(`🔎 Page ${pageNum}: found ${links.length} job links`);
            for (const url of links.slice(0, maxItems)) {
                await crawler.addRequests([{ url, userData: { label: 'DETAIL' } }]);
            }

            if (pageNum < maxPages) {
                const next = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}&p=${pageNum + 1}`;
                await crawler.addRequests([{ url: next, userData: { label: 'SEARCH', page: pageNum + 1 } }]);
            }
            return;
        }

        if (label === 'DETAIL') {
            const data = await extractJobData(page, request.url);
            if (data.title) {
                await Dataset.pushData(data);
                log.info(`✅ Saved job: ${data.title}`);
            }
        }
    },
    handlePageTimeout: async ({ request, session, error }) => {
        log.warning(`⚠️ Timeout on ${request.url}, retiring session...`);
        session.retire();
        throw error;
    },
    failedRequestHandler({ request, error }) {
        log.error(`❌ Failed ${request.url}: ${error.message}`);
    },
});

const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);
log.info('🎉 Finished run');

await Actor.exit();
