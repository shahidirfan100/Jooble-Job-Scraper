/* ────────────────────────────────────────────────────────────── *
 *  Jooble job scraper – Apify Actor (Apify SDK + Crawlee)        *
 *  – works with `"type": "module"` (ESM)                         *
 * ────────────────────────────────────────────────────────────── */

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore } from 'crawlee';

// ------------------------------------------------------------------
// 1️⃣  INPUT HANDLING
// ------------------------------------------------------------------
async function getInput() {
    const raw = await KeyValueStore.getInput();

    const defaults = {
        searchQuery: 'software engineer',
        location: '',
        jobAge: 'all',           // 'all' | '1' | '7' | '30'
        maxPages: 5,
        maxConcurrency: 10,
        proxyConfiguration: { useApifyProxy: true },
        requestHeaders: {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept':
                'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Upgrade-Insecure-Requests': '1',
        },
    };

    const input = { ...defaults, ...(raw || {}) };

    // Defensive sanitizing
    if (!Number.isFinite(+input.maxPages) || +input.maxPages < 1) input.maxPages = 1;
    if (!Number.isFinite(+input.maxConcurrency) || +input.maxConcurrency < 1) input.maxConcurrency = 5;

    return input;
}

// ------------------------------------------------------------------
// 2️⃣  BUILD SEARCH URL
// ------------------------------------------------------------------
function buildSearchUrl({ searchQuery, location, page = 1, jobAge = 'all' }) {
    const base = 'https://jooble.org/SearchResult';
    const params = new URLSearchParams();

    if (searchQuery && searchQuery.trim()) params.set('ukw', searchQuery.trim());
    if (location && location.trim()) params.set('l', location.trim());
    if (page > 1) params.set('p', String(page));
    if (jobAge && jobAge !== 'all') params.set('date', String(jobAge));

    const qs = params.toString();
    return qs ? `${base}?${qs}` : base;
}

// ------------------------------------------------------------------
// 3️⃣  MAIN ACTOR FUNCTION (Apify entry point)
// ------------------------------------------------------------------
export async function main() {
    await Actor.init();

    try {
        const input = await getInput();
        log.info(`🎬 Starting Jooble scraper with query: "${input.searchQuery}"`);

        // ✅ Use Apify helper in the platform environment
        const proxyConfiguration = await Actor.createProxyConfiguration(input.proxyConfiguration);

        const crawler = new CheerioCrawler({
            proxyConfiguration,
            maxConcurrency: input.maxConcurrency,
            requestHandlerTimeoutSecs: 90,

            prepareRequestFunction: async ({ request }) => {
                request.headers = { ...request.headers, ...input.requestHeaders };
                return request;
            },

            async requestHandler({ $, request, enqueueLinks }) {
                const label = request.userData?.label ?? 'SEARCH';
                const page  = request.userData?.page ?? 1;

                if (label === 'SEARCH') {
                    await handleSearchPage($, enqueueLinks, request, input);
                } else if (label === 'DETAIL') {
                    await handleDetailPage($, request);
                } else {
                    log.warning(`⚠️ Unknown label "${label}" at ${request.url}`);
                }
            },

            failedRequestHandler({ request, error }) {
                log.error(`❌ Request failed ${request.url}: ${error?.message || error}`);
            },
        });

        await crawler.run([
            {
                url: buildSearchUrl({
                    searchQuery: input.searchQuery,
                    location: input.location,
                    page: 1,
                    jobAge: input.jobAge,
                }),
                userData: { label: 'SEARCH', page: 1 },
            },
        ]);

        log.info('✅ Crawling finished – check the default dataset for results.');
    } catch (err) {
        log.error('❌ Unexpected error in main():', err);
        if (err && err.stack) console.error(err.stack);
    } finally {
        await Actor.exit();
    }
}

// ------------------------------------------------------------------
// 4️⃣  SEARCH-RESULT PAGE HANDLER
// ------------------------------------------------------------------
async function handleSearchPage($, enqueueLinks, request, input) {
    const currentPage = request.userData?.page ?? 1;

    log.info(`🔍 Scraping search page ${currentPage}: ${request.url}`);

    // Collect detail links
    const jobLinks = [];
    $('a[href*="/desc/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;
        if (!href.includes('/desc/')) return;
        const fullUrl = href.startsWith('http') ? href : `https://jooble.org${href}`;
        if (!jobLinks.includes(fullUrl)) jobLinks.push(fullUrl);
    });

    log.info(`   Found ${jobLinks.length} job links on page ${currentPage}`);

    // ✅ Correct: set userData via transformRequestFunction
    if (jobLinks.length) {
        await enqueueLinks({
            urls: jobLinks,
            transformRequestFunction: (req) => {
                req.userData = { label: 'DETAIL', searchPage: currentPage };
                return req;
            },
        });
    }

    // Pagination
    if (currentPage < input.maxPages && jobLinks.length > 0) {
        const nextPage = currentPage + 1;
        const nextPageUrl = buildSearchUrl({
            searchQuery: input.searchQuery,
            location: input.location,
            page: nextPage,
            jobAge: input.jobAge,
        });

        log.info(`   ➡️ Enqueuing next page: ${nextPage}`);

        await enqueueLinks({
            urls: [nextPageUrl],
            transformRequestFunction: (req) => {
                req.userData = { label: 'SEARCH', page: nextPage };
                return req;
            },
        });
    } else {
        log.info(`   ⏹️ Stopping pagination at page ${currentPage}`);
    }
}

// ------------------------------------------------------------------
// 5️⃣  JOB-DETAIL PAGE HANDLER
// ------------------------------------------------------------------
async function handleDetailPage($, request) {
    log.info(`📄 Scraping job detail: ${request.url}`);

    const getFirst = (selectors) => {
        for (const sel of selectors) {
            const txt = $(sel).first().text().trim();
            if (txt) return txt;
        }
        return '';
    };

    const title = getFirst(['h1', '.job-title', '.title']);
    const company = getFirst(['.company', '.employer', '.company-name']);
    const location = getFirst(['.location', '.job-location']);
    const salary = getFirst(['.salary', '.pay', '.compensation']);

    const descNode = $('.job-description, .description, .vacancy-description, .content, main').first();
    const descriptionHtml = descNode.html() || '';
    const descriptionText = descNode.text().replace(/\s+/g, ' ').trim() || '';

    const job = {
        title,
        company,
        location,
        salary,
        description_html: descriptionHtml,
        description_text: descriptionText,
        job_url: request.url,
        scrapedAt: new Date().toISOString(),
        source: 'Jooble',
    };

    if (job.title) {
        await Dataset.pushData(job);
        log.info(`   ✅ Saved: "${job.title}"`);
    } else {
        log.warning(`   ⚠️ No title found for ${request.url}`);
    }
}

// ------------------------------------------------------------------
// 6️⃣  LOCAL TESTING ENTRY-POINT
// ------------------------------------------------------------------
if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch((e) => {
        log.error('❌ Unexpected error in main():', e);
        if (e && e.stack) console.error(e.stack);
        process.exit(1);
    });
}
