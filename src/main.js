/* ────────────────────────────────────────────────────────────── *
 *  Jooble job scraper – Apify Actor (Apify SDK + Crawlee)        *
 *  – works with `"type": "module"` (ESM)                         *
 * ────────────────────────────────────────────────────────────── */

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, KeyValueStore, ProxyConfiguration } from 'crawlee';

// ------------------------------------------------------------------
// 1️⃣  INPUT HANDLING
// ------------------------------------------------------------------
async function getInput() {
    const raw = await KeyValueStore.getInput();

    const defaults = {
        searchQuery: 'software engineer',
        location: '',
        jobAge: 'all',
        maxPages: 5,
        maxConcurrency: 10,
        proxyConfiguration: { useApifyProxy: true },
        requestHeaders: {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            Accept:
                'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Upgrade-Insecure-Requests': '1',
        },
    };

    return { ...defaults, ...raw };
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
    if (jobAge && jobAge !== 'all') params.set('date', jobAge);
    return `${base}?${params.toString()}`;
}

// ------------------------------------------------------------------
// 3️⃣  MAIN ACTOR FUNCTION (Apify entry point)
// ------------------------------------------------------------------
export async function main() {
    await Actor.init();

    try {
        const input = await getInput();
        log.info(`🎬 Starting Jooble scraper with query: "${input.searchQuery}"`);

        // ✅ FIXED: Correct ProxyConfiguration creation
        const proxyConfiguration = await ProxyConfiguration.create(input.proxyConfiguration);

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
                const page = request.userData?.page ?? 1;

                if (label === 'SEARCH') {
                    await handleSearchPage($, enqueueLinks, request, input);
                } else if (label === 'DETAIL') {
                    await handleDetailPage($, request);
                } else {
                    log.warning(`⚠️ Unknown label "${label}" at ${request.url}`);
                }
            },

            failedRequestHandler({ request, error }) {
                log.error(`❌ Request failed ${request.url}: ${error.message}`);
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
    } finally {
        await Actor.exit();
    }
}

// ------------------------------------------------------------------
// 4️⃣  SEARCH-RESULT PAGE HANDLER
// ------------------------------------------------------------------
async function handleSearchPage($, enqueueLinks, request, input) {
    const { url, userData } = request;
    const currentPage = userData?.page ?? 1;

    log.info(`🔍 Scraping search page ${currentPage}: ${url}`);

    const jobLinks = [];
    $('a[href*="/desc/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (href && href.includes('/desc/')) {
            const fullUrl = href.startsWith('http') ? href : `https://jooble.org${href}`;
            if (!jobLinks.includes(fullUrl)) jobLinks.push(fullUrl);
        }
    });

    log.info(`   Found ${jobLinks.length} job links on page ${currentPage}`);

    if (jobLinks.length) {
        await enqueueLinks({
            urls: jobLinks,
            userData: { label: 'DETAIL', searchPage: currentPage },
        });
    }

    if (currentPage < input.maxPages && jobLinks.length > 0) {
        const nextPageUrl = buildSearchUrl({
            searchQuery: input.searchQuery,
            location: input.location,
            page: currentPage + 1,
            jobAge: input.jobAge,
        });
        log.info(`   ➡️ Enqueuing next page: ${currentPage + 1}`);
        await enqueueLinks({
            urls: [nextPageUrl],
            userData: { label: 'SEARCH', page: currentPage + 1 },
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

    const descriptionHtml =
        $('.job-description, .description, .vacancy-description, .content, main')
            .first()
            .html() || '';
    const descriptionText =
        $('.job-description, .description, .vacancy-description, .content, main')
            .first()
            .text()
            .replace(/\s+/g, ' ')
            .trim() || '';

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
        console.error(e.stack);
        process.exit(1);
    });
}
