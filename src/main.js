/* ────────────────────────────────────────────────────────────── *
 *  Jooble job scraper – Apify Actor (ESM, Crawlee, resilient)    *
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
        jobAge: 'all',
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
    input.maxPages = Number.isFinite(+input.maxPages) && +input.maxPages > 0 ? +input.maxPages : 1;
    input.maxConcurrency = Number.isFinite(+input.maxConcurrency) && +input.maxConcurrency > 0 ? +input.maxConcurrency : 5;
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
// 3️⃣  USER-AGENT POOL
// ------------------------------------------------------------------
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
];

// ------------------------------------------------------------------
// 4️⃣  MAIN ACTOR FUNCTION
// ------------------------------------------------------------------
export async function main() {
    await Actor.init();

    try {
        const input = await getInput();
        log.info(`🎬 Starting Jooble scraper | query="${input.searchQuery}" | pages=${input.maxPages}`);

        const proxyConfiguration = await Actor.createProxyConfiguration(input.proxyConfiguration);

        const crawler = new CheerioCrawler({
            proxyConfiguration,
            maxConcurrency: input.maxConcurrency,
            requestHandlerTimeoutSecs: 90,
            useSessionPool: true,
            persistCookiesPerSession: true,

            // Supported option for global headers
            additionalHttpHeaders: { ...input.requestHeaders },

            async requestHandler(context) {
                const { $, request, enqueueLinks, session, proxyInfo } = context;
                const label = request.userData?.label ?? 'SEARCH';
                const page = request.userData?.page ?? 1;

                // 🧱 Cookie / Bot wall detection
                const htmlText = $.root().text().toLowerCase();
                if (/are you human|verify you are human|captcha|cookie consent/i.test(htmlText)) {
                    const retries = request.userData?.retries ?? 0;
                    log.warning(`🚧 Detected cookie/bot wall on ${request.url} (retry ${retries})`);
                    if (retries < 3) {
                        // Rotate UA + new session
                        const newUA = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
                        request.headers['User-Agent'] = newUA;
                        if (session) {
                            session.retire();
                            log.info(`🔄 Retired session due to block. New session will be created.`);
                        }
                        // Wait 2–5 s before retry
                        await Actor.sleep(2000 + Math.random() * 3000);
                        await enqueueLinks({
                            urls: [request.url],
                            transformRequestFunction: (req) => {
                                req.userData = { ...request.userData, retries: retries + 1 };
                                return req;
                            },
                        });
                    } else {
                        log.error(`❌ Giving up on ${request.url} after ${retries} retries.`);
                    }
                    return;
                }

                if (label === 'SEARCH') {
                    await handleSearchPage(context, input);
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
        if (err?.stack) console.error(err.stack);
    } finally {
        await Actor.exit();
    }
}

// ------------------------------------------------------------------
// 5️⃣  SEARCH-RESULT PAGE HANDLER
// ------------------------------------------------------------------
async function handleSearchPage({ $, enqueueLinks, request }, input) {
    const currentPage = request.userData?.page ?? 1;
    log.info(`🔍 Scraping search page ${currentPage}: ${request.url}`);

    const jobLinks = [];
    $('a[href*="/desc/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href || !href.includes('/desc/')) return;
        const fullUrl = href.startsWith('http') ? href : `https://jooble.org${href}`;
        if (!jobLinks.includes(fullUrl)) jobLinks.push(fullUrl);
    });

    log.info(`   Found ${jobLinks.length} job links on page ${currentPage}`);

    if (jobLinks.length) {
        await enqueueLinks({
            urls: jobLinks,
            transformRequestFunction: (req) => {
                req.userData = { label: 'DETAIL', searchPage: currentPage };
                return req;
            },
        });
    }

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
// 6️⃣  JOB-DETAIL PAGE HANDLER
// ------------------------------------------------------------------
async function handleDetailPage($, request) {
    log.info(`📄 Scraping job detail: ${request.url}`);

    const getFirst = (sels) => {
        for (const sel of sels) {
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
    const description_html = descNode.html() || '';
    const description_text = descNode.text().replace(/\s+/g, ' ').trim() || '';

    const job = {
        title,
        company,
        location,
        salary,
        description_html,
        description_text,
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
// 7️⃣  LOCAL TEST ENTRY
// ------------------------------------------------------------------
if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch((e) => {
        log.error('❌ Unexpected error in main():', e);
        if (e?.stack) console.error(e.stack);
        process.exit(1);
    });
}
