/* ────────────────────────────────────────────────────────────── *
 *  Jooble job scraper – Apify Actor (ESM, Crawlee safe version)  *
 *  Compatible with Apify platform images and Crawlee 3.x+        *
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
        userAgents: [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1'
        ],
    };

    const input = { ...defaults, ...(raw || {}) };
    input.maxPages = +input.maxPages > 0 ? +input.maxPages : 1;
    input.maxConcurrency = +input.maxConcurrency > 0 ? +input.maxConcurrency : 5;
    return input;
}

// ------------------------------------------------------------------
// 2️⃣  URL BUILDER
// ------------------------------------------------------------------
function buildSearchUrl({ searchQuery, location, page = 1, jobAge = 'all' }) {
    const base = 'https://jooble.org/SearchResult';
    const p = new URLSearchParams();
    if (searchQuery) p.set('ukw', searchQuery.trim());
    if (location) p.set('l', location.trim());
    if (page > 1) p.set('p', String(page));
    if (jobAge !== 'all') p.set('date', String(jobAge));
    const qs = p.toString();
    return qs ? `${base}?${qs}` : base;
}

// ------------------------------------------------------------------
// 3️⃣  MAIN ACTOR
// ------------------------------------------------------------------
export async function main() {
    await Actor.init();

    try {
        const input = await getInput();
        const proxyConfiguration = await Actor.createProxyConfiguration(input.proxyConfiguration);
        log.info(`🎬 Starting Jooble scraper | query="${input.searchQuery}"`);

        const crawler = new CheerioCrawler({
            proxyConfiguration,
            useSessionPool: true,
            persistCookiesPerSession: true,
            maxConcurrency: input.maxConcurrency,
            requestHandlerTimeoutSecs: 90,

            async requestHandler(context) {
                const { request, $, enqueueLinks, session } = context;
                const label = request.userData?.label ?? 'SEARCH';
                const page = request.userData?.page ?? 1;

                // ------------------- Cookie wall detection -------------------
                const bodyText = $.root().text().toLowerCase();
                if (/are you human|verify you are human|captcha|cookie/i.test(bodyText)) {
                    const retries = request.userData?.retries ?? 0;
                    log.warning(`🚧 Cookie/bot wall detected on ${request.url} (retry ${retries})`);
                    if (retries < 3) {
                        session.retire();
                        const newUA = input.userAgents[Math.floor(Math.random() * input.userAgents.length)];
                        request.headers['User-Agent'] = newUA;
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

                // ------------------- Normal flow -------------------
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
                headers: {
                    'User-Agent': input.userAgents[Math.floor(Math.random() * input.userAgents.length)],
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                },
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
// 4️⃣  SEARCH PAGE HANDLER
// ------------------------------------------------------------------
async function handleSearchPage({ $, enqueueLinks, request }, input) {
    const currentPage = request.userData?.page ?? 1;
    log.info(`🔍 Page ${currentPage}: ${request.url}`);

    const jobLinks = [];
    $('a[href*="/desc/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href || !href.includes('/desc/')) return;
        const full = href.startsWith('http') ? href : `https://jooble.org${href}`;
        jobLinks.push(full);
    });

    log.info(`   Found ${jobLinks.length} job links`);

    if (jobLinks.length) {
        await enqueueLinks({
            urls: jobLinks,
            transformRequestFunction: (req) => {
                req.userData = { label: 'DETAIL', searchPage: currentPage };
                req.headers = {
                    'User-Agent': input.userAgents[Math.floor(Math.random() * input.userAgents.length)],
                };
                return req;
            },
        });
    }

    if (currentPage < input.maxPages && jobLinks.length > 0) {
        const nextPage = currentPage + 1;
        const nextUrl = buildSearchUrl({
            searchQuery: input.searchQuery,
            location: input.location,
            page: nextPage,
            jobAge: input.jobAge,
        });
        log.info(`   ➡️ Enqueuing next page ${nextPage}`);
        await enqueueLinks({
            urls: [nextUrl],
            transformRequestFunction: (req) => {
                req.userData = { label: 'SEARCH', page: nextPage };
                req.headers = {
                    'User-Agent': input.userAgents[Math.floor(Math.random() * input.userAgents.length)],
                };
                return req;
            },
        });
    }
}

// ------------------------------------------------------------------
// 5️⃣  DETAIL PAGE HANDLER
// ------------------------------------------------------------------
async function handleDetailPage($, request) {
    log.info(`📄 Job detail: ${request.url}`);

    const getFirst = (sels) => {
        for (const s of sels) {
            const txt = $(s).first().text().trim();
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
        log.warning(`   ⚠️ Missing title at ${request.url}`);
    }
}

// ------------------------------------------------------------------
// 6️⃣  LOCAL RUN SUPPORT
// ------------------------------------------------------------------
if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch((e) => {
        log.error('❌ Fatal error:', e);
        console.error(e.stack);
        process.exit(1);
    });
}
