/* ────────────────────────────────────────────────────────────── *
 *  Jooble job scraper – Apify Actor (Apify SDK + Crawlee)        *
 *  – works with `"type": "module"` (ESM)                         *
 * ────────────────────────────────────────────────────────────── */

import { CheerioCrawler, Dataset, KeyValueStore, log, ProxyConfiguration } from 'crawlee';

// ------------------------------------------------------------------
// 1️⃣  INPUT HANDLING
// ------------------------------------------------------------------
async function getInput() {
    const raw = await KeyValueStore.getInput();

    const defaults = {
        searchQuery: 'software engineer',
        location: '',
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
function buildSearchUrl({ searchQuery, location, page = 1 }) {
    const base = 'https://jooble.org';
    const query = encodeURIComponent(searchQuery.trim());
    const loc = location ? `&l=${encodeURIComponent(location.trim())}` : '';
    return `${base}/jobs-${query}?p=${page}${loc}`;
}

// ------------------------------------------------------------------
// 3️⃣  SELECTOR MAP (multiple fall‑backs)
// ------------------------------------------------------------------
const selectors = {
    // ---- SEARCH RESULTS ----
    jobCards: [
        '.vacancy-wrapper',
        '.job-item',
        '.vacancy-card',
        '[data-id]',
    ],
    title: [
        '.job-title',
        '.vacancy-title',
        '.title',
        'h2',
        '[data-qa="job-title"]',
        'a[data-job-title]',
    ],
    company: [
        '.company',
        '.employer',
        '.vacancy-company',
        '[data-qa="company-name"]',
        '.company-name',
    ],
    location: [
        '.location',
        '.vacancy-location',
        '[data-qa="job-location"]',
        '.job-location',
    ],
    salary: [
        '.salary',
        '.vacancy-salary',
        '.compensation',
        '[data-qa="salary"]',
    ],
    postedDate: [
        '.date',
        '.posted',
        '.timeago',
        '[data-qa="posting-date"]',
        '.posting-date',
    ],

    // ---- DETAIL PAGE ----
    detail: {
        title: [
            '.job-title',
            '.vacancy-title',
            'h1',
            '[data-qa="job-title"]',
        ],
        company: [
            '.company-name',
            '.employer',
            '[data-qa="company-name"]',
        ],
        location: [
            '.job-location',
            '.location',
            '[data-qa="job-location"]',
        ],
        salary: [
            '.salary',
            '.compensation',
            '[data-qa="salary"]',
        ],
        jobType: [
            '.job-type',
            '.employment-type',
            '[data-qa="employment-type"]',
        ],
        experience: [
            '.experience',
            '.seniority',
            '[data-qa="experience-level"]',
        ],
        description: [
            '.job-description',
            '.vacancy-description',
            '[data-qa="job-description"]',
            '.description',
            '.content',
        ],
        requirements: [
            '.requirements',
            '.qualifications',
            '[data-qa="requirements"]',
        ],
        benefits: [
            '.benefits',
            '.perks',
            '[data-qa="benefits"]',
        ],
        postedDate: [
            '.posting-date',
            '.date',
            '[data-qa="posting-date"]',
        ],
        applicationUrl: [
            '.apply-button',
            '.apply-link',
            '[data-qa="apply-link"]',
            'a[data-apply]',
        ],
    },
};

// ------------------------------------------------------------------
// 4️⃣  MAIN ACTOR FUNCTION (exported for Apify)
// ------------------------------------------------------------------
export async function main() {
    const input = await getInput();

    const proxyConfiguration = await ProxyConfiguration(input.proxyConfiguration);

    const crawler = new CheerioCrawler({
        proxyConfiguration,
        maxConcurrency: input.maxConcurrency,
        requestHandlerTimeoutSecs: 90,

        // Attach custom headers to every request
        prepareRequestFunction: async ({ request }) => {
            request.headers = { ...request.headers, ...input.requestHeaders };
            return request;
        },

        // --------------------------------------------------------------
        // 4.1 REQUEST HANDLER – decides what to do with each page
        // --------------------------------------------------------------
        async requestHandler({ $, request, log }) {
            const { url, userData } = request;
            const label = userData?.label ?? 'UNKNOWN';

            if (label === 'SEARCH') {
                await handleSearchPage($, request, input);
                return;
            }

            if (label === 'DETAIL') {
                await handleDetailPage($, request);
                return;
            }

            log.warning(`Page with unknown label "${label}" – ${url}`);
        },

        // --------------------------------------------------------------
        // 4.2 FAILED REQUEST HANDLER
        // --------------------------------------------------------------
        failedRequestHandler({ request, error }) {
            log.error(`❌ Request ${request.url} failed: ${error.message}`);
        },
    });

    // ------------------------------------------------------------------
    // 5️⃣  ENQUEUE STARTING URL (page 1)
    // ------------------------------------------------------------------
    await crawler.run([
        {
            url: buildSearchUrl({
                searchQuery: input.searchQuery,
                location: input.location,
                page: 1,
            }),
            userData: { label: 'SEARCH', page: 1 },
        },
    ]);

    log.info('✅ Crawling finished – check the default dataset for results.');
}

// ------------------------------------------------------------------
// 6️⃣  SEARCH‑RESULT PAGE HANDLER
// ------------------------------------------------------------------
async function handleSearchPage($, request, input) {
    const { url, userData } = request;
    const currentPage = userData?.page ?? 1;
    const jobs = [];

    // ---------- EXTRACT EACH CARD ----------
    for (const cardSel of selectors.jobCards) {
        $(cardSel).each((_, el) => {
            const $card = $(el);

            const getFirst = (list) => {
                for (const sel of list) {
                    const txt = $card.find(sel).text().trim();
                    if (txt) return txt;
                }
                return '';
            };

            const href = $card.find('a').attr('href');
            const jobUrl = href
                ? href.startsWith('http')
                    ? href
                    : new URL(href, 'https://jooble.org').href
                : null;

            const job = {
                title: getFirst(selectors.title),
                company: getFirst(selectors.company),
                location: getFirst(selectors.location),
                salary: getFirst(selectors.salary),
                postedDate: getFirst(selectors.postedDate),

                jobUrl,
                source: 'Jooble',
                searchUrl: url,
                crawlPage: currentPage,
                rawHtml: $.html(el),
            };

            // Keep only records that have a title + URL
            if (job.title && job.jobUrl) {
                jobs.push(job);

                // Enqueue the detail page (only once per job)
                request.enqueue({
                    url: job.jobUrl,
                    userData: { label: 'DETAIL', parentSearchUrl: url },
                });
            }
        });
    }

    // ---------- SAVE EXTRACTED JOBS ----------
    if (jobs.length) {
        await Dataset.pushData(jobs);
        log.info(`🔎 Page ${currentPage}: extracted ${jobs.length} jobs`);
    } else {
        log.warning(`⚠️ No jobs found on ${url}`);
    }

    // ---------- HANDLE PAGINATION ----------
    if (currentPage < input.maxPages) {
        const nextHref = $(
            '.pagination-next, a[rel="next"], a[aria-label="Next"], .next-page'
        )
            .attr('href');

        if (nextHref) {
            const nextUrl = new URL(nextHref, 'https://jooble.org').href;
            request.enqueue({
                url: nextUrl,
                userData: { label: 'SEARCH', page: currentPage + 1 },
            });
            log.info(`➡️ Enqueued next page → ${nextUrl}`);
        } else {
            log.info('🔚 No further pagination link found – crawling stops.');
        }
    }
}

// ------------------------------------------------------------------
// 7️⃣  JOB‑DETAIL PAGE HANDLER
// ------------------------------------------------------------------
async function handleDetailPage($, request) {
    const getFirst = (list) => {
        for (const sel of list) {
            const txt = $(sel).text().trim();
            if (txt) return txt;
        }
        return '';
    };

    const job = {
        title: getFirst(selectors.detail.title),
        company: getFirst(selectors.detail.company),
        location: getFirst(selectors.detail.location),
        salary: getFirst(selectors.detail.salary),
        jobType: getFirst(selectors.detail.jobType),
        experience: getFirst(selectors.detail.experience),

        description: getFirst(selectors.detail.description),

        requirements: getFirst(selectors.detail.requirements),
        benefits: getFirst(selectors.detail.benefits),

        postedDate: getFirst(selectors.detail.postedDate),

        applicationUrl: (function () {
            const href = $(selectors.detail.applicationUrl.join(', ')).attr('href');
            if (!href) return null;
            return href.startsWith('http')
                ? href
                : new URL(href, 'https://jooble.org').href;
        })(),

        sourceUrl: request.url,
        source: 'Jooble',
        scrapedAt: new Date().toISOString(),
    };

    // Clean up description (remove extra whitespace)
    if (job.description) {
        job.description = job.description.replace(/\s+/g, ' ').trim();
    }

    await Dataset.pushData(job);
    log.info(`🗂️ Scraped detail for "${job.title}"`);
}

// ------------------------------------------------------------------
// 8️⃣  GRACEFUL SHUTDOWN (optional but nice)
// ------------------------------------------------------------------
process.on('SIGINT', () => {
    log.info('🔌 Received SIGINT – exiting...');
    process.exit(0);
});
process.on('SIGTERM', () => {
    log.info('🔌 Received SIGTERM – exiting...');
    process.exit(0);
});

/* -----------------------------------------------------------------
   9️⃣  LOCAL TESTING ENTRY‑POINT (ESM‑compatible)
   ----------------------------------------------------------------- */
if (import.meta.url === `file://${process.argv[1]}`) {
    // When you run `node main.js` locally the script will start here.
    // On the Apify platform the platform itself calls the exported `main()`.
    main().catch((e) => {
        log.error('❌ Unexpected error in main():', e);
        process.exit(1);
    });
}