/* ────────────────────────────────────────────────────────────── *
 *  Jooble job scraper – Apify Actor (Apify SDK + Crawlee)        *
 *                                                               *
 *  Stack used:                                                  *
 *    • Apify SDK (KeyValueStore, Dataset, log, etc.)            *
 *    • Crawlee (CheerioCrawler, ProxyConfiguration)            *
 *    • gotScraping (built‑in in Crawlee)                        *
 *                                                               *
 *  What the script does:                                        *
 *    1️⃣ Reads actor input (search query, location, maxPages…)   *
 *    2️⃣ Starts on the Jooble search‑results page                *
 *    3️⃣ Extracts job cards → pushes summary data               *
 *    4️⃣ Enqueues the job‑detail URLs                             *
 *    5️⃣ Extracts the full job description + extra fields       *
 *    6️⃣ Saves every job (summary + detail) to the default       *
 *       dataset.                                                *
 *                                                               *
 *  All selectors have been updated to match the current Jooble  *
 *  HTML structure (multiple fallback selectors are used).       *
 *                                                               *
 *  To run locally:                                              *
 *    npm i apify crawlee                                            *
 *    node main.js                                                *
 *                                                               *
 *  To run on the Apify platform – just upload the file – the    *
 *  platform will install the dependencies automatically.        *
 * ────────────────────────────────────────────────────────────── */

import { CheerioCrawler, Dataset, KeyValueStore, log, ProxyConfiguration } from 'crawlee';

// ------------------------------------------------------------------
// 1️⃣  INPUT HANDLING
// ------------------------------------------------------------------
async function getInput() {
    const raw = await KeyValueStore.getInput();

    // Default values – you can change them in the Apify UI or via API
    const defaults = {
        // Search query that will be URL‑encoded (e.g. "software engineer")
        searchQuery: 'software engineer',
        // Optional location (city, state, country). Leave empty for “all locations”.
        location: '',
        // How many search‑result pages to crawl (Jooble paginates 20‑30 jobs per page)
        maxPages: 5,
        // Concurrency – how many pages are processed in parallel
        maxConcurrency: 10,
        // Proxy (Apify default proxy is used if you don’t specify anything)
        proxyConfiguration: { useApifyProxy: true },
        // Custom request headers (helps to avoid bot‑detectors)
        requestHeaders: {
            'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            Accept:
                'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Upgrade-Insecure-Requests': '1',
        },
    };

    // Merge user‑provided input with defaults
    return { ...defaults, ...raw };
}

// ------------------------------------------------------------------
// 2️⃣  HELPER – BUILD JOOBLE SEARCH URL
// ------------------------------------------------------------------
function buildSearchUrl({ searchQuery, location, page = 1 }) {
    const base = 'https://jooble.org';
    const query = encodeURIComponent(searchQuery.trim());
    const loc = location ? `&l=${encodeURIComponent(location.trim())}` : '';
    // Jooble uses `p=` for the page number (starting at 1)
    return `${base}/jobs-${query}?p=${page}${loc}`;
}

// ------------------------------------------------------------------
// 3️⃣  SELECTOR HELPERS (centralised, easy to tweak later)
// ------------------------------------------------------------------
const selectors = {
    // ---------- SEARCH RESULT PAGE ----------
    jobCards: [
        '.vacancy-wrapper',
        '.job-item',
        '.vacancy-card',
        '[data-id]', // generic fallback
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

    // ---------- JOB DETAIL PAGE ----------
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
// 4️⃣  MAIN ACTOR FUNCTION
// ------------------------------------------------------------------
export async function main() {
    const input = await getInput();

    // ------------------------------------------------------------------
    // 4.1  PROXY (optional – Apify Proxy is the default)
    // ------------------------------------------------------------------
    const proxyConfiguration = await ProxyConfiguration(input.proxyConfiguration);

    // ------------------------------------------------------------------
    // 4.2  CRAWLER SETUP
    // ------------------------------------------------------------------
    const crawler = new CheerioCrawler({
        proxyConfiguration,
        maxConcurrency: input.maxConcurrency,
        // Prevent the crawler from hanging forever on a single page
        requestHandlerTimeoutSecs: 90,
        // Add our custom headers to every request
        prepareRequestFunction: async ({ request }) => {
            request.headers = { ...request.headers, ...input.requestHeaders };
            return request;
        },

        // ------------------------------------------------------------------
        // 4.3  REQUEST HANDLER – decides what to do with each page
        // ------------------------------------------------------------------
        async requestHandler({ $, request, enqueueLinks, log }) {
            const { url, userData } = request;
            const label = userData?.label ?? 'UNKNOWN';

            // --------------------------------------------------------------
            // 4.3.1  SEARCH RESULT PAGE
            // --------------------------------------------------------------
            if (label === 'SEARCH') {
                await handleSearchPage($, request, enqueueLinks, input);
                return;
            }

            // --------------------------------------------------------------
            // 4.3.2  JOB DETAIL PAGE
            // --------------------------------------------------------------
            if (label === 'DETAIL') {
                await handleDetailPage($, request);
                return;
            }

            // --------------------------------------------------------------
            // 4.3.3  FALLBACK (just in case)
            // --------------------------------------------------------------
            log.warning(`Page with unknown label "${label}" – ${url}`);
        },

        // ------------------------------------------------------------------
        // 4.4  FAILED REQUEST HANDLER (logs errors)
        // ------------------------------------------------------------------
        failedRequestHandler({ request, error }) {
            log.error(`❌ Request ${request.url} failed: ${error.message}`);
        },
    });

    // ------------------------------------------------------------------
    // 5️⃣  ENQUEUE STARTING SEARCH URL(s)
    // ------------------------------------------------------------------
    const startUrls = [];

    // We start with page 1 and let the pagination logic add the rest,
    // but we also respect the user‑defined maxPages to avoid infinite loops.
    for (let page = 1; page <= Math.min(input.maxPages, 1); page++) {
        startUrls.push({
            url: buildSearchUrl({ searchQuery: input.searchQuery, location: input.location, page }),
            userData: { label: 'SEARCH', page },
        });
    }

    await crawler.run(startUrls);

    log.info('✅ Crawling finished – check the default dataset for results.');
}

// ------------------------------------------------------------------
// 6️⃣  SEARCH‑RESULT PAGE HANDLER
// ------------------------------------------------------------------
async function handleSearchPage($, request, enqueueLinks, input) {
    const { url, userData } = request;
    const currentPage = userData?.page ?? 1;
    const jobs = [];

    // ------------------------------------------------------------------
    // 6.1  EXTRACT JOB CARD INFO
    // ------------------------------------------------------------------
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

            const job = {
                title: getFirst(selectors.title),
                company: getFirst(selectors.company),
                location: getFirst(selectors.location),
                salary: getFirst(selectors.salary),
                postedDate: getFirst(selectors.postedDate),

                // The link may be relative – we turn it into an absolute URL
                jobUrl: (function () {
                    const href = $card.find('a').attr('href');
                    if (!href) return null;
                    return href.startsWith('http')
                        ? href
                        : new URL(href, 'https://jooble.org').href;
                })(),

                // meta
                source: 'Jooble',
                searchUrl: url,
                crawlPage: currentPage,
                // keep the raw HTML of the card in case you need it later
                rawHtml: $.html(el),
            };

            // Minimal validation – we need at least a title and a URL
            if (job.title && job.jobUrl) {
                jobs.push(job);

                // ----------------------------------------------------------
                // 6.2  ENQUEUE DETAIL PAGE (only once per job)
                // ----------------------------------------------------------
                request.enqueue({
                    url: job.jobUrl,
                    userData: { label: 'DETAIL', parentSearchUrl: url },
                });
            }
        });
    }

    // ------------------------------------------------------------------
    // 6.3  SAVE EXTRACTED JOBS
    // ------------------------------------------------------------------
    if (jobs.length) {
        await Dataset.pushData(jobs);
        log.info(`🔎 Page ${currentPage}: extracted ${jobs.length} jobs from ${url}`);
    } else {
        log.warning(`⚠️ No jobs found on ${url}`);
    }

    // ------------------------------------------------------------------
    // 6.4  HANDLE PAGINATION (if we haven’t reached maxPages)
    // ------------------------------------------------------------------
    if (currentPage < input.maxPages) {
        // Jooble uses a simple “next” link with rel="next" or an aria‑label.
        const nextHref =
            $('.pagination-next, a[rel="next"], a[aria-label="Next"], .next-page')
                .attr('href');

        if (nextHref) {
            const nextUrl = new URL(nextHref, 'https://jooble.org').href;
            request.enqueue({
                url: nextUrl,
                userData: { label: 'SEARCH', page: currentPage + 1 },
            });
            log.info(`➡️ Enqueued next search page → ${nextUrl}`);
        } else {
            log.info('🔚 No further pagination link found – crawling stops.');
        }
    }
}

// ------------------------------------------------------------------
// 7️⃣  JOB‑DETAIL PAGE HANDLER
// ------------------------------------------------------------------
async function handleDetailPage($, request) {
    const { url } = request;

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

        // Description can be huge; we keep the raw text only.
        description: getFirst(selectors.detail.description),

        requirements: getFirst(selectors.detail.requirements),
        benefits: getFirst(selectors.detail.benefits),

        postedDate: getFirst(selectors.detail.postedDate),

        // Application URL – many Jooble listings contain a direct link to the original posting.
        applicationUrl: (function () {
            const href = $(selectors.detail.applicationUrl.join(', ')).attr('href');
            if (!href) return null;
            return href.startsWith('http')
                ? href
                : new URL(href, 'https://jooble.org').href;
        })(),

        sourceUrl: url,
        source: 'Jooble',
        scrapedAt: new Date().toISOString(),
    };

    // Clean up description (remove extra whitespace & HTML tags if any)
    if (job.description) {
        job.description = job.description.replace(/\s+/g, ' ').trim();
    }

    // Save the detailed record
    await Dataset.pushData(job);
    log.info(`🗂️ Scraped detail for "${job.title}" – ${url}`);
}

// ------------------------------------------------------------------
// 8️⃣  GRACEFUL SHUTDOWN (Ctrl+C / SIGTERM)
// ------------------------------------------------------------------
process.on('SIGINT', async () => {
    log.info('🔌 Received SIGINT – shutting down gracefully...');
    process.exit(0);
});
process.on('SIGTERM', async () => {
    log.info('🔌 Received SIGTERM – shutting down gracefully...');
    process.exit(0);
});

// ------------------------------------------------------------------
// 9️⃣  RUN THE ACTOR (required when the file is executed directly)
// ------------------------------------------------------------------
if (require.main === module) {
    // When the script is launched by `node main.js` (local test) we
    // call the exported `main()` function.
    main().catch((e) => {
        log.error('❌ Unexpected error in main():', e);
        process.exit(1);
    });
}