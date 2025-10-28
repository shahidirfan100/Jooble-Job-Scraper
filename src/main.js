/* ────────────────────────────────────────────────────────────── *
 *  Jooble job scraper – Apify Actor (Apify SDK + Crawlee)        *
 *  – works with `"type": "module"` (ESM)                         *
 * ────────────────────────────────────────────────────────────── */

import { CheerioCrawler, Dataset, log } from 'crawlee';
import { Actor } from 'apify';

// ------------------------------------------------------------------
// 1️⃣  INPUT HANDLING
// ------------------------------------------------------------------
async function getInput() {
    const raw = await Actor.getInput();

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
    if (searchQuery && searchQuery.trim()) {
        params.set('ukw', searchQuery.trim());
    }
    if (location && location.trim()) {
        params.set('l', location.trim());
    }
    if (page > 1) {
        params.set('p', String(page));
    }
    // Add job age filter: 1 = 24 hours, 7 = 7 days, 30 = 30 days
    if (jobAge && jobAge !== 'all') {
        params.set('date', jobAge);
    }
    return params.toString() ? `${base}?${params.toString()}` : base;
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
    // Initialize Apify Actor (required for platform)
    await Actor.init();
    
    const input = await getInput();

    // Create proxy configuration using Apify SDK
    const proxyConfiguration = input.proxyConfiguration 
        ? await Actor.createProxyConfiguration(input.proxyConfiguration)
        : undefined;

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
        async requestHandler({ $, request, enqueueLinks, log: crawlerLog }) {
            const { url, userData } = request;
            const label = userData?.label ?? 'UNKNOWN';

            // Create a wrapper that uses enqueueLinks from crawler context
            const requestWithEnqueue = {
                ...request,
                enqueueLinks: async (options) => {
                    await enqueueLinks(options);
                }
            };

            if (label === 'SEARCH') {
                await handleSearchPage($, requestWithEnqueue, input);
                return;
            }

            if (label === 'DETAIL') {
                await handleDetailPage($, request);
                return;
            }

            crawlerLog.warning(`Page with unknown label "${label}" – ${url}`);
        },

        // --------------------------------------------------------------
        // 4.2 FAILED REQUEST HANDLER
        // --------------------------------------------------------------
        failedRequestHandler({ request, error }) {
            log.error(`❌ Request ${request.url} failed: ${error.message}`);
        },
    });

    // ------------------------------------------------------------------
    // 5️⃣  ENQUEUE STARTING URL (page 1)
    // ------------------------------------------------------------------
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
    
    // Exit Apify Actor
    await Actor.exit();
}

// ------------------------------------------------------------------
// 6️⃣  SEARCH‑RESULT PAGE HANDLER
// ------------------------------------------------------------------
async function handleSearchPage($, request, input) {
    const { url, userData } = request;
    const currentPage = userData?.page ?? 1;
    const jobs = [];

    log.info(`🔍 Scraping search page ${currentPage}: ${url}`);

    // Jooble uses links with /desc/ pattern for job details
    // Extract all job detail links from the page
    const jobLinks = [];
    $('a[href*="/desc/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (href && href.includes('/desc/')) {
            const fullUrl = href.startsWith('http') ? href : `https://jooble.org${href}`;
            if (!jobLinks.includes(fullUrl)) {
                jobLinks.push(fullUrl);
            }
        }
    });

    log.info(`   Found ${jobLinks.length} job links on page ${currentPage}`);

    // Enqueue each job detail page
    for (const jobUrl of jobLinks) {
        await request.enqueueLinks({
            urls: [jobUrl],
            userData: { label: 'DETAIL', searchPage: currentPage },
        });
    }

    // ---------- PAGINATION ----------
    const shouldContinue = currentPage < input.maxPages;
    if (shouldContinue && jobLinks.length > 0) {
        const nextPageUrl = buildSearchUrl({
            searchQuery: input.searchQuery,
            location: input.location,
            page: currentPage + 1,
            jobAge: input.jobAge,
        });
        
        log.info(`   ➡️  Enqueuing next page: ${currentPage + 1}`);
        
        await request.enqueueLinks({
            urls: [nextPageUrl],
            userData: { label: 'SEARCH', page: currentPage + 1 },
        });
    } else {
        log.info(`   ⏹️  Stopping pagination at page ${currentPage}`);
    }
}

// ------------------------------------------------------------------
// 7️⃣  JOB‑DETAIL PAGE HANDLER
// ------------------------------------------------------------------
async function handleDetailPage($, request) {
    log.info(`📄 Scraping job detail: ${request.url}`);
    
    // Helper to try multiple selectors
    const getFirst = (selectors) => {
        for (const sel of selectors) {
            const txt = $(sel).first().text().trim();
            if (txt) return txt;
        }
        return '';
    };

    // Extract job data - Jooble shows basic info on listing pages
    // Detail pages may have more info but structure varies
    const title = getFirst(['h1', '.job-title', '.title']) || '';
    const company = getFirst(['.company', '.employer', '.company-name']) || '';
    const location = getFirst(['.location', '.job-location']) || '';
    const salary = getFirst(['.salary', '.pay', '.compensation']) || '';
    
    // Description - usually in a main content area
    const descriptionHtml = $('.job-description, .description, .vacancy-description, .content, main')
        .first()
        .html() || '';
    
    const descriptionText = $('.job-description, .description, .vacancy-description, .content, main')
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
        date_posted: getFirst(['.date', '.posted', '.time']) || null,
        job_type: null,
        job_category: null,
        scrapedAt: new Date().toISOString(),
        source: 'Jooble',
    };

    // Only save if we have at least a title
    if (job.title) {
        await Dataset.pushData(job);
        log.info(`   ✅ Saved: "${job.title}" at ${job.company || 'Unknown Company'}`);
    } else {
        log.warning(`   ⚠️  No title found for ${request.url}`);
    }
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