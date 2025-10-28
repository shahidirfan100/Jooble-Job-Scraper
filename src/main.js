import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

const BASE_URL = 'https://jooble.org';

// Enhanced user agents that look more legitimate
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
];

const getRandomUA = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

// Extract job links from HTML using Cheerio
function extractJobLinks(baseUrl, $) {
    const links = new Set();
    $('a[href*="/desc/"], a[data-qa="vacancy-serp__vacancy-title"], a[class*="job-link"]').each((_, el) => {
        const href = $(el).attr('href');
        if (href) {
            const abs = href.startsWith('http') ? href : new URL(href, baseUrl).href;
            if (abs.includes('/desc/')) links.add(abs);
        }
    });
    return [...links];
}

// Extract job data from Cheerio object
function extractJobData($, url) {
    const job = {
        title: '',
        company: '',
        location: '',
        salary: '',
        description: '',
        job_url: url,
        scrapedAt: new Date().toISOString(),
    };

    // Try multiple selectors for each field
    const selectors = {
        title: ['h1', '.job-title', '.vacancy-title', '[data-qa="vacancy-title"]'],
        company: ['.company', '.employer', '.company-name', '[data-qa="vacancy-company-name"]'],
        location: ['.location', '.job-location', '[data-qa="vacancy-view-location"]'],
        salary: ['.salary', '.compensation', '[data-qa="vacancy-salary"]'],
        description: ['.job-description', '.vacancy-description', '.description', '[data-qa="vacancy-description"]', 'main', 'article'],
    };

    for (const selector of selectors.title) {
        if (!job.title) {
            job.title = $(selector).first().text().trim();
        }
    }

    for (const selector of selectors.company) {
        if (!job.company) {
            job.company = $(selector).first().text().trim();
        }
    }

    for (const selector of selectors.location) {
        if (!job.location) {
            job.location = $(selector).first().text().trim();
        }
    }

    for (const selector of selectors.salary) {
        if (!job.salary) {
            job.salary = $(selector).first().text().trim();
        }
    }

    for (const selector of selectors.description) {
        if (!job.description) {
            job.description = $(selector).first().text().trim().substring(0, 5000);
        }
    }

    return job;
}

// ---------- Main Execution ----------

await Actor.init();

const input = (await Actor.getInput()) || {};
const {
    searchQuery = 'developer',
    maxPages = 3,
    maxItems = 20,
} = input;

log.info(`🎬 Jooble scraper started | query="${searchQuery}" maxPages=${maxPages} maxItems=${maxItems}`);

// Create proxy configuration with residential proxies for better success
let proxyConfiguration;
try {
    proxyConfiguration = await Actor.createProxyConfiguration({
        useApifyProxy: true,
        apifyProxyGroups: ['RESIDENTIAL'],
        countryCode: 'US',
    });
    log.info('✅ Residential proxy configured');
} catch (e) {
    log.warning('⚠️ Residential proxy not available, trying datacenter');
    try {
        proxyConfiguration = await Actor.createProxyConfiguration({
            useApifyProxy: true,
        });
        log.info('✅ Datacenter proxy configured');
    } catch (e2) {
        log.warning('⚠️ No proxy available, continuing without');
        proxyConfiguration = undefined;
    }
}

let saved = 0;

const crawler = new CheerioCrawler({
    proxyConfiguration,
    maxConcurrency: 1, // Very low concurrency to avoid detection
    maxRequestRetries: 5, // More retries
    requestHandlerTimeoutSecs: 60,
    useSessionPool: true,
    sessionPoolOptions: {
        maxPoolSize: 20,
        sessionOptions: {
            maxUsageCount: 2, // Rotate sessions more frequently
            maxErrorScore: 5,
        },
    },

    // Enhanced request options for stealth
    requestOptions: {
        headers: {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'DNT': '1',
        },
    },

    async requestHandler({ $, request, crawler }) {
        const label = request.userData?.label || 'SEARCH';
        const pageNum = request.userData?.page || 1;

        // Check for blocks
        const bodyText = $('body').text();
        const title = $('title').text() || '';

        if (/captcha|verify|blocked|access denied|403|robot|bot/i.test(bodyText) ||
            /captcha|verify|blocked|403/i.test(title)) {
            log.warning(`🚧 Detected block on ${request.url} - Title: ${title.substring(0, 50)}`);
            throw new Error('Page blocked or captcha detected');
        }

        if (label === 'SEARCH') {
            const jobLinks = extractJobLinks(request.url, $);
            log.info(`🔎 Page ${pageNum}: found ${jobLinks.length} job links`);

            if (jobLinks.length === 0) {
                log.warning(`No job links found on page ${pageNum}. HTML length: ${$.html().length}`);
                return;
            }

            // Queue job detail pages
            const remaining = maxItems - saved;
            const linksToProcess = jobLinks.slice(0, Math.max(0, remaining));

            for (const jobUrl of linksToProcess) {
                await crawler.addRequests([{
                    url: jobUrl,
                    userData: { label: 'DETAIL' },
                    headers: {
                        'User-Agent': getRandomUA(),
                        'Referer': request.url,
                    },
                }]);
            }

            log.info(`📋 Queued ${linksToProcess.length} job detail pages`);

            // Queue next search page
            if (pageNum < maxPages && saved < maxItems && jobLinks.length > 0) {
                const nextPageNum = pageNum + 1;
                const nextUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}&p=${nextPageNum}`;

                await crawler.addRequests([{
                    url: nextUrl,
                    userData: { label: 'SEARCH', page: nextPageNum },
                    headers: {
                        'User-Agent': getRandomUA(),
                        'Referer': request.url,
                    },
                }]);

                log.info(`📄 Queued page ${nextPageNum}`);
            }
        } else if (label === 'DETAIL') {
            const jobData = extractJobData($, request.url);

            if (jobData.title) {
                await Dataset.pushData(jobData);
                saved++;
                log.info(`✅ [${saved}/${maxItems}] ${jobData.title.substring(0, 50)}`);

                // Stop if reached max items
                if (saved >= maxItems) {
                    log.info(`🎯 Reached maxItems (${maxItems}), stopping crawler`);
                    await crawler.autoscaledPool?.abort();
                }
            } else {
                log.warning(`⚠️ No title found on ${request.url}`);
            }
        }
    },

    failedRequestHandler({ request, error }) {
        log.error(`❌ Failed ${request.url}: ${error.message}`);
    },
});

const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);

log.info(`🎉 Done! Total jobs scraped: ${saved}`);
await Actor.exit();