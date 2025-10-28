import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';

const BASE_URL = 'https://jooble.org';

// Realistic user agents
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
];

const getRandomUA = () => USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

// Extract job links from HTML using regex (faster and more reliable)
function extractJobLinks(html, baseUrl) {
    const links = new Set();
    const regex = /href="([^"]*\/desc\/[^"]*)"/gi;
    let match;
    
    while ((match = regex.exec(html)) !== null) {
        try {
            const href = match[1];
            const fullUrl = href.startsWith('http') ? href : new URL(href, baseUrl).href;
            if (fullUrl.includes('/desc/')) {
                links.add(fullUrl);
            }
        } catch (e) {
            // Skip invalid URLs
        }
    }
    
    return [...links];
}

// Extract job data from page
async function extractJobData(page, url) {
    const job = {
        title: '',
        company: '',
        location: '',
        salary: '',
        description: '',
        job_url: url,
        scrapedAt: new Date().toISOString(),
    };

    try {
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
                job.title = await page.locator(selector).first().textContent().catch(() => '') || '';
                job.title = job.title.trim();
            }
        }

        for (const selector of selectors.company) {
            if (!job.company) {
                job.company = await page.locator(selector).first().textContent().catch(() => '') || '';
                job.company = job.company.trim();
            }
        }

        for (const selector of selectors.location) {
            if (!job.location) {
                job.location = await page.locator(selector).first().textContent().catch(() => '') || '';
                job.location = job.location.trim();
            }
        }

        for (const selector of selectors.salary) {
            if (!job.salary) {
                job.salary = await page.locator(selector).first().textContent().catch(() => '') || '';
                job.salary = job.salary.trim();
            }
        }

        for (const selector of selectors.description) {
            if (!job.description) {
                job.description = await page.locator(selector).first().textContent().catch(() => '') || '';
                job.description = job.description.trim().substring(0, 5000);
            }
        }
    } catch (err) {
        log.warning(`⚠️ Error extracting data from ${url}: ${err.message}`);
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

// Create proxy configuration - try default first, avoid RESIDENTIAL group issues
let proxyConfiguration;
try {
    proxyConfiguration = await Actor.createProxyConfiguration({
        useApifyProxy: true,
    });
    log.info('✅ Proxy configured');
} catch (e) {
    log.warning('⚠️ No proxy available, continuing without');
    proxyConfiguration = undefined;
}

let saved = 0;

const crawler = new PlaywrightCrawler({
    proxyConfiguration,
    maxConcurrency: 1, // Lower concurrency to avoid detection
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 120,
    maxRequestRetries: 2,
    useSessionPool: true,
    sessionPoolOptions: {
        maxPoolSize: 10,
        sessionOptions: {
            maxUsageCount: 5,
            maxErrorScore: 3,
        },
    },

    launchContext: {
        launchOptions: {
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--disable-gpu',
                '--window-size=1920,1080',
                '--disable-infobars',
                '--disable-notifications',
                '--disable-popup-blocking',
            ],
        },
    },

    preNavigationHooks: [
        async ({ page, request }) => {
            const userAgent = getRandomUA();
            
            // Set user agent
            await page.setExtraHTTPHeaders({
                'User-Agent': userAgent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
            });

            // Comprehensive stealth scripts - MUST run before navigation
            await page.addInitScript(() => {
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });

                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });

                // Mock languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });

                // Add chrome object
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {},
                };

                // Mock permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );

                // Override toString
                window.navigator.chrome = {
                    runtime: {},
                };

                // Mock media devices
                Object.defineProperty(navigator, 'mediaDevices', {
                    get: () => ({
                        enumerateDevices: () => Promise.resolve([]),
                    }),
                });

                // Remove automation indicators
                delete navigator.__proto__.webdriver;
            });

            // Set viewport
            await page.setViewportSize({ width: 1920, height: 1080 });

            // Block unnecessary resources to speed up and look more natural
            await page.route('**/*', (route) => {
                const resourceType = route.request().resourceType();
                if (['image', 'stylesheet', 'font', 'media'].includes(resourceType)) {
                    route.abort();
                } else {
                    route.continue();
                }
            });

            log.debug(`🔗 Navigating to: ${request.url}`);
        },
    ],

    async requestHandler({ page, request, crawler, session }) {
        const label = request.userData?.label || 'SEARCH';
        const pageNum = request.userData?.page || 1;

        // Wait for page to be ready
        await page.waitForLoadState('domcontentloaded');
        await page.waitForTimeout(2000 + Math.random() * 2000); // Human-like delay

        // Get page content
        const html = await page.content();

        // Check for blocks
        const title = await page.title();
        if (
            /captcha|verify|blocked|403|access denied/i.test(html) ||
            /captcha|verify|blocked|403/i.test(title)
        ) {
            log.warning(`🚧 Detected block on ${request.url}`);
            session?.retire();
            throw new Error('Page blocked or captcha detected');
        }

        if (label === 'SEARCH') {
            const jobLinks = extractJobLinks(html, request.url);
            log.info(`🔎 Page ${pageNum}: found ${jobLinks.length} job links`);

            if (jobLinks.length === 0) {
                log.warning(`No job links found on page ${pageNum}. Possible blocking or empty results.`);
                return;
            }

            // Queue job detail pages
            const remaining = maxItems - saved;
            const linksToProcess = jobLinks.slice(0, Math.max(0, remaining));
            
            for (const jobUrl of linksToProcess) {
                await crawler.addRequests([{
                    url: jobUrl,
                    userData: { label: 'DETAIL' },
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
                }]);
                
                log.info(`📄 Queued page ${nextPageNum}`);
            }
        } else if (label === 'DETAIL') {
            const jobData = await extractJobData(page, request.url);

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
