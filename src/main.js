import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { chromium } from 'playwright';

const BASE_URL = 'https://jooble.org';

// Enhanced stealth configuration
const STEALTH_CONFIG = {
    contexts: [
        'chrome-windows',
        'chrome-macos'
    ],
    languages: ['en-US', 'en'],
    screen: {
        width: 1920,
        height: 1080
    }
};

function extractJobLinks(html, pageUrl) {
    const links = new Set();
    // Match href patterns for job links
    const hrefPattern = /href="([^"]*(?:\/desc\/|vacancy)[^"]*)"/gi;
    let match;
    
    while ((match = hrefPattern.exec(html)) !== null) {
        const href = match[1];
        if (!href.includes('/SearchResult') && !href.includes('?ukw=')) {
            const abs = href.startsWith('http') ? href : new URL(href, pageUrl).href;
            if (abs.includes('/desc/') || abs.includes('vacancy')) {
                links.add(abs);
            }
        }
    }
    
    return [...links];
}

async function extractJobData(page, url) {
    const data = {
        title: '',
        company: '',
        location: '',
        salary: '',
        date_posted: '',
        job_type: '',
        description: '',
        job_url: url,
        scrapedAt: new Date().toISOString()
    };

    try {
        // Try JSON-LD first (most reliable)
        const jsonLd = await page.$$eval('script[type="application/ld+json"]', scripts => {
            for (const script of scripts) {
                try {
                    const json = JSON.parse(script.textContent || '{}');
                    if (json['@type'] === 'JobPosting') return json;
                } catch (e) {}
            }
            return null;
        }).catch(() => null);

        if (jsonLd) {
            data.title = jsonLd.title || '';
            data.company = jsonLd.hiringOrganization?.name || '';
            data.location = jsonLd.jobLocation?.address?.addressLocality || 
                           jsonLd.jobLocation?.address?.addressRegion || '';
            data.salary = jsonLd.baseSalary?.value?.value || jsonLd.baseSalary?.minValue || '';
            data.date_posted = jsonLd.datePosted || '';
            data.job_type = jsonLd.employmentType || '';
            data.description = jsonLd.description || '';
        }

        // Fallback to DOM selectors
        if (!data.title) {
            data.title = await page.locator('h1, .job-title, .vacancy-title, [data-qa="vacancy-title"]').first().textContent().catch(() => '') || '';
        }
        
        if (!data.company) {
            data.company = await page.locator('.company, .employer, .company-name, [data-qa="vacancy-company-name"]').first().textContent().catch(() => '') || '';
        }
        
        if (!data.location) {
            data.location = await page.locator('.location, .job-location, [data-qa="vacancy-view-location"]').first().textContent().catch(() => '') || '';
        }
        
        if (!data.salary) {
            data.salary = await page.locator('.salary, .compensation, [data-qa="vacancy-salary"]').first().textContent().catch(() => '') || '';
        }
        
        if (!data.description) {
            data.description = await page.locator('.job-description, .description, .vacancy-description, [data-qa="vacancy-description"], main, article').first().textContent().catch(() => '') || '';
            data.description = data.description.trim().substring(0, 5000);
        }
    } catch (err) {
        log.warning(`Error extracting data: ${err.message}`);
    }

    return data;
}

async function run() {
    await Actor.init();
    let saved = 0;

    try {
        const input = (await Actor.getInput()) || {};
        const {
            searchQuery = 'developer',
            maxPages = 3,
            maxItems = 50,
            maxConcurrency = 2,
            proxyConfiguration: inputProxyConfig
        } = input;

        log.info(`🎬 Jooble scraper started | query="${searchQuery}" maxPages=${maxPages} maxItems=${maxItems}`);

        // Create proxy configuration
        let proxyConfiguration;
        try {
            if (inputProxyConfig?.useApifyProxy) {
                proxyConfiguration = await Actor.createProxyConfiguration(inputProxyConfig);
            } else {
                proxyConfiguration = await Actor.createProxyConfiguration();
            }
            log.info('✅ Proxy configured');
        } catch (e) {
            log.warning('⚠️ No proxy, continuing without');
            proxyConfiguration = undefined;
        }

        const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
        
        const crawler = new PlaywrightCrawler({
            proxyConfiguration,
            maxConcurrency,
            maxRequestRetries: 3,
            requestHandlerTimeoutSecs: 90,
            navigationTimeoutSecs: 60,
            headless: true,
            
            // Browser launch options with stealth
            launchContext: {
                launcher: chromium,
                launchOptions: {
                    headless: true,
                    args: [
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-web-security',
                        '--disable-features=IsolateOrigins,site-per-process',
                        '--window-size=1920,1080'
                    ]
                }
            },

            // Pre-navigation stealth setup
            async preNavigationHooks({ page, request }) {
                // Stealth: Remove webdriver flags
                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                    
                    // Override permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                    
                    // Chrome runtime
                    window.chrome = { runtime: {} };
                });

                // Set realistic viewport
                await page.setViewportSize(STEALTH_CONFIG.screen);
                
                log.debug(`➡️ Navigating: ${request.url}`);
            },
            
            async requestHandler({ page, request, crawler }) {
                const label = request.userData?.label || 'SEARCH';
                const pageNum = request.userData?.page || 1;

                // Wait for page to load
                try {
                    await page.waitForLoadState('domcontentloaded', { timeout: 30000 });
                    await page.waitForTimeout(1500 + Math.random() * 1000); // Human-like delay
                } catch (e) {
                    log.warning(`Timeout waiting for page load: ${request.url}`);
                }

                // Check for blocks/captcha
                const html = await page.content();
                if (/blocked|captcha|verify you are human|access denied/i.test(html)) {
                    log.warning(`🚧 Bot detection on ${request.url}`);
                    throw new Error('Bot detection triggered');
                }

                if (label === 'SEARCH') {
                    const links = extractJobLinks(html, request.url);
                    log.info(`🔎 Page ${pageNum}: found ${links.length} job links`);

                    if (links.length === 0) {
                        log.warning(`No jobs found on page ${pageNum}. Page might be blocked or no results.`);
                        return;
                    }

                    // Enqueue job detail pages
                    const remaining = maxItems - saved;
                    for (const jobUrl of links.slice(0, Math.max(0, remaining))) {
                        await crawler.addRequests([{
                            url: jobUrl,
                            userData: { label: 'DETAIL' }
                        }]);
                    }

                    // Pagination
                    if (pageNum < maxPages && saved < maxItems && links.length > 0) {
                        const nextPageNum = pageNum + 1;
                        const nextUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}&p=${nextPageNum}`;
                        await crawler.addRequests([{
                            url: nextUrl,
                            userData: { label: 'SEARCH', page: nextPageNum }
                        }]);
                        log.info(`📄 Queued page ${nextPageNum}`);
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    const jobData = await extractJobData(page, request.url);
                    
                    if (jobData.title) {
                        await Dataset.pushData(jobData);
                        saved++;
                        log.info(`✅ Saved #${saved}: ${jobData.title.substring(0, 60)}`);
                        
                        // Stop if reached max
                        if (saved >= maxItems) {
                            log.info(`🎯 Reached maxItems (${maxItems})`);
                            await crawler.autoscaledPool?.abort();
                        }
                    } else {
                        log.warning(`⚠️ No title found on ${request.url}`);
                    }
                }
            },

            failedRequestHandler({ request, error }) {
                log.error(`❌ Failed: ${request.url} | ${error.message}`);
            },
        });

        await crawler.run([{ 
            url: startUrl, 
            userData: { label: 'SEARCH', page: 1 } 
        }]);

        log.info(`🎉 Scraping complete! Total jobs: ${saved}`);
    } catch (err) {
        log.error('❌ Fatal error:', err);
        throw err;
    } finally {
        await Actor.exit();
    }
}

await Actor.main(run);
