import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset } from 'crawlee';
import { load as loadCheerio } from 'cheerio';

const BASE_URL = 'https://jooble.org';

function extractJobLinks(html, pageUrl) {
    const links = new Set();
    const hrefPattern = /href="([^"]*(?:\/desc\/|vacancy)[^"]*)"/gi;
    let match;
    while ((match = hrefPattern.exec(html)) !== null) {
        const href = match[1];
        if (!href.includes('/SearchResult') && !href.includes('?ukw=')) {
            const abs = href.startsWith('http') ? href : new URL(href, pageUrl).href;
            if (abs.includes('/desc/') || abs.includes('vacancy')) links.add(abs);
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
        scrapedAt: new Date().toISOString(),
    };

    try {
        const jsonLd = await page.$$eval('script[type="application/ld+json"]', scripts => {
            for (const s of scripts) {
                try {
                    const json = JSON.parse(s.textContent || '{}');
                    if (json['@type'] === 'JobPosting') return json;
                } catch (_) {}
            }
            return null;
        });

        if (jsonLd) {
            data.title = jsonLd.title || '';
            data.company = jsonLd.hiringOrganization?.name || '';
            data.location = jsonLd.jobLocation?.address?.addressLocality || '';
            data.salary = jsonLd.baseSalary?.value?.value || '';
            data.date_posted = jsonLd.datePosted || '';
            data.job_type = jsonLd.employmentType || '';
            data.description = jsonLd.description || '';
        }

        if (!data.title)
            data.title = await page.locator('h1, .job-title, .vacancy-title').first().textContent().catch(() => '') || '';
        if (!data.company)
            data.company = await page.locator('.company, .employer, .company-name').first().textContent().catch(() => '') || '';
        if (!data.location)
            data.location = await page.locator('.location, .job-location').first().textContent().catch(() => '') || '';
        if (!data.salary)
            data.salary = await page.locator('.salary, .compensation').first().textContent().catch(() => '') || '';
        if (!data.description)
            data.description = (await page.locator('.job-description, main, article').first().textContent().catch(() => ''))
                .trim()
                .substring(0, 5000);
    } catch (err) {
        log.warning(`Error extracting job data: ${err.message}`);
    }

    return data;
}

async function run() {
    await Actor.init();
    let saved = 0;

    try {
        const input = (await Actor.getInput()) || {};
        const { searchQuery = 'developer', maxPages = 3, maxItems = 20, proxyConfiguration: inputProxyConfig } = input;

        log.info(`🎬 Jooble scraper started | query="${searchQuery}" maxPages=${maxPages} maxItems=${maxItems}`);

        // ✅ Proxy setup
        let proxyConfiguration;
        try {
            proxyConfiguration = await Actor.createProxyConfiguration(
                inputProxyConfig?.useApifyProxy ? inputProxyConfig : { useApifyProxy: true, apifyProxyGroups: ['RESIDENTIAL'] },
            );
            log.info('✅ Proxy configured');
        } catch {
            log.warning('⚠️ Proxy not available, continuing without');
        }

        const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;

        // ✅ Fixed PlaywrightCrawler constructor
        const crawler = new PlaywrightCrawler({
            proxyConfiguration,
            headless: true,
            maxRequestsPerCrawl: maxPages * 20, // prevents infinite crawl loops
            requestHandlerTimeoutSecs: 90,
            navigationTimeoutSecs: 60,
            useSessionPool: true,
            launchContext: {
                launchOptions: {
                    headless: true,
                    args: [
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-blink-features=AutomationControlled',
                        '--window-size=1920,1080',
                    ],
                },
            },
            preNavigationHooks: [
                async ({ page }) => {
                    // minimal stealth setup
                    await page.addInitScript(() => {
                        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                        window.chrome = { runtime: {} };
                    });
                    await page.setViewportSize({ width: 1920, height: 1080 });
                },
            ],
            async requestHandler({ page, request, enqueueLinks, crawler }) {
                const label = request.userData?.label || 'SEARCH';
                const pageNum = request.userData?.page || 1;

                await page.waitForLoadState('domcontentloaded');
                await page.waitForTimeout(1000 + Math.random() * 500);
                const html = await page.content();

                if (/blocked|captcha|verify you are human/i.test(html)) {
                    log.warning(`🚧 Bot detection on ${request.url}`);
                    throw new Error('Blocked by bot wall');
                }

                if (label === 'SEARCH') {
                    const links = extractJobLinks(html, request.url);
                    log.info(`🔎 Page ${pageNum}: found ${links.length} job links`);

                    if (!links.length) {
                        log.warning(`No jobs on page ${pageNum}`);
                        return;
                    }

                    for (const link of links.slice(0, maxItems - saved)) {
                        await crawler.addRequests([{ url: link, userData: { label: 'DETAIL' } }]);
                    }

                    if (pageNum < maxPages && saved < maxItems) {
                        const nextUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}&p=${pageNum + 1}`;
                        await crawler.addRequests([{ url: nextUrl, userData: { label: 'SEARCH', page: pageNum + 1 } }]);
                        log.info(`📄 Queued next page ${pageNum + 1}`);
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    const data = await extractJobData(page, request.url);
                    if (data.title) {
                        await Dataset.pushData(data);
                        saved++;
                        log.info(`✅ Saved #${saved}: ${data.title}`);
                        if (saved >= maxItems) {
                            log.info(`🎯 Reached maxItems (${maxItems})`);
                            await crawler.autoscaledPool?.abort();
                        }
                    }
                }
            },
            failedRequestHandler({ request, error }) {
                log.error(`❌ Failed: ${request.url} | ${error.message}`);
            },
        });

        await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);
        log.info(`🎉 Scraping complete! Total jobs: ${saved}`);
    } catch (err) {
        log.error('❌ Fatal error:', err);
        throw err;
    } finally {
        await Actor.exit();
    }
}

// ✅ Make sure it executes on start
await Actor.main(run);
