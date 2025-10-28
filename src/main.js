import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';

const BASE_URL = 'https://jooble.org';

function extractJobLinks(pageUrl, $) {
    const set = new Set();
    // Multiple selectors for job links on Jooble
    $('a[href*="/desc/"], a[data-qa="vacancy-serp__vacancy-title"], a[class*="job-link"], a.position-link, article a').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;
        // Skip non-job links
        if (href.includes('/SearchResult') || href.includes('?ukw=')) return;
        const abs = href.startsWith('http') ? href : new URL(href, pageUrl).href;
        if (abs.includes('/desc/')) {
            set.add(abs);
        }
    });
    return [...set];
}

function extractJobData($, url) {
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

    // Try JSON-LD first
    $('script[type="application/ld+json"]').each((_, el) => {
        try {
            const json = JSON.parse($(el).html() || '{}');
            if (json['@type'] === 'JobPosting') {
                data.title = json.title || data.title;
                data.company = json.hiringOrganization?.name || data.company;
                data.location = json.jobLocation?.address?.addressLocality || 
                               json.jobLocation?.address?.addressRegion || data.location;
                data.salary = json.baseSalary?.value?.value || json.baseSalary?.minValue || data.salary;
                data.date_posted = json.datePosted || data.date_posted;
                data.job_type = json.employmentType || data.job_type;
                data.description = json.description || data.description;
            }
        } catch (e) {
            // Skip invalid JSON
        }
    });

    // Fallback to HTML selectors
    if (!data.title) {
        data.title = $('h1').first().text().trim() ||
                    $('.job-title, .vacancy-title, [data-qa="vacancy-title"]').first().text().trim();
    }
    
    if (!data.company) {
        data.company = $('.company, .employer, .company-name, [data-qa="vacancy-company-name"]').first().text().trim();
    }
    
    if (!data.location) {
        data.location = $('.location, .job-location, [data-qa="vacancy-view-location"]').first().text().trim();
    }
    
    if (!data.salary) {
        data.salary = $('.salary, .compensation, [data-qa="vacancy-salary"]').first().text().trim();
    }
    
    if (!data.description) {
        data.description = $('.job-description, .description, .vacancy-description, [data-qa="vacancy-description"]').first().text().trim() ||
                          $('main, article, .content').first().text().trim().substring(0, 5000);
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
            maxConcurrency = 5,
            proxyConfiguration: inputProxyConfig
        } = input;

        log.info(`🎬 Jooble scraper started | query="${searchQuery}" maxPages=${maxPages} maxItems=${maxItems}`);

        // Create proxy configuration - handle both local and Apify platform
        let proxyConfiguration;
        try {
            if (inputProxyConfig?.useApifyProxy) {
                proxyConfiguration = await Actor.createProxyConfiguration(inputProxyConfig);
                log.info('✅ Using Apify proxy');
            } else {
                proxyConfiguration = await Actor.createProxyConfiguration();
                log.info('✅ Proxy configuration created');
            }
        } catch (e) {
            log.warning('⚠️ No proxy available, continuing without proxy');
            proxyConfiguration = undefined;
        }

        const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
        
        const crawler = new CheerioCrawler({
            proxyConfiguration,
            maxConcurrency,
            maxRequestRetries: 3,
            requestHandlerTimeoutSecs: 60,
            
            // Enhanced request headers for stealth
            async requestHandler({ $, request, enqueueLinks, crawler }) {
                const label = request.userData?.label || 'SEARCH';
                const pageNum = request.userData?.page || 1;

                // Check for errors or blocks
                const bodyText = $('body').text();
                if (/blocked|captcha|verify you are human/i.test(bodyText)) {
                    log.warning(`🚧 Possible block detected on ${request.url}`);
                    return;
                }

                if (label === 'SEARCH') {
                    const links = extractJobLinks(request.url, $);
                    log.info(`🔎 Page ${pageNum}: found ${links.length} job links`);

                    // Enqueue job detail pages
                    const remaining = maxItems - saved;
                    for (const jobUrl of links.slice(0, Math.max(0, remaining))) {
                        await crawler.addRequests([{
                            url: jobUrl,
                            userData: { label: 'DETAIL' }
                        }]);
                    }

                    // Pagination
                    if (pageNum < maxPages && saved < maxItems) {
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
                    const jobData = extractJobData($, request.url);
                    
                    if (jobData.title) {
                        await Dataset.pushData(jobData);
                        saved++;
                        log.info(`✅ Saved #${saved}: ${jobData.title.substring(0, 60)}`);
                    } else {
                        log.warning(`⚠️ No title found on ${request.url}`);
                    }

                    // Stop if reached max items
                    if (saved >= maxItems) {
                        log.info(`🎯 Reached maxItems (${maxItems}), stopping crawler`);
                        await crawler.autoscaledPool?.abort();
                    }
                }
            },

            failedRequestHandler({ request, error }) {
                log.error(`❌ Request failed: ${request.url} | ${error.message}`);
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
