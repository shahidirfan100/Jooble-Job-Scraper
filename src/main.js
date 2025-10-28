// Jooble.org Job Scraper - Improved for Apify Platform
// Uses: Apify SDK, Crawlee, CheerioCrawler, gotScraping (HTTP-based scraping)

import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

await Actor.init();

async function main() {
    try {
        const input = (await Actor.getInput()) || {};
        const {
            searchQuery = '',
            maxItems: MAX_ITEMS_RAW = 100,
            startUrl,
            startUrls,
            proxyConfiguration,
            collectDetails = true,
        } = input;

        const MAX_ITEMS = Number.isFinite(+MAX_ITEMS_RAW) ? Math.max(1, +MAX_ITEMS_RAW) : Number.MAX_SAFE_INTEGER;
        const MAX_PAGES = 999;

        // --- Helper Functions ---
        const toAbs = (href, base = 'https://jooble.org') => {
            try { return new URL(href, base).href; } catch { return null; }
        };

        const cleanText = (html) => {
            if (!html) return '';
            const $ = cheerioLoad(html);
            $('script, style, noscript, iframe').remove();
            return $.root().text().replace(/\s+/g, ' ').trim();
        };

        const buildStartUrl = (query) => {
            const u = new URL('https://jooble.org/SearchResult');
            if (query) u.searchParams.set('ukw', String(query).trim());
            return u.href;
        };

        // --- Build start URLs ---
        let parsedStartUrls = [];
        if (startUrls) {
            if (typeof startUrls === 'string') {
                try {
                    parsedStartUrls = JSON.parse(startUrls);
                    if (!Array.isArray(parsedStartUrls)) parsedStartUrls = [];
                } catch {
                    parsedStartUrls = [];
                }
            } else if (Array.isArray(startUrls)) parsedStartUrls = startUrls;
        }

        const initial = [];
        if (parsedStartUrls.length) initial.push(...parsedStartUrls);
        if (startUrl) initial.push(startUrl);
        if (!initial.length) {
            if (!searchQuery) log.warning('⚠️ No searchQuery or startUrls provided. Defaulting to "developer".');
            initial.push(buildStartUrl(searchQuery || 'developer'));
        }

        const proxyConf = proxyConfiguration ? await Actor.createProxyConfiguration(proxyConfiguration) : undefined;
        let saved = 0;
        let totalPages = 0;

        // --- Extractors ---
        function extractFromJsonLd($) {
            const scripts = $('script[type="application/ld+json"]');
            for (let i = 0; i < scripts.length; i++) {
                try {
                    const parsed = JSON.parse($(scripts[i]).html() || '');
                    const arr = Array.isArray(parsed) ? parsed : [parsed];
                    for (const e of arr) {
                        if (!e) continue;
                        const t = e['@type'] || e.type;
                        if (t === 'JobPosting' || (Array.isArray(t) && t.includes('JobPosting'))) {
                            return {
                                title: e.title || e.name || null,
                                company: e.hiringOrganization?.name || null,
                                date_posted: e.datePosted || null,
                                description_html: e.description || null,
                                location: (e.jobLocation && e.jobLocation.address && (e.jobLocation.address.addressLocality || e.jobLocation.address.addressRegion)) || null,
                                salary: e.baseSalary?.value || null,
                            };
                        }
                    }
                } catch { /* ignore invalid JSON */ }
            }
            return null;
        }

        function findJobLinks($, base) {
            const links = new Set();

            // modern Jooble selectors
            $('.result__job a[href*="/desc/"], .vacancy__list a[href*="/desc/"], a.job_link[href*="/desc/"]').each((_, a) => {
                const href = $(a).attr('href');
                if (href) {
                    const abs = toAbs(href, base);
                    if (abs) links.add(abs);
                }
            });

            return [...links];
        }

        function findNextPage($, currentUrl, pageNo) {
            const nextBtn = $('.pagination__next, a.pagination__next, a[rel="next"]').first();
            if (nextBtn && nextBtn.length) {
                const href = nextBtn.attr('href');
                if (href) return toAbs(href, currentUrl);
            }
            try {
                const url = new URL(currentUrl);
                const current = Number(url.searchParams.get('p') || pageNo || 1);
                url.searchParams.set('p', String(current + 1));
                return url.href;
            } catch {
                return null;
            }
        }

        const USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
        ];

        // --- Main Crawler ---
        const crawler = new CheerioCrawler({
            proxyConfiguration: proxyConf,
            maxRequestRetries: 3,
            useSessionPool: true,
            maxConcurrency: 5,
            requestHandlerTimeoutSecs: 60,

            prepareRequestFunction: async ({ request }) => {
                const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
                request.headers = {
                    ...(request.headers || {}),
                    'User-Agent': ua,
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': request.userData?.referer || buildStartUrl(searchQuery),
                };
                return request;
            },

            async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
                const label = request.userData?.label || 'LIST';
                const pageNo = request.userData?.pageNo || 1;
                await new Promise(resolve => setTimeout(resolve, Math.random() * 2000 + 1000));

                // Debug info
                crawlerLog.info(`🧭 Loaded ${request.url} (Label: ${label}, Page: ${pageNo}) - Title: ${$('title').text()}`);

                if (label === 'LIST') {
                    totalPages++;
                    const links = findJobLinks($, request.url);
                    crawlerLog.info(`LIST page ${pageNo}: found ${links.length} job links`);

                    if (links.length === 0 && pageNo === 1) {
                        crawlerLog.warning('⚠️ No job links found. Check if selectors changed or query returns no results.');
                    }

                    // Extract metadata if collectDetails = false
                    $('.result__job, .vacancy, .vacancy__item, .result').each((_, el) => {
                        const $el = $(el);
                        const title = $el.find('.job-title, h2, h3').first().text().trim() || null;
                        const company = $el.find('.company, .company_name').first().text().trim() || null;
                        const location = $el.find('.location').first().text().trim() || null;
                        const salary = $el.find('.salary, .pay, .compensation').first().text().trim() || null;
                        const posted = $el.find('.date, .posted, .age, .time').first().text().trim() || null;
                        if (!collectDetails && title) {
                            if (saved < MAX_ITEMS) {
                                Dataset.pushData({ title, company, location, date_posted: posted, salary, job_url: null });
                                saved++;
                            }
                        }
                    });

                    if (collectDetails && links.length) {
                        const remaining = MAX_ITEMS - saved;
                        const toEnqueue = links.slice(0, Math.max(0, remaining)).map(u => ({
                            url: u,
                            userData: { label: 'DETAIL', referer: request.url, retries: 0 },
                        }));
                        if (toEnqueue.length) await enqueueLinks({ urls: toEnqueue });
                    }

                    if (saved < MAX_ITEMS && pageNo < MAX_PAGES) {
                        const next = findNextPage($, request.url, pageNo);
                        if (next) await enqueueLinks({ urls: [next], userData: { label: 'LIST', pageNo: pageNo + 1 } });
                    }
                    return;
                }

                // --- DETAIL PAGE ---
                if (label === 'DETAIL' && saved < MAX_ITEMS) {
                    try {
                        const json = extractFromJsonLd($) || {};
                        const data = { ...json };

                        if (!data.title) data.title = $('h1, .job-title, .title').first().text().trim() || null;
                        if (!data.company) data.company = $('.company, .company_name, .employer').first().text().trim() || null;
                        if (!data.location) data.location = $('.location, .place').first().text().trim() || null;
                        if (!data.date_posted) data.date_posted = $('.date, .posted, .time').first().text().trim() || null;

                        if (!data.description_html) {
                            const desc = $('.job-description, .desc, .vacancy-description, .description, .job-body').first();
                            data.description_html = desc && desc.length ? String(desc.html()).trim() : null;
                        }
                        data.description_text = data.description_html ? cleanText(data.description_html) : null;
                        data.job_type = json.employmentType || null;

                        const salaryDom = $('.salary, .pay, .compensation').first().text().trim();
                        data.salary = data.salary || salaryDom || null;

                        const item = {
                            title: data.title || null,
                            company: data.company || null,
                            location: data.location || null,
                            date_posted: data.date_posted || null,
                            job_type: data.job_type || null,
                            description_html: data.description_html || null,
                            description_text: data.description_text || null,
                            job_url: request.url,
                            salary: data.salary || null,
                        };

                        await Dataset.pushData(item);
                        saved++;
                        crawlerLog.info(`✅ Saved job #${saved}: ${item.title}`);
                    } catch (err) {
                        crawlerLog.error(`DETAIL ${request.url} failed: ${err.message || err}`);
                    }
                }
            },
        });

        // --- Run the crawler ---
        await crawler.run(initial.map(u => ({ url: u, userData: { label: 'LIST', pageNo: 1 } })));

        if (saved === 0) {
            log.warning('⚠️ No items were saved. Try adjusting the query, selectors, or enable proxy.');
        } else {
            log.info(`🎉 Finished scraping. Total saved: ${saved} job(s) across ${totalPages} page(s).`);
        }
    } finally {
        await Actor.exit();
    }
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
