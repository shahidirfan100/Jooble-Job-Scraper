import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { gotScraping } from 'got-scraping';
import { load as cheerioLoad } from 'cheerio';

// ───────────────────────────────────────────────────────────────
// Helpers & Config
// ───────────────────────────────────────────────────────────────
const BASE_URL = 'https://jooble.org';
const MAX_RETRIES = 3;

const UA_PROFILES = [
    {
        ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        ch: {
            'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="128", "Google Chrome";v="128"',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-mobile': '?0',
        },
    },
    {
        ua: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
        ch: {
            'sec-ch-ua-platform': '"macOS"',
            'sec-ch-ua-mobile': '?0',
        },
    },
];

function randomProfile() {
    return UA_PROFILES[Math.floor(Math.random() * UA_PROFILES.length)];
}

function isCookieOrBotWall(html) {
    const t = html.toLowerCase();
    return /are you human|verify you are human|captcha|cloudflare|before you continue to jooble|accept our cookies|consent/i.test(t);
}

function extractDetailLinks($, base = BASE_URL) {
    const set = new Set();
    $('a[href*="desc"], a[data-qa="vacancy-serp__vacancy-title"], a[class*="job-link"], a[class*="link position-link"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href || !href.includes('desc')) return;
        const abs = href.startsWith('http') ? href : new URL(href, base).href;
        set.add(abs);
    });
    return [...set];
}

function rand(min, max) { return Math.random() * (max - min) + min; }
async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ───────────────────────────────────────────────────────────────
// Auto-fetch Jooble consent cookies
// ───────────────────────────────────────────────────────────────
async function getConsentCookies(proxyUrl) {
    log.info('🌐 Fetching Jooble consent cookies...');
    const profile = randomProfile();
    const headers = {
        'User-Agent': profile.ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Upgrade-Insecure-Requests': '1',
        ...profile.ch,
    };
    const res = await gotScraping({
        url: BASE_URL,
        proxyUrl,
        headers,
        timeout: { request: 15000 },
        throwHttpErrors: false,
    });

    const setCookies = res.headers['set-cookie'] || [];
    const cookieHeader = Array.isArray(setCookies)
        ? setCookies.map(c => c.split(';')[0]).join('; ')
        : '';
    log.info(cookieHeader ? '✅ Consent cookies obtained' : '⚠️ No cookies returned');
    return cookieHeader;
}

// ───────────────────────────────────────────────────────────────
// Main
// ───────────────────────────────────────────────────────────────
export async function main() {
    await Actor.init();

    let saved = 0;
    let consentCookies = '';

    try {
        const input = (await Actor.getInput()) || {};
        const {
            searchQuery = 'developer',
            maxPages = 3,
            maxConcurrency = 3,
            maxItems = 50,
        } = input;

        const proxyConfiguration = await Actor.createProxyConfiguration({
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL'],
        });
        const proxyUrl = await proxyConfiguration.newUrl();
        consentCookies = await getConsentCookies(proxyUrl);

        const crawler = new CheerioCrawler({
            proxyConfiguration,
            useSessionPool: true,
            persistCookiesPerSession: true,
            maxConcurrency,
            requestHandlerTimeoutSecs: 60,

            async requestFunction({ request, session }) {
                const profile = randomProfile();
                const headers = {
                    'User-Agent': profile.ua,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Upgrade-Insecure-Requests': '1',
                    Cookie: consentCookies, // ✅ attach consent cookies
                    ...profile.ch,
                };
                const proxyUrl = await proxyConfiguration.newUrl(session?.id);

                const response = await gotScraping({
                    url: request.url,
                    proxyUrl,
                    headers,
                    http2: true,
                    throwHttpErrors: false,
                    timeout: { request: 20000 },
                });

                return { body: response.body, statusCode: response.statusCode };
            },

            async requestHandler({ request, body, enqueueLinks, session }) {
                const label = request.userData?.label || 'SEARCH';
                const page = request.userData?.page || 1;
                const $ = cheerioLoad(body);
                const status = request.statusCode;

                if (!body || isCookieOrBotWall(body)) {
                    log.warning(`🚧 Cookie wall on ${request.url}`);
                    session.retire();

                    // Refresh consent cookie and retry
                    consentCookies = await getConsentCookies(await proxyConfiguration.newUrl(session.id));
                    if ((request.userData.retries || 0) < MAX_RETRIES) {
                        await enqueueLinks({
                            urls: [request.url],
                            transformRequestFunction: (req) => {
                                req.userData = { ...request.userData, retries: (request.userData.retries || 0) + 1 };
                                return req;
                            },
                        });
                    } else {
                        await Dataset.pushData({ error: 'Blocked', url: request.url });
                    }
                    return;
                }

                if (label === 'SEARCH') {
                    const links = extractDetailLinks($, request.url);
                    log.info(`🔎 Page ${page}: ${links.length} jobs`);
                    for (const link of links.slice(0, maxItems - saved)) {
                        await enqueueLinks({
                            urls: [link],
                            transformRequestFunction: (req) => {
                                req.userData = { label: 'DETAIL', referer: request.url };
                                return req;
                            },
                        });
                    }

                    if (page < maxPages && saved < maxItems) {
                        const next = new URL(request.url);
                        next.searchParams.set('p', page + 1);
                        await enqueueLinks({
                            urls: [next.href],
                            transformRequestFunction: (req) => {
                                req.userData = { label: 'SEARCH', page: page + 1 };
                                return req;
                            },
                        });
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    const title = $('h1, .job-title, .title').first().text().trim();
                    const company = $('.company, .employer, .company-name').first().text().trim();
                    const location = $('.location, .job-location').first().text().trim();
                    const salary = $('.salary, .compensation').first().text().trim();
                    const desc = $('.job-description, .description, .vacancy-description, .content, main').first().text().replace(/\s+/g, ' ').trim();

                    if (title) {
                        await Dataset.pushData({ title, company, location, salary, description: desc, job_url: request.url });
                        saved++;
                        log.info(`✅ Saved #${saved}: ${title}`);
                    } else {
                        log.warning(`⚠️ Empty detail page: ${request.url}`);
                    }
                }
            },
        });

        const startUrl = `${BASE_URL}/SearchResult?ukw=${encodeURIComponent(searchQuery)}`;
        await crawler.run([{ url: startUrl, userData: { label: 'SEARCH', page: 1 } }]);
        log.info(`🎉 Finished — ${saved} job(s) saved.`);
    } catch (err) {
        log.error('❌ Error in main():', err);
    } finally {
        await Actor.exit();
    }
}
