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

function getSessionProfile(session) {
    if (!session) {
        const profile = randomProfile();
        return { ua: profile.ua, ch: { ...(profile.ch || {}) } };
    }
    if (!session.userData.profile) {
        const profile = randomProfile();
        session.userData.profile = { ua: profile.ua, ch: { ...(profile.ch || {}) } };
    }
    return session.userData.profile;
}

function buildHeaders(profile, referer, fetchSite) {
    const headers = {
        'User-Agent': profile.ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control': 'no-cache',
        Pragma: 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': fetchSite,
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        ...(profile.ch || {}),
    };
    if (referer) headers.Referer = referer;
    return headers;
}

function parseSetCookieHeaders(setCookieHeader) {
    const arr = Array.isArray(setCookieHeader) ? setCookieHeader : (setCookieHeader ? [setCookieHeader] : []);
    const cookies = {};
    for (const header of arr) {
        if (!header) continue;
        const [pair] = header.split(';');
        const eqIndex = pair.indexOf('=');
        if (eqIndex === -1) continue;
        const name = pair.slice(0, eqIndex).trim();
        const value = pair.slice(eqIndex + 1).trim();
        if (!name) continue;
        cookies[name] = value;
    }
    return cookies;
}

function mergeCookies(target = {}, updates = {}) {
    const merged = { ...(target || {}) };
    for (const [name, value] of Object.entries(updates)) {
        if (value === undefined || value === '') {
            delete merged[name];
        } else {
            merged[name] = value;
        }
    }
    return merged;
}

function cookieJarToHeader(jar = {}) {
    return Object.entries(jar)
        .filter(([, value]) => value !== undefined && value !== '')
        .map(([name, value]) => `${name}=${value}`)
        .join('; ');
}

async function ensureConsentForSession(session, proxyInfo) {
    if (!session) return '';
    if (!session.userData.cookieJar) session.userData.cookieJar = {};
    if (Object.keys(session.userData.cookieJar).length > 0) {
        return cookieJarToHeader(session.userData.cookieJar);
    }

    const profile = getSessionProfile(session);
    const headers = buildHeaders(profile, undefined, 'none');
    const response = await gotScraping({
        url: BASE_URL,
        proxyUrl: proxyInfo?.url,
        headers,
        http2: true,
        throwHttpErrors: false,
        timeout: { request: 15000 },
    });

    if (response.statusCode >= 400) {
        throw new Error(`Consent fetch failed with status ${response.statusCode}`);
    }

    await sleep(rand(200, 500));

    const updates = parseSetCookieHeaders(response.headers?.['set-cookie']);
    if (Object.keys(updates).length === 0) {
        throw new Error('Consent cookies missing in response');
    }
    session.userData.cookieJar = mergeCookies(session.userData.cookieJar, updates);
    return cookieJarToHeader(session.userData.cookieJar);
}

function isCookieOrBotWall(html) {
    const t = (html || '').toLowerCase();
    return /are you human|verify you are human|captcha|cloudflare|before you continue to jooble|accept our cookies|consent|access denied/i.test(t);
}

function extractDetailLinks($, base = BASE_URL) {
    const set = new Set();
    const patterns = [/\/desc\//i, /\/job-offer\//i, /\bdesc\b/i, /\/j\//i, /\bjobid\b/i];
    $('a[href]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href) return;
        if (!patterns.some((pattern) => pattern.test(href))) return;
        const abs = href.startsWith('http') ? href : new URL(href, base).href;
        if (!abs.includes('jooble.org')) return;
        set.add(abs);
    });
    return [...set];
}

function rand(min, max) { return Math.random() * (max - min) + min; }
async function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

// ───────────────────────────────────────────────────────────────
// Main
// ───────────────────────────────────────────────────────────────
export async function main() {
    await Actor.init();

    let saved = 0;
    let plannedDetails = 0;

    try {
        const input = (await Actor.getInput()) || {};
        const {
            searchQuery = 'developer',
            location = '',
            maxPages = 3,
            maxConcurrency = 3,
            maxItems = 50,
        } = input;

        const proxyConfiguration = await Actor.createProxyConfiguration({
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL'],
        });

        const crawler = new CheerioCrawler({
            proxyConfiguration,
            useSessionPool: true,
            persistCookiesPerSession: true,
            maxConcurrency,
            maxRequestRetries: MAX_RETRIES,
            requestHandlerTimeoutSecs: 60,
            async requestFunction({ request, session, proxyInfo }) {
                const profile = getSessionProfile(session);
                const referer = request.userData?.referer || `${BASE_URL}/`;
                const fetchSite = referer ? 'same-origin' : 'none';
                const headers = buildHeaders(profile, referer, fetchSite);

                if (session) {
                    try {
                        await ensureConsentForSession(session, proxyInfo);
                    } catch (error) {
                        log.debug(`Consent fetch failed for session ${session.id}: ${error.message}`);
                        session.retire();
                        throw error;
                    }
                    const cookieHeader = cookieJarToHeader(session.userData.cookieJar);
                    if (cookieHeader) headers.Cookie = cookieHeader;
                }

                await sleep(rand(200, 700));

                const response = await gotScraping({
                    url: request.url,
                    proxyUrl: proxyInfo?.url,
                    headers,
                    http2: true,
                    throwHttpErrors: false,
                    timeout: { request: 20000 },
                });

                if (session && response.headers?.['set-cookie']) {
                    session.userData.cookieJar = mergeCookies(
                        session.userData.cookieJar,
                        parseSetCookieHeaders(response.headers['set-cookie']),
                    );
                }

                const contentType = response.headers?.['content-type'];
                return { body: response.body, statusCode: response.statusCode, headers: response.headers, contentType };
            },

            async requestHandler(context) {
                const { request, session, enqueueLinks, response, body } = context;
                const label = request.userData?.label || 'SEARCH';
                const page = request.userData?.page || 1;
                const status = response?.statusCode ?? 0;
                const $ = context.$ || cheerioLoad(body || '');

                if (!body || status === 403 || status === 401 || isCookieOrBotWall(body)) {
                    log.warning(`🚧 Blocked (${status}) on ${request.url}`);
                    session?.retire();
                    throw new Error(`Blocked ${status}`);
                }

                if (status >= 400) {
                    throw new Error(`HTTP ${status}`);
                }

                if (label === 'SEARCH') {
                    if (saved >= maxItems) return;

                    const links = extractDetailLinks($, request.url);
                    log.info(`🔎 Page ${page}: ${links.length} potential jobs`);

                    for (const link of links) {
                        if (saved + plannedDetails >= maxItems) break;
                        const { processedRequests = [] } = await enqueueLinks({
                            urls: [link],
                            transformRequestFunction: (req) => {
                                req.userData = { label: 'DETAIL', referer: request.url, planned: true };
                                return req;
                            },
                        });
                        if (processedRequests.length) plannedDetails += processedRequests.length;
                    }

                    if (page < maxPages && saved + plannedDetails < maxItems) {
                        const next = new URL(request.url);
                        next.searchParams.set('p', page + 1);
                        await enqueueLinks({
                            urls: [next.href],
                            transformRequestFunction: (req) => {
                                req.userData = { label: 'SEARCH', page: page + 1, referer: request.url };
                                return req;
                            },
                        });
                    }
                    return;
                }

                if (label === 'DETAIL') {
                    if (request.userData?.planned) {
                        plannedDetails = Math.max(0, plannedDetails - 1);
                        request.userData.planned = false;
                    }
                    if (saved >= maxItems) return;

                    const title = $('h1, [data-test="vacancy-title"], .job-title, .title').first().text().trim();
                    const company = $('[data-test="vacancy-company"], .company, .employer, .company-name').first().text().trim();
                    const locationVal = $('[data-test="vacancy-location"], .location, .job-location').first().text().trim();
                    const salary = $('[data-test="vacancy-salary"], .salary, .compensation').first().text().trim();
                    const desc = $('[data-test="vacancy-description"], .job-description, .description, .vacancy-description, .content, main').first().text().replace(/\s+/g, ' ').trim();

                    if (!title) {
                        log.warning(`⚠️ Missing title on ${request.url}`);
                        return;
                    }

                    await Dataset.pushData({
                        title,
                        company,
                        location: locationVal,
                        salary,
                        description: desc,
                        job_url: request.url,
                        source_page: request.userData?.referer,
                    });
                    saved++;
                    session?.markGood();
                    log.info(`✅ Saved #${saved}: ${title}`);
                }
            },

            async failedRequestHandler({ request }) {
                log.error(`❌ Failed after retries: ${request.url}`);
                await Dataset.pushData({
                    error: 'RequestFailed',
                    url: request.url,
                    label: request.userData?.label,
                });
            },
        });

        const startUrl = new URL(`${BASE_URL}/SearchResult`);
        startUrl.searchParams.set('ukw', searchQuery);
        if (location) startUrl.searchParams.set('rgns', location);

        await crawler.run([{ url: startUrl.href, userData: { label: 'SEARCH', page: 1 } }]);
        log.info(`🎉 Finished — ${saved} job(s) saved.`);
    } catch (err) {
        log.error('❌ Error in main():', err);
        throw err;
    } finally {
        await Actor.exit();
    }
}

await main();
