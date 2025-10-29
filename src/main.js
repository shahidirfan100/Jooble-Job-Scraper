import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset } from 'crawlee';
import { gotScraping } from 'got-scraping';
import { load as cheerioLoad } from 'cheerio';

// ───────────────────────────────────────────────────────────────
// Helpers & Config
// ───────────────────────────────────────────────────────────────
const BASE_URL = 'https://jooble.org';
const MAX_RETRIES = 5;

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
        'DNT': '1',
        'Cache-Control': 'no-cache',
        Pragma: 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': fetchSite,
        'Sec-Fetch-User': '?1',
        'Priority': 'u=0, i',
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
    const endpoints = [
        `${BASE_URL}/robots.txt`,
        `${BASE_URL}/`,
    ];

    for (let i = 0; i < endpoints.length; i++) {
        try {
            const response = await gotScraping({
                url: endpoints[i],
                proxyUrl: proxyInfo?.url,
                headers,
                http2: i === 0, // try http2 first on robots, then fallback to http1
                throwHttpErrors: false,
                timeout: { request: 15000 },
            });

            if (response.statusCode >= 400) {
                continue;
            }

            await sleep(rand(200, 500));
            const updates = parseSetCookieHeaders(response.headers?.['set-cookie']);
            if (Object.keys(updates).length > 0) {
                session.userData.cookieJar = mergeCookies(session.userData.cookieJar, updates);
                return cookieJarToHeader(session.userData.cookieJar);
            }
        } catch {
            // ignore and try next endpoint
        }
    }

    // Could not obtain consent cookies; return empty and let caller handle with retries/backoff
    return '';
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

function isPlainObject(obj) {
    return obj && typeof obj === 'object' && !Array.isArray(obj);
}

function sanitizeProxyConfiguration(inputProxyConfiguration) {
    if (!isPlainObject(inputProxyConfiguration)) return null;
    const allowedKeys = new Set([
        'useApifyProxy', 'apifyProxyGroups', 'apifyProxyCountry', 'password', 'newUrlFunction',
        'hostname', 'port', 'username', 'endpoint', 'groups', 'country',
    ]);
    const out = {};
    for (const [k, v] of Object.entries(inputProxyConfiguration)) {
        if (!allowedKeys.has(k)) continue;
        out[k] = v;
    }
    // Basic normalization of legacy aliases
    if (Array.isArray(out.groups) && !out.apifyProxyGroups) out.apifyProxyGroups = out.groups;
    if (typeof out.country === 'string' && !out.apifyProxyCountry) out.apifyProxyCountry = out.country;
    if (typeof out.useApifyProxy !== 'boolean' && (out.apifyProxyGroups || out.apifyProxyCountry)) out.useApifyProxy = true;
    return Object.keys(out).length ? out : null;
}

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
            startUrls,
            proxyConfiguration: inputProxyConfiguration,
        } = input;

        let proxyConfiguration;
        try {
            const sanitized = sanitizeProxyConfiguration(inputProxyConfiguration);
            proxyConfiguration = await Actor.createProxyConfiguration(
                sanitized || { useApifyProxy: true }
            );
        } catch (e) {
            log.warning(`Proxy configuration invalid, falling back to direct connection: ${e?.message || e}`);
            proxyConfiguration = await Actor.createProxyConfiguration({ useApifyProxy: false });
        }

        const crawler = new CheerioCrawler({
            proxyConfiguration,
            useSessionPool: true,
            persistCookiesPerSession: true,
            maxConcurrency,
            maxRequestRetries: MAX_RETRIES,
            requestHandlerTimeoutSecs: 60,
            sessionPoolOptions: {
                maxPoolSize: Math.max(4, Math.min(30, maxConcurrency * 3)),
                sessionOptions: {
                    maxUsageCount: 15,
                },
            },
            preNavigationHooks: [
                async (crawlingContext, goToRequest) => {
                    const { request, session, proxyInfo } = crawlingContext;
                    const profile = getSessionProfile(session);
                    const referer = request.userData?.referer || `${BASE_URL}/`;
                    const fetchSite = referer ? 'same-origin' : 'none';
                    const headers = buildHeaders(profile, referer, fetchSite);

                    if (session) {
                        const cookieHeaderBefore = cookieJarToHeader(session.userData.cookieJar);
                        if (cookieHeaderBefore) headers.Cookie = cookieHeaderBefore;
                        const consentCookieHeader = await ensureConsentForSession(session, proxyInfo);
                        if (consentCookieHeader) headers.Cookie = consentCookieHeader;
                        else session.markBad();
                    }

                    request.headers = { ...(request.headers || {}), ...headers };
                    await sleep(rand(200, 700));
                    await goToRequest();
                },
            ],
            postNavigationHooks: [
                async ({ session, response }) => {
                    if (session && response?.headers?.['set-cookie']) {
                        session.userData.cookieJar = mergeCookies(
                            session.userData.cookieJar,
                            parseSetCookieHeaders(response.headers['set-cookie']),
                        );
                    }
                },
            ],

            async requestHandler(context) {
                const { request, session, enqueueLinks, response, body } = context;
                const label = request.userData?.label || 'SEARCH';
                const page = request.userData?.page || 1;
                const status = response?.statusCode ?? 0;
                const $ = context.$ || cheerioLoad(body || '');

                // Handle blocking and throttling
                if (!body || status === 401 || status === 403 || status === 429 || isCookieOrBotWall(body)) {
                    log.warning(`🚧 Blocked/Throttled (${status}) on ${request.url}`);
                    // 429 (Too Many Requests): mark session bad but not retire immediately
                    if (status === 429) session?.markBad(); else session?.retire();
                    // Exponential backoff based on retry count
                    const attempt = (request.retryCount ?? 0) + 1;
                    const backoffMs = Math.min(15000, 500 * 2 ** attempt + rand(0, 500));
                    await sleep(backoffMs);
                    throw new Error(`Blocked ${status}`);
                }

                if (status >= 400) {
                    throw new Error(`HTTP ${status}`);
                }

                if (label === 'SEARCH') {
                    if (saved >= maxItems) return;

                    // Try to extract total results or detect empty result pages
                    const possibleNoResults = $('h1:contains("no results"), .zero-result, .no-results').length > 0;
                    const links = extractDetailLinks($, request.url);
                    if (links.length === 0 && possibleNoResults) {
                        log.info(`ℹ️ No results detected on page ${page} — stopping pagination.`);
                        return;
                    }
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
                        // Find explicit next link first
                        let nextHref = $('a[rel="next"], a.next, a:contains("Next")').attr('href');
                        if (!nextHref) {
                            const next = new URL(request.url);
                            next.searchParams.set('p', page + 1);
                            nextHref = next.href;
                        } else {
                            nextHref = nextHref.startsWith('http') ? nextHref : new URL(nextHref, request.url).href;
                        }
                        await enqueueLinks({
                            urls: [nextHref],
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

                    // Prefer JSON-LD JobPosting when available
                    let title = '';
                    let company = '';
                    let locationVal = '';
                    let salary = '';
                    let descriptionHtml = '';
                    let descriptionText = '';

                    const jsonLd = [];
                    $('script[type="application/ld+json"]').each((_, el) => {
                        try {
                            const txt = $(el).contents().text();
                            if (!txt) return;
                            const parsed = JSON.parse(txt);
                            if (Array.isArray(parsed)) jsonLd.push(...parsed);
                            else jsonLd.push(parsed);
                        } catch { /* ignore */ }
                    });

                    const jobPosting = jsonLd.find((o) => o && (o['@type'] === 'JobPosting' || (Array.isArray(o['@type']) && o['@type'].includes('JobPosting'))));
                    if (jobPosting) {
                        title = jobPosting.title || '';
                        company = jobPosting.hiringOrganization?.name || jobPosting.employerOverview?.name || '';
                        locationVal = jobPosting.jobLocation?.address?.addressLocality || jobPosting.jobLocation?.address?.addressRegion || '';
                        salary = jobPosting.baseSalary?.value?.value ? `${jobPosting.baseSalary?.value?.value} ${jobPosting.baseSalary?.value?.currency || ''}`.trim() : '';
                        if (jobPosting.description) {
                            descriptionHtml = String(jobPosting.description);
                            descriptionText = cheerioLoad(descriptionHtml)('body').text().replace(/\s+/g, ' ').trim();
                        }
                    }

                    if (!title) title = $('h1, [data-test="vacancy-title"], .job-title, .title').first().text().trim();
                    if (!company) company = $('[data-test="vacancy-company"], .company, .employer, .company-name').first().text().trim();
                    if (!locationVal) locationVal = $('[data-test="vacancy-location"], .location, .job-location').first().text().trim();
                    if (!salary) salary = $('[data-test="vacancy-salary"], .salary, .compensation').first().text().trim();
                    if (!descriptionText) {
                        const descEl = $('[data-test="vacancy-description"], .job-description, .description, .vacancy-description, .content, main').first();
                        descriptionHtml = descEl.html() || '';
                        descriptionText = descEl.text().replace(/\s+/g, ' ').trim();
                    }

                    if (!title) {
                        log.warning(`⚠️ Missing title on ${request.url}`);
                        return;
                    }

                    await Dataset.pushData({
                        title,
                        company,
                        location: locationVal,
                        salary,
                        description_html: descriptionHtml,
                        description_text: descriptionText,
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

        let startRequests = [];
        if (Array.isArray(startUrls) && startUrls.length > 0) {
            const validUrls = startUrls.filter((u) => typeof u === 'string' && u.trim());
            startRequests = validUrls.map((u, idx) => ({ url: u, userData: { label: 'SEARCH', page: 1, idx } }));
        } else {
            const startUrl = new URL(`${BASE_URL}/SearchResult`);
            if (searchQuery) startUrl.searchParams.set('ukw', searchQuery);
            if (location) startUrl.searchParams.set('rgns', location);
            startRequests = [{ url: startUrl.href, userData: { label: 'SEARCH', page: 1 } }];
        }

        await crawler.run(startRequests);
        log.info(`🎉 Finished — ${saved} job(s) saved.`);
    } catch (err) {
        const message = err?.message || String(err);
        const stack = err?.stack || '';
        log.error(`❌ Error in main(): ${message}`);
        if (stack) log.error(stack);
        throw err;
    } finally {
        await Actor.exit();
    }
}

await main();
