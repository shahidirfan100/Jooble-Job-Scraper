/* ────────────────────────────────────────────────────────────── *
 *  Jooble Scraper – Apify Actor (ESM)
 *  Stack: Apify SDK + Crawlee BasicCrawler + gotScraping + Cheerio
 *  Focus: Stealth & Resilience (403/429/cookie-wall handling)
 * ────────────────────────────────────────────────────────────── */

import { Actor, log } from 'apify';
import {
    BasicCrawler,
    Dataset,
    KeyValueStore,
    RequestQueue,
    SessionPool,
} from 'crawlee';
import { gotScraping } from 'got-scraping';
import { load as cheerioLoad } from 'cheerio';

// ───────────────────────────────────────────────────────────────
// 1) INPUT
// ───────────────────────────────────────────────────────────────
async function getInput() {
    const raw = await KeyValueStore.getInput();
    const defaults = {
        searchQuery: 'software engineer',
        location: '',
        jobAge: 'all',             // 'all' | '1' | '7' | '30'
        maxPages: 5,
        maxConcurrency: 3,         // tuned for stealth; raise to 5–8 once stable
        maxItems: 100,
        // Backoff (+ jitter) limits for block conditions (403/429/cookie-wall)
        maxStealthRetries: 4,
        baseBackoffMs: 1200,
        // Proxy
        proxyConfiguration: { useApifyProxy: true, // consider RESIDENTIAL or specific country group(s)
            // apifyProxyGroups: ['RESIDENTIAL']
        },
        // Realistic Accept-* headers are built dynamically per-request
    };
    const input = { ...defaults, ...(raw || {}) };
    input.maxPages = +input.maxPages > 0 ? +input.maxPages : 1;
    input.maxConcurrency = +input.maxConcurrency > 0 ? +input.maxConcurrency : 3;
    input.maxItems = +input.maxItems > 0 ? +input.maxItems : 50;
    input.maxStealthRetries = Math.min(Math.max(+input.maxStealthRetries || 3, 1), 6);
    input.baseBackoffMs = Math.max(+input.baseBackoffMs || 1000, 300);
    return input;
}

// ───────────────────────────────────────────────────────────────
// 2) UA + CLIENT HINTS POOL (Oct 2025 realistic)
// ───────────────────────────────────────────────────────────────
const UA_PROFILES = [
    {
        ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        chBrands: '"Not.A/Brand";v="99", "Chromium";v="128", "Google Chrome";v="128"',
        chPlatform: '"Windows"',
        chMobile: '?0',
    },
    {
        ua: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
        // Safari does not send sec-ch-ua brands typically; we’ll omit brands for Safari-like requests
        chBrands: '',
        chPlatform: '"macOS"',
        chMobile: '?0',
    },
    {
        ua: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        chBrands: '"Not.A/Brand";v="99", "Chromium";v="128", "Google Chrome";v="128"',
        chPlatform: '"Linux"',
        chMobile: '?0',
    },
    {
        ua: 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1',
        chBrands: '',
        chPlatform: '"iOS"',
        chMobile: '?1',
    },
];

function randomProfile() {
    return UA_PROFILES[Math.floor(Math.random() * UA_PROFILES.length)];
}

// Build realistic CH headers coherent with UA
function buildHeaders({ profile, referer }) {
    const h = {
        'User-Agent': profile.ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Site': referer ? 'same-origin' : 'none',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        // No DNT, no sec-gpc – avoid bot signatures
    };
    if (referer) h['Referer'] = referer;

    // Only send CH when Chromium-like profiles
    if (profile.chBrands) {
        h['sec-ch-ua'] = profile.chBrands;
        h['sec-ch-ua-mobile'] = profile.chMobile;
        h['sec-ch-ua-platform'] = profile.chPlatform;
    }
    return h;
}

// ───────────────────────────────────────────────────────────────
// 3) URLS + HELPERS
// ───────────────────────────────────────────────────────────────
function buildSearchUrl({ searchQuery, location, page = 1, jobAge = 'all' }) {
    const base = 'https://jooble.org/SearchResult';
    const p = new URLSearchParams();
    if (searchQuery && String(searchQuery).trim()) p.set('ukw', String(searchQuery).trim());
    if (location && String(location).trim()) p.set('l', String(location).trim());
    if (page > 1) p.set('p', String(page));
    if (jobAge && jobAge !== 'all') p.set('date', String(jobAge));
    const qs = p.toString();
    return qs ? `${base}?${qs}` : base;
}

function rand(min, max) {
    return Math.random() * (max - min) + min;
}

async function humanPauseShort() {
    // small latency + think time
    await Actor.sleep(rand(120, 380));
}

async function humanPauseRead() {
    // reading time on page
    await Actor.sleep(rand(900, 1800));
}

function isCookieOrBotWall(htmlText) {
    const t = htmlText.toLowerCase();
    return /are you human|verify you are human|captcha|cloudflare|cookies? (required|consent)/i.test(t);
}

function isBlockedStatus(statusCode) {
    return statusCode === 403 || statusCode === 429;
}

function backoffDelay(baseMs, attempt) {
    // Exponential backoff with jitter
    const expo = baseMs * Math.pow(2, attempt);
    const jitter = rand(0.5, 1.3);
    return Math.min(expo * jitter, 15000); // cap 15s
}

function pickProxyUrl(proxyConfiguration, session) {
    if (!proxyConfiguration) return undefined;
    // Stick session id to proxy for consistency; rotate when session retires
    return proxyConfiguration.newUrl(session?.id);
}

// ───────────────────────────────────────────────────────────────
// 4) PARSERS
// ───────────────────────────────────────────────────────────────
function extractDetailLinks($, base = 'https://jooble.org') {
    const set = new Set();
    $('a[href*="/desc/"]').each((_, el) => {
        const href = $(el).attr('href');
        if (!href || !href.includes('/desc/')) return;
        const abs = href.startsWith('http') ? href : new URL(href, base).href;
        set.add(abs);
    });
    return [...set];
}

function extractJobDetail($, url) {
    const getFirst = (sels) => {
        for (const s of sels) {
            const txt = $(s).first().text().trim();
            if (txt) return txt;
        }
        return '';
    };
    const title = getFirst(['h1', '.job-title', '.title']);
    const company = getFirst(['.company', '.employer', '.company-name']);
    const location = getFirst(['.location', '.job-location']);
    const salary = getFirst(['.salary', '.pay', '.compensation']);
    const descNode = $('.job-description, .description, .vacancy-description, .content, main').first();
    const description_html = descNode.html() || '';
    const description_text = descNode.text().replace(/\s+/g, ' ').trim() || '';

    return {
        title,
        company,
        location,
        salary,
        description_html,
        description_text,
        job_url: url,
        scrapedAt: new Date().toISOString(),
        source: 'Jooble',
    };
}

// ───────────────────────────────────────────────────────────────
// 5) MAIN
// ───────────────────────────────────────────────────────────────
export async function main() {
    await Actor.init();
    let saved = 0;

    try {
        const input = await getInput();
        const rq = await RequestQueue.open();
        const proxyConfiguration = await Actor.createProxyConfiguration(input.proxyConfiguration);
        const sessionPool = await SessionPool.open({ maxPoolSize: 100 });

        // seed
        await rq.addRequest({
            url: buildSearchUrl({ searchQuery: input.searchQuery, location: input.location, page: 1, jobAge: input.jobAge }),
            userData: { label: 'SEARCH', page: 1, retries: 0, referer: 'https://jooble.org/' },
        });

        const crawler = new BasicCrawler({
            requestQueue: rq,
            useSessionPool: true,
            maxConcurrency: input.maxConcurrency,
            // BasicCrawler lets us fully control networking via gotScraping.
            async requestHandler({ request, session }) {
                if (saved >= input.maxItems) return;

                const label = request.userData?.label ?? 'SEARCH';
                const attempt = request.userData?.retries ?? 0;
                const referer = request.userData?.referer;
                const profile = randomProfile();
                const headers = buildHeaders({ profile, referer });

                const proxyUrl = pickProxyUrl(proxyConfiguration, session);

                await humanPauseShort(); // latency jitter pre-flight
                let response;
                try {
                    response = await gotScraping({
                        url: request.url,
                        method: 'GET',
                        headers,
                        proxyUrl,
                        timeout: { request: 20000 },
                        retry: { limit: 0 }, // our own retry strategy
                        http2: true,
                        decompress: true,
                        throwHttpErrors: false, // we inspect status ourselves
                    });
                } catch (e) {
                    // Network or TLS failure – re-enqueue with backoff
                    const delay = backoffDelay(input.baseBackoffMs, attempt);
                    log.warning(`🌐 Network error on ${request.url} (${e.message}); retry in ${Math.round(delay)}ms`);
                    session?.markBad();
                    session?.retire();
                    await Actor.sleep(delay);
                    await rq.addRequest({
                        url: request.url,
                        userData: { ...request.userData, retries: attempt + 1, // keep label/page
                            // New referer chain still okay
                        },
                        forefront: false,
                    });
                    return;
                }

                const { statusCode, body } = response;
                const bodyStr = typeof body === 'string' ? body : body?.toString?.('utf8') || String(body || '');

                // Block signals
                if (isBlockedStatus(statusCode) || isCookieOrBotWall(bodyStr)) {
                    if (attempt < input.maxStealthRetries) {
                        const delay = backoffDelay(input.baseBackoffMs, attempt);
                        log.warning(`🧱 Blocked (${statusCode}) or wall on ${request.url} – retry ${attempt + 1} in ${Math.round(delay)}ms`);
                        // rotate session + UA
                        session?.markBad();
                        session?.retire();
                        await Actor.sleep(delay);
                        await rq.addRequest({
                            url: request.url,
                            userData: {
                                ...request.userData,
                                retries: attempt + 1,
                                // keep referer chain
                            },
                            forefront: false,
                        });
                        return;
                    } else {
                        log.error(`❌ Giving up after ${attempt} retries: ${request.url}`);
                        return;
                    }
                }

                // Parse HTML
                const $ = cheerioLoad(bodyStr);

                if (label === 'SEARCH') {
                    const pageNo = request.userData?.page ?? 1;

                    // Realistic “reading time”
                    await humanPauseRead();

                    const links = extractDetailLinks($, request.url);
                    log.info(`🔎 Search p${pageNo}: found ${links.length} detail links`);

                    if (links.length) {
                        // enqueue details with natural referer chain + UA rotation on fetch
                        for (const detailUrl of links) {
                            await rq.addRequest({
                                url: detailUrl,
                                userData: {
                                    label: 'DETAIL',
                                    searchPage: pageNo,
                                    retries: 0,
                                    referer: request.url, // build a real referer chain
                                },
                            });
                        }
                    }

                    // Pagination (stop if limit reached)
                    if (pageNo < input.maxPages && saved < input.maxItems) {
                        const nextUrl = buildSearchUrl({
                            searchQuery: input.searchQuery,
                            location: input.location,
                            page: pageNo + 1,
                            jobAge: input.jobAge,
                        });
                        await rq.addRequest({
                            url: nextUrl,
                            userData: { label: 'SEARCH', page: pageNo + 1, retries: 0, referer: request.url },
                            forefront: false, // let details mix in for pacing
                        });
                        // human think time between pages
                        await humanPauseShort();
                    }

                    return;
                }

                if (label === 'DETAIL') {
                    // Realistic “reading time”
                    await humanPauseRead();

                    const item = extractJobDetail($, request.url);
                    if (item.title) {
                        await Dataset.pushData(item);
                        saved++;
                        log.info(`✅ Saved #${saved}: ${item.title}`);
                    } else {
                        log.warning(`⚠️ No title on detail: ${request.url}`);
                    }
                    return;
                }
            },

            failedRequestHandler: async ({ request, error }) => {
                log.error(`❌ Failed permanently ${request.url} – ${error?.message || error}`);
            },
        });

        await crawler.run();

        log.info(`🎉 Done. Saved ${saved} item(s).`);
    } catch (err) {
        log.error('❌ Unexpected error in main():', err);
        if (err?.stack) console.error(err.stack);
    } finally {
        await Actor.exit();
    }
}
