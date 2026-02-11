import { Actor } from 'apify';
import { log as crawleeLog, PlaywrightCrawler } from 'crawlee';

// ─── Types ────────────────────────────────────────────────────────────────────

interface StartUrl {
    url: string;
}

interface Input {
    startURLs?: Array<string | StartUrl>;
    startUrls?: Array<string | StartUrl>;
    search?: string;
    country?: string;
    adType?: string;
    activeStatus?: string;
    maxItems?: number;
    endPage?: number;
    proxyCountry?: string;
    proxy?: {
        useApifyProxy?: boolean;
        apifyProxyGroups?: string[];
        proxyUrls?: string[];
    };
    customData?: Record<string, unknown>;
}

interface AdSnapshot {
    cta_type?: string;
    link_url?: string;
    page_name?: string;
    page_id?: string;
    page_profile_uri?: string;
    page_like_count?: number;
    page_categories?: string[];
}

interface RawAd {
    adid?: string;
    adArchiveID?: string;
    pageID?: string;
    pageName?: string;
    pageCategories?: string[];
    publisherPlatform?: string[];
    snapshot?: AdSnapshot;
    startDate?: number;
    endDate?: number | null;
    // snake_case variants
    ad_archive_id?: string;
    page_id?: string;
    page_name?: string;
    publisher_platforms?: string[];
    publisher_platform?: string[];
    ad_delivery_start_time?: string;
    ad_delivery_stop_time?: string | null;
    start_date?: number;
    end_date?: number | null;
    start_date_formatted?: string;
    end_date_formatted?: string;
}

interface ProcessedAd {
    adArchiveID: string;
    pageName: string | null;
    pageID: string | null;
    pageProfileURI: string | null;
    pageCategories: string[];
    pageLikeCount: number | null;
    publisherPlatforms: string[];
    startDate: string | null;
    endDate: string | null;
    ctaType: string | null;
    linkURL: string | null;
    scrapedAt: string;
    customData: Record<string, unknown> | null;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

const AD_LIBRARY_BASE = 'https://www.facebook.com/ads/library/';


const AD_TYPE_MAP: Record<string, string> = {
    'ALL': 'all',
    'POLITICAL_AND_ISSUE_ADS': 'political_and_issue_ads',
    'HOUSING': 'housing',
    'CREDIT': 'credit',
    'EMPLOYMENT': 'employment',
    'ISSUE_ADS': 'political_and_issue_ads',
};

const ACTIVE_STATUS_MAP: Record<string, string> = {
    'ALL': 'all',
    'ACTIVE': 'active',
    'INACTIVE': 'inactive',
};

function epochToISO(epoch: number | null | undefined): string | null {
    if (!epoch) return null;
    return new Date(epoch * 1000).toISOString();
}

function processAd(raw: RawAd, customData: Record<string, unknown> | null = null): ProcessedAd | null {
    const id = raw.adArchiveID ?? raw.adid ?? raw.ad_archive_id;
    if (!id) return null;

    const snap = raw.snapshot ?? {};

    const platforms: string[] = (
        raw.publisherPlatform ??
        raw.publisher_platform ??
        raw.publisher_platforms ??
        []
    );

    const startDate = raw.startDate
        ? epochToISO(raw.startDate)
        : raw.start_date
            ? epochToISO(raw.start_date)
            : (raw.ad_delivery_start_time ?? raw.start_date_formatted ?? null);

    const endDate = raw.endDate != null
        ? epochToISO(raw.endDate)
        : raw.end_date != null
            ? epochToISO(raw.end_date)
            : (raw.ad_delivery_stop_time ?? raw.end_date_formatted ?? null);

    return {
        adArchiveID: String(id),
        pageName: snap.page_name ?? raw.pageName ?? raw.page_name ?? null,
        pageID: snap.page_id ? String(snap.page_id) : raw.pageID ? String(raw.pageID) : (raw.page_id ?? null),
        pageProfileURI: snap.page_profile_uri ?? null,
        pageCategories: snap.page_categories ?? raw.pageCategories ?? [],
        pageLikeCount: snap.page_like_count ?? null,
        publisherPlatforms: [...new Set(platforms)],
        startDate,
        endDate,
        ctaType: snap.cta_type ?? null,
        linkURL: snap.link_url ?? null,
        scrapedAt: new Date().toISOString(),
        customData,
    };
}

function findAdsInObject(obj: unknown, depth = 0): RawAd[] {
    if (depth > 15 || obj === null || typeof obj !== 'object') return [];
    const results: RawAd[] = [];
    if (Array.isArray(obj)) {
        for (const item of obj) results.push(...findAdsInObject(item, depth + 1));
        return results;
    }
    const record = obj as Record<string, unknown>;
    const hasId = (record['adArchiveID'] && Boolean(record['adArchiveID']))
        || (record['adid'] && Boolean(record['adid']))
        || (record['ad_archive_id'] && Boolean(record['ad_archive_id']));
    if (hasId) {
        results.push(record as unknown as RawAd);
        return results;
    }
    for (const value of Object.values(record)) {
        results.push(...findAdsInObject(value, depth + 1));
    }
    return results;
}

function parseAdsFromResponseText(text: string): RawAd[] {
    const ads: RawAd[] = [];
    // Facebook async endpoint wraps JSON in "for (;;);" anti-hijacking prefix
    const cleaned = text.replace(/^for\s*\(;;\);?\s*/, '').trim();
    const lines = cleaned.split('\n');
    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        let json: unknown;
        try { json = JSON.parse(trimmed); } catch { continue; }
        ads.push(...findAdsInObject(json));
    }
    return ads;
}

// ─── Token & Cookie extraction helpers ────────────────────────────────────────

interface FbTokens {
    cookies: string;
    dtsg: string;
    lsd: string;
    /** The exact proxy URL used by the browser — HTTP calls must use this same IP */
    proxyUrl: string | undefined;
    /** Captured async search response bodies from the browser's own network traffic */
    capturedSearchResponses: string[];
}

function cookieMapToHeader(map: Record<string, string>): string {
    return Object.entries(map).map(([k, v]) => `${k}=${v}`).join('; ');
}

function extractTokensFromHtml(html: string): { dtsg: string; lsd: string } {
    const dtsgMatch = html.match(/"token"\s*:\s*"(AQ[^"]{10,})"/)
        ?? html.match(/"dtsg"\s*[^}]*"token"\s*:\s*"([^"]+)"/)
        ?? html.match(/name="fb_dtsg"\s+value="([^"]+)"/)
        ?? html.match(/"DTSGInitialData"[^}]{0,200}"token":"([^"]+)"/)
        ?? html.match(/\["DTSGInitData",[^\]]*,"([^"]+)"/)
        ?? html.match(/fb_dtsg[^"]*"([^"]{20,})"/);
    const dtsg = dtsgMatch?.[1] ?? '';

    const lsdMatch = html.match(/"LSD"\s*,\s*\[\]\s*,\s*\{"token"\s*:\s*"([^"]+)"/)
        ?? html.match(/name="lsd"\s+value="([^"]+)"/)
        ?? html.match(/"token":"([A-Za-z0-9_\-]{8,20})"[^}]*"ttl"/)
        ?? html.match(/"lsd"\s*:\s*\{\s*"token"\s*:\s*"([^"]+)"/)
        ?? html.match(/\["LSD"[^\]]*"([A-Za-z0-9_\-]{6,20})"\]/);
    const lsd = lsdMatch?.[1] ?? '';

    return { dtsg, lsd };
}

/**
 * Uses a real Playwright browser to load the Ad Library page, solve any
 * __rd_verify challenge automatically (JS runs in the real browser), and
 * extract the cookies + tokens needed for subsequent HTTP-only API calls.
 *
 * This is the only place where a browser is used — all pagination requests
 * are done via HTTP (gotScraping), keeping cost low.
 */
async function fetchFbTokensViaBrowser(
    searchUrl: string,
    proxyConfiguration: import('apify').ProxyConfiguration | undefined,
    maxItems: number,
): Promise<FbTokens> {
    // Pin a single proxy URL so that both the browser warmup AND the subsequent
    // HTTP calls hit the exact same IP — the rd_challenge cookie is IP-bound.
    const pinnedProxyUrl = await proxyConfiguration?.newUrl('warmup_session') ?? undefined;
    crawleeLog.info(`Pinned proxy URL for this session: ${pinnedProxyUrl ? 'set' : 'none'}`);

    let result: FbTokens = { cookies: '', dtsg: '', lsd: '', proxyUrl: pinnedProxyUrl, capturedSearchResponses: [] };

    // Create a proxy configuration that always returns the same pinned URL
    const pinnedProxyConfig = pinnedProxyUrl
        ? await Actor.createProxyConfiguration({ proxyUrls: [pinnedProxyUrl] })
        : proxyConfiguration;

    const warmupCrawler = new PlaywrightCrawler({
        proxyConfiguration: pinnedProxyConfig,
        maxRequestsPerCrawl: 1,
        requestHandlerTimeoutSecs: 120,
        navigationTimeoutSecs: 60,
        maxSessionRotations: 0,   // don't rotate — we pinned a proxy IP
        sessionPoolOptions: { maxPoolSize: 1 },

        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--lang=en-US,en',
                ],
            },
            useChrome: true,
        },

        browserPoolOptions: {
            fingerprintOptions: {
                fingerprintGeneratorOptions: {
                    browsers: ['chrome'],
                    operatingSystems: ['windows', 'macos'],
                    locales: ['en-US'],
                },
            },
        },

        preNavigationHooks: [
            async ({ page, request }) => {
                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => false });
                });

                // Block heavy binary resources to reduce proxy data usage.
                // Stylesheets and scripts must pass through — Facebook needs them
                // to execute the __rd_verify challenge and render the ad feed.
                const BLOCKED_RESOURCE_TYPES = new Set(['image', 'media', 'font']);
                // Block third-party media CDNs (images/videos of ad creatives)
                const BLOCKED_URL_FRAGMENTS = [
                    'google-analytics.com', 'doubleclick.net',
                    'scontent.f', // fbcdn user-generated image/video CDN
                    'video.f',    // fbcdn video CDN
                ];
                await page.route('**/*', async (route) => {
                    const req = route.request();
                    if (BLOCKED_RESOURCE_TYPES.has(req.resourceType())) {
                        await route.abort();
                        return;
                    }
                    const url = req.url();
                    if (BLOCKED_URL_FRAGMENTS.some(f => url.includes(f))) {
                        await route.abort();
                        return;
                    }
                    await route.continue();
                });

                // Monitor the Ad Library page response status — just log, don't block.
                // We handle 403 (__rd_verify challenge) by waiting for the JS to reload the page.
                // Using route.fetch() caused deadlocks (second HTTP request times out while
                // the browser is still waiting for the original response).
                page.on('response', (resp) => {
                    const url = resp.url();
                    if (url.includes('/ads/library') && !url.includes('async') && !url.includes('graphql')) {
                        crawleeLog.info(`Ad Library page response: ${resp.status()} for ${url.slice(0, 80)}`);
                    }
                });

                // Passively capture async/search_ads responses the browser makes while rendering.
                // These are authentic AJAX responses with full ad data — no token replication needed.
                const capturedBodies: string[] = [];
                page.on('response', async (resp) => {
                    const url = resp.url();
                    if (!url.includes('async/search_ads') && !url.includes('api/graphql')) return;
                    if (resp.status() < 200 || resp.status() >= 300) return;
                    try {
                        const text = await resp.text();
                        if (text.includes('adArchiveID') || text.includes('ad_archive_id')) {
                            crawleeLog.info(`Captured response: ${text.length} chars from ${url.slice(0, 80)}`);
                            capturedBodies.push(text);
                        }
                    } catch { /* ignore */ }
                });
                request.userData['capturedBodies'] = capturedBodies;
            },
        ],

        async requestHandler({ page, log, request }) {
            log.info('Browser: page loaded, waiting for challenge resolution...');

            // The __rd_verify challenge JS runs automatically:
            //   fetch('/__rd_verify_...', { method: 'POST' }).finally(() => window.location.reload())
            // Wait for up to 3 cycles for the challenge to clear.
            for (let i = 0; i < 3; i++) {
                try {
                    await page.waitForLoadState('networkidle', { timeout: 20000 });
                } catch { /* continue */ }
                await page.waitForTimeout(2000);
                const html = await page.content();
                if (!html.includes('__rd_verify')) break;
                log.info(`Challenge still present after wait ${i + 1}, continuing...`);
            }

            const finalUrl = page.url();
            log.info(`Browser final URL: ${finalUrl}`);

            // Diagnostic: check cookies and page state
            const browserCookiesEarly = await page.context().cookies();
            const cookieNamesEarly = browserCookiesEarly.map(c => c.name).join(', ');
            log.info(`Cookies after challenge: ${cookieNamesEarly}`);

            const htmlAfterChallenge = await page.content();
            const hasVerify = htmlAfterChallenge.includes('__rd_verify');
            const hasLoginForm = htmlAfterChallenge.includes('login') || htmlAfterChallenge.includes('Log in');
            const hasAdsLib = htmlAfterChallenge.includes('AdsLibrary') || htmlAfterChallenge.includes('ads/library');
            log.info(`Page state: hasVerify=${hasVerify}, hasLoginForm=${hasLoginForm}, hasAdsLib=${hasAdsLib}, size=${htmlAfterChallenge.length}`);

            // Wait for the ad feed to mount — up to 30 seconds.
            // Facebook's React app can take a while to hydrate after the challenge clears.
            const FEED_SELECTORS = [
                '[data-pagelet="AdsLibrary"]',
                '[data-testid="ads-library-results"]',
                '._8nfl',
                '[class*="x1qhmfi1"]',   // common Ads Library result container class
                'div[role="feed"]',
            ];
            let feedMounted = false;
            for (const selector of FEED_SELECTORS) {
                try {
                    await page.waitForSelector(selector, { timeout: 30000 });
                    log.info(`Feed mounted — found selector: ${selector}`);
                    feedMounted = true;
                    break;
                } catch { /* try next */ }
            }

            if (!feedMounted) {
                // Log what we actually see for debugging
                const bodySnippet = await page.evaluate((): string => document.body.innerText.slice(0, 500).replace(/\n/g, ' '));
                log.warning(`Feed did NOT mount after 30s. Body snippet: "${bodySnippet}"`);
                // Save screenshot for debugging
                try {
                    const screenshotBuf = await page.screenshot({ fullPage: false });
                    await Actor.setValue('debug_screenshot', screenshotBuf, { contentType: 'image/png' });
                    log.info('Debug screenshot saved to key-value store as "debug_screenshot"');
                } catch { /* ignore */ }
            }

            // Scroll repeatedly to trigger lazy-loaded ad batches.
            // Each scroll to the bottom causes the React feed to fetch the next
            // async/search_ads page — we capture those responses via page.on('response').
            const targetItems = (request.userData['maxItems'] as number) ?? 100;
            const capturedBodies: string[] = (request.userData['capturedBodies'] as string[]) ?? [];

            let lastCapturedCount = 0;
            let noNewResponseRounds = 0;
            const MAX_NO_NEW_ROUNDS = 3; // stop if 3 consecutive scrolls yield nothing new

            for (let scrollRound = 0; scrollRound < 20; scrollRound++) {
                // Count ads captured so far (rough estimate: ~30 per response)
                const estimatedAds = capturedBodies.length * 30;
                if (targetItems > 0 && estimatedAds >= targetItems) break;

                await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
                try { await page.waitForLoadState('networkidle', { timeout: 8000 }); } catch { /* ok */ }
                await page.waitForTimeout(800);

                if (capturedBodies.length === lastCapturedCount) {
                    noNewResponseRounds++;
                    if (noNewResponseRounds >= MAX_NO_NEW_ROUNDS) {
                        log.info(`No new responses after ${MAX_NO_NEW_ROUNDS} scroll rounds — stopping`);
                        break;
                    }
                } else {
                    noNewResponseRounds = 0;
                    lastCapturedCount = capturedBodies.length;
                    log.info(`Scroll round ${scrollRound + 1}: ${capturedBodies.length} response(s) captured so far`);
                }
            }

            log.info(`Captured ${capturedBodies.length} search response(s) from browser network`);

            const html = await page.content();
            log.info(`Browser page size: ${html.length} chars`);

            // Extract cookies
            const browserCookies = await page.context().cookies();
            const cookieMap: Record<string, string> = {};
            for (const c of browserCookies) cookieMap[c.name] = c.value;
            const cookieHeader = cookieMapToHeader(cookieMap);
            log.info(`Browser cookies: ${Object.keys(cookieMap).join(', ')}`);

            const { lsd } = extractTokensFromHtml(html);
            log.info(`lsd: ${lsd ? 'OK' : 'MISSING'}`);

            result = { cookies: cookieHeader, dtsg: '', lsd, proxyUrl: pinnedProxyUrl, capturedSearchResponses: capturedBodies };
        },

        // Facebook returns 403 for the __rd_verify challenge page — don't treat as failure.
        // The browser still executes the inline JS which solves the challenge and reloads.
        errorHandler({ request, log }, error) {
            log.warning(`Request error (may be 403 challenge — continuing): ${error.message}`);
            request.noRetry = true; // don't retry — let the requestHandler handle it
        },

        failedRequestHandler({ log }, error) {
            log.error(`Browser warmup failed: ${error.message}`);
        },
    });

    await warmupCrawler.run([{ url: searchUrl, userData: { maxItems } }]);
    return result;
}

// ─── Search params type ────────────────────────────────────────────────────────

interface SearchParams {
    search: string;
    country: string;
    adType: string;
    activeStatus: string;
}

// ─── URL parsing helpers ───────────────────────────────────────────────────────

const SESSION_PARAMS = ['search_run_id', 'sort_data', 'is_targeted_country'];

function cleanAdLibraryUrl(rawUrl: string): string {
    try {
        const url = new URL(rawUrl);
        for (const param of SESSION_PARAMS) {
            for (const key of [...url.searchParams.keys()]) {
                if (key === param || key.startsWith(`${param}[`)) {
                    url.searchParams.delete(key);
                }
            }
        }
        return url.toString();
    } catch {
        return rawUrl;
    }
}

function normaliseUrls(raw: Array<string | StartUrl> | undefined): string[] {
    if (!raw || raw.length === 0) return [];
    return raw
        .map((item) => (typeof item === 'string' ? item : item.url))
        .filter(Boolean)
        .map(cleanAdLibraryUrl);
}

function urlToSearchParams(url: string): SearchParams | null {
    try {
        const u = new URL(url);
        const q = u.searchParams.get('q');
        const country = u.searchParams.get('country');
        if (!q || !country) return null;

        // Reverse-map Facebook URL values back to our enum keys
        const adTypeRaw = u.searchParams.get('ad_type') ?? 'all';
        const activeStatusRaw = u.searchParams.get('active_status') ?? 'all';

        const adType = Object.entries(AD_TYPE_MAP).find(([, v]) => v === adTypeRaw)?.[0] ?? 'ALL';
        const activeStatus = Object.entries(ACTIVE_STATUS_MAP).find(([, v]) => v === activeStatusRaw)?.[0] ?? 'ALL';

        return { search: q, country, adType, activeStatus };
    } catch {
        return null;
    }
}

function buildSearchURL(params: {
    search?: string;
    country?: string;
    adType?: string;
    activeStatus?: string;
}): string {
    const url = new URL(AD_LIBRARY_BASE);
    const adTypeRaw = (params.adType ?? 'ALL').toUpperCase();
    const activeStatusRaw = (params.activeStatus ?? 'ALL').toUpperCase();
    url.searchParams.set('active_status', ACTIVE_STATUS_MAP[activeStatusRaw] ?? 'all');
    url.searchParams.set('ad_type', AD_TYPE_MAP[adTypeRaw] ?? 'all');
    url.searchParams.set('country', (params.country ?? 'BR').toUpperCase());
    url.searchParams.set('media_type', 'all');
    if (params.search) {
        let searchVal = params.search;
        try { searchVal = decodeURIComponent(params.search); } catch { /* keep as-is */ }
        url.searchParams.set('q', searchVal);
    }
    return url.toString();
}

// ─── Entry point ──────────────────────────────────────────────────────────────

await Actor.init();

const input = (await Actor.getInput<Input>()) ?? {};

const {
    search,
    country = 'BR',
    adType = 'ALL',
    activeStatus = 'ALL',
    maxItems = 100,
    endPage = 0,
    proxyCountry,
    proxy,
    customData = null,
} = input;

// Build URL list
const startURLs = normaliseUrls(input.startURLs ?? input.startUrls);

if (startURLs.length === 0 && !search) {
    throw new Error('Provide either "startUrls" or "search" (+ country + adType) in the input.');
}

const initialURLs: string[] =
    startURLs.length > 0
        ? startURLs
        : [buildSearchURL({ search, country, adType, activeStatus })];

crawleeLog.info(`Starting scrape for ${initialURLs.length} URL(s). maxItems=${maxItems}, endPage=${endPage}`);

// Build proxy configuration — used by the browser warmup to pin a session IP
let proxyConfiguration: import('apify').ProxyConfiguration | undefined;

if (proxy?.useApifyProxy !== false) {
    const groups = proxy?.apifyProxyGroups ?? ['RESIDENTIAL'];
    const proxyCountryCode = proxyCountry?.toUpperCase() || undefined;
    proxyConfiguration = await Actor.createProxyConfiguration({
        groups,
        ...(proxyCountryCode ? { countryCode: proxyCountryCode } : {}),
    });
    crawleeLog.info(`Using Apify proxy group(s): ${JSON.stringify(groups)}, country: ${proxyCountryCode ?? 'auto'}`);
} else if (proxy?.proxyUrls && proxy.proxyUrls.length > 0) {
    proxyConfiguration = await Actor.createProxyConfiguration({
        proxyUrls: proxy.proxyUrls,
    });
    crawleeLog.info(`Using custom proxy URLs`);
} else {
    crawleeLog.warning('No proxy configured — Facebook will likely block datacenter IPs!');
}

let totalScraped = 0;
const globalSeenIds = new Set<string>();

for (const sourceURL of initialURLs) {
    crawleeLog.info(`Processing URL: ${sourceURL}`);

    // Determine search params from URL or input
    let searchParams: SearchParams | null = urlToSearchParams(sourceURL);
    if (!searchParams) {
        if (!search) {
            crawleeLog.warning(`Cannot extract search params from URL and no search input provided: ${sourceURL}`);
            continue;
        }
        searchParams = {
            search,
            country,
            adType,
            activeStatus,
        };
    }

    crawleeLog.info(`Search params: ${JSON.stringify(searchParams)}`);

    // Step 1: Use Playwright browser to solve __rd_verify challenge and fetch all
    // ad pages via browser's own fetch (page.evaluate) — no IP/cookie mismatch issues.
    const remainingForUrl = maxItems > 0 ? maxItems - totalScraped : 0;
    let tokens: FbTokens;
    try {
        tokens = await fetchFbTokensViaBrowser(sourceURL, proxyConfiguration, remainingForUrl);
    } catch (err) {
        crawleeLog.error(`Failed to fetch tokens from ${sourceURL}: ${(err as Error).message}`);
        continue;
    }

    if (!tokens.cookies) {
        crawleeLog.warning(`No cookies obtained for ${sourceURL} — skipping`);
        continue;
    }

    // Step 2a: Process ads already captured from the browser's own network traffic.
    // These are perfectly valid responses — the browser made the async/search_ads
    // calls naturally while loading the page.
    const ads: ProcessedAd[] = [];
    const seenIds = new Set<string>();

    crawleeLog.info(`Processing ${tokens.capturedSearchResponses.length} captured browser response(s)...`);
    for (const body of tokens.capturedSearchResponses) {
        const rawAds = parseAdsFromResponseText(body);
        crawleeLog.info(`  → ${rawAds.length} ads found in captured response`);
        for (const raw of rawAds) {
            if (maxItems > 0 && ads.length >= maxItems) break;
            const processed = processAd(raw, customData);
            if (!processed) continue;
            if (seenIds.has(processed.adArchiveID)) continue;
            seenIds.add(processed.adArchiveID);
            ads.push(processed);
        }
    }

    crawleeLog.info(`Total ads for URL: ${ads.length} (all fetched via browser in-page fetch)`);

    // Step 3: Push to dataset
    for (const ad of ads) {
        if (globalSeenIds.has(ad.adArchiveID)) continue;
        globalSeenIds.add(ad.adArchiveID);
        await Actor.pushData(ad);
        totalScraped++;
        if (maxItems > 0 && totalScraped >= maxItems) break;
    }

    if (maxItems > 0 && totalScraped >= maxItems) {
        crawleeLog.info(`Reached maxItems (${maxItems}), stopping.`);
        break;
    }
}

crawleeLog.info(`Scraping complete. Total ads scraped: ${totalScraped}`);

await Actor.exit();
