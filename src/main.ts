import { Actor, ProxyConfiguration } from 'apify';
import { PlaywrightCrawler, log as crawleeLog } from 'crawlee';
import type { Page, Response } from 'playwright';

// ─── Types ────────────────────────────────────────────────────────────────────

interface StartUrl {
    url: string;
}

interface Input {
    // Accept both camelCase variants and array-of-objects format (Apify default)
    startURLs?: Array<string | StartUrl>;
    startUrls?: Array<string | StartUrl>;
    search?: string;
    country?: string;
    adType?: string;
    activeStatus?: string;
    maxItems?: number;
    endPage?: number;
    proxy?: {
        useApifyProxy?: boolean;
        apifyProxyGroups?: string[];
        proxyUrls?: string[];
    };
}

interface SpendRange {
    lower_bound?: string | number;
    upper_bound?: string | number;
}

interface DemographicEntry {
    age?: string;
    gender?: string;
    percentage?: string | number;
}

interface RegionEntry {
    region?: string;
    percentage?: string | number;
}

interface AdSnapshot {
    body?: { markup?: { __html?: string } } | string;
    title?: string;
    link_url?: string;
    link_description?: string;
    link_caption?: string;
    images?: Array<{ original_image_url?: string; resized_image_url?: string }>;
    videos?: Array<{ video_hd_url?: string; video_sd_url?: string; video_preview_image_url?: string }>;
    cards?: Array<{
        body?: string;
        title?: string;
        link_url?: string;
        caption?: string;
        resized_image_url?: string;
        original_image_url?: string;
        video_hd_url?: string;
        video_sd_url?: string;
    }>;
    cta_type?: string;
    cta_text?: string;
}

interface RawAd {
    adid?: string;
    adArchiveID?: string;
    archiveTypes?: string[];
    categories?: number[];
    collationCount?: number;
    collationID?: string;
    currency?: string;
    endDate?: number | null;
    entityType?: string;
    instagramActorName?: string | null;
    isActive?: boolean;
    isProfilePage?: boolean;
    pageID?: string;
    pageName?: string;
    pageIsDeleted?: boolean;
    pageProfilePictureURL?: string;
    pageCategories?: string[];
    publisherPlatform?: string[];
    snapshot?: AdSnapshot;
    startDate?: number;
    spend?: SpendRange;
    impressions?: SpendRange;
    demographicDistribution?: DemographicEntry[];
    regionDistribution?: RegionEntry[];
    // Raw GraphQL response fields
    ad_archive_id?: string;
    page_id?: string;
    page_name?: string;
    ad_delivery_start_time?: string;
    ad_delivery_stop_time?: string | null;
    ad_snapshot_url?: string;
    ad_creative_bodies?: string[];
    ad_creative_link_titles?: string[];
    ad_creative_link_descriptions?: string[];
    publisher_platforms?: string[];
    estimated_audience_size?: SpendRange;
}

interface ProcessedAd {
    adArchiveID: string;
    pageName: string | null;
    pageID: string | null;
    entityType: string | null;
    startDate: string | null;
    endDate: string | null;
    isActive: boolean;
    currency: string | null;
    spend: SpendRange | null;
    impressions: SpendRange | null;
    estimatedAudienceSize: SpendRange | null;
    publisherPlatforms: string[];
    snapshot: string | null;
    categories: number[];
    collationCount: number;
    instagramActorName: string | null;
    pageIsDeleted: boolean;
    pageProfilePictureURL: string | null;
    pageCategories: string[];
    adCreativeBodies: string[];
    adCreativeLinkTitles: string[];
    adCreativeLinkDescriptions: string[];
    adCreativeLinkCaptions: string[];
    adCreativeImages: string[];
    adCreativeVideos: string[];
    demographicDistribution: DemographicEntry[];
    regionDistribution: RegionEntry[];
    scrapedAt: string;
    sourceURL: string;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

const AD_LIBRARY_BASE = 'https://www.facebook.com/ads/library/';
const GRAPHQL_URL_PATTERN = 'facebook.com/api/graphql';

// Map input schema values to Facebook URL parameter values
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
        // Decode first to avoid double-encoding if the value arrived already percent-encoded
        let searchVal = params.search;
        try { searchVal = decodeURIComponent(params.search); } catch { /* keep as-is */ }
        url.searchParams.set('q', searchVal);
    }
    return url.toString();
}

function epochToISO(epoch: number | null | undefined): string | null {
    if (!epoch) return null;
    return new Date(epoch * 1000).toISOString();
}

function extractText(body: AdSnapshot['body']): string {
    if (!body) return '';
    if (typeof body === 'string') return body;
    return body?.markup?.__html?.replace(/<[^>]+>/g, '') ?? '';
}

function processAd(raw: RawAd, sourceURL: string): ProcessedAd | null {
    const id = raw.adArchiveID ?? raw.adid ?? raw.ad_archive_id;
    if (!id) return null;

    const snapshot = raw.snapshot ?? {};
    const images: string[] = [];
    const videos: string[] = [];

    // Extract images
    if (snapshot.images) {
        for (const img of snapshot.images) {
            const url = img.original_image_url ?? img.resized_image_url;
            if (url) images.push(url);
        }
    }
    // Extract videos
    if (snapshot.videos) {
        for (const vid of snapshot.videos) {
            const url = vid.video_hd_url ?? vid.video_sd_url;
            if (url) videos.push(url);
        }
    }
    // Extract from carousel cards
    if (snapshot.cards) {
        for (const card of snapshot.cards) {
            const img = card.original_image_url ?? card.resized_image_url;
            if (img) images.push(img);
            const vid = card.video_hd_url ?? card.video_sd_url;
            if (vid) videos.push(vid);
        }
    }

    const bodies: string[] = [];
    const bodyText = extractText(snapshot.body);
    if (bodyText) bodies.push(bodyText);
    if (raw.ad_creative_bodies) bodies.push(...raw.ad_creative_bodies);

    const linkTitles: string[] = [];
    if (snapshot.title) linkTitles.push(snapshot.title);
    if (raw.ad_creative_link_titles) linkTitles.push(...raw.ad_creative_link_titles);

    const linkDescriptions: string[] = [];
    if (snapshot.link_description) linkDescriptions.push(snapshot.link_description);
    if (raw.ad_creative_link_descriptions) linkDescriptions.push(...raw.ad_creative_link_descriptions);

    const linkCaptions: string[] = [];
    if (snapshot.link_caption) linkCaptions.push(snapshot.link_caption);

    const platforms = raw.publisherPlatform ?? raw.publisher_platforms ?? [];
    const snapshotUrl = raw.ad_snapshot_url ?? null;

    return {
        adArchiveID: String(id),
        pageName: raw.pageName ?? raw.page_name ?? null,
        pageID: raw.pageID ? String(raw.pageID) : (raw.page_id ?? null),
        entityType: raw.entityType ?? null,
        startDate: raw.startDate ? epochToISO(raw.startDate) : (raw.ad_delivery_start_time ?? null),
        endDate: raw.endDate ? epochToISO(raw.endDate) : (raw.ad_delivery_stop_time ?? null),
        isActive: raw.isActive ?? false,
        currency: raw.currency ?? null,
        spend: raw.spend ?? null,
        impressions: raw.impressions ?? null,
        estimatedAudienceSize: raw.estimated_audience_size ?? null,
        publisherPlatforms: platforms,
        snapshot: snapshotUrl,
        categories: raw.categories ?? [],
        collationCount: raw.collationCount ?? 0,
        instagramActorName: raw.instagramActorName ?? null,
        pageIsDeleted: raw.pageIsDeleted ?? false,
        pageProfilePictureURL: raw.pageProfilePictureURL ?? null,
        pageCategories: raw.pageCategories ?? [],
        adCreativeBodies: [...new Set(bodies)],
        adCreativeLinkTitles: [...new Set(linkTitles)],
        adCreativeLinkDescriptions: [...new Set(linkDescriptions)],
        adCreativeLinkCaptions: [...new Set(linkCaptions)],
        adCreativeImages: [...new Set(images)],
        adCreativeVideos: [...new Set(videos)],
        demographicDistribution: raw.demographicDistribution ?? [],
        regionDistribution: raw.regionDistribution ?? [],
        scrapedAt: new Date().toISOString(),
        sourceURL,
    };
}

/**
 * Tries to extract ads from a GraphQL response body.
 * Facebook sends several GraphQL responses - we try to find the one with ad data.
 */
function extractAdsFromGraphQL(text: string): RawAd[] {
    const ads: RawAd[] = [];
    try {
        // Facebook sometimes sends multiple JSON objects separated by newlines
        const lines = text.split('\n');
        for (const line of lines) {
            if (!line.trim()) continue;
            let json: Record<string, unknown>;
            try {
                json = JSON.parse(line);
            } catch {
                continue;
            }
            // Walk the JSON tree looking for ad-like objects
            const found = findAdsInObject(json);
            ads.push(...found);
        }
    } catch {
        // Ignore parse errors
    }
    return ads;
}

function isAdObject(obj: Record<string, unknown>): boolean {
    return (
        typeof obj === 'object' &&
        obj !== null &&
        (('adArchiveID' in obj && Boolean(obj.adArchiveID)) ||
            ('adid' in obj && Boolean(obj.adid)) ||
            ('ad_archive_id' in obj && Boolean(obj.ad_archive_id)))
    );
}

function findAdsInObject(obj: unknown, depth = 0): RawAd[] {
    if (depth > 15 || obj === null || typeof obj !== 'object') return [];
    const results: RawAd[] = [];

    if (Array.isArray(obj)) {
        for (const item of obj) {
            results.push(...findAdsInObject(item, depth + 1));
        }
        return results;
    }

    const record = obj as Record<string, unknown>;
    if (isAdObject(record)) {
        results.push(record as unknown as RawAd);
        return results;
    }

    for (const value of Object.values(record)) {
        results.push(...findAdsInObject(value, depth + 1));
    }
    return results;
}

/**
 * Extracts ads from the page HTML.
 * Facebook embeds ad data in inline <script> tags using require() / __d() patterns.
 * We grab the full HTML and search for JSON chunks containing ad archive IDs.
 */
async function extractAdsFromDOM(page: Page, sourceURL: string): Promise<ProcessedAd[]> {
    const ads: ProcessedAd[] = [];
    try {
        const html = await page.content();
        if (!html.includes('adArchiveID') && !html.includes('ad_archive_id')) return ads;

        // Extract all JSON-like blobs from the HTML.
        // Facebook wraps data in: require("TimeSliceImpl").collectDataForCapture(...)
        // or: __d("...",[],(function(...){})); or plain JSON objects in <script> tags.
        // Strategy: find all occurrences of adArchiveID and extract surrounding JSON object.
        const marker = '"adArchiveID"';
        let pos = 0;
        while (pos < html.length) {
            const idx = html.indexOf(marker, pos);
            if (idx === -1) break;
            pos = idx + marker.length;

            // Walk back to find the opening '{' of the object containing this key
            let depth = 0;
            let start = -1;
            for (let i = idx; i >= Math.max(0, idx - 5000); i--) {
                if (html[i] === '}') depth++;
                else if (html[i] === '{') {
                    if (depth === 0) { start = i; break; }
                    depth--;
                }
            }
            if (start === -1) continue;

            // Walk forward to find the matching closing '}'
            depth = 0;
            let end = -1;
            for (let i = start; i < Math.min(html.length, start + 50000); i++) {
                if (html[i] === '{') depth++;
                else if (html[i] === '}') {
                    depth--;
                    if (depth === 0) { end = i; break; }
                }
            }
            if (end === -1) continue;

            const chunk = html.slice(start, end + 1);
            try {
                const parsed = JSON.parse(chunk);
                if (isAdObject(parsed as Record<string, unknown>)) {
                    const processed = processAd(parsed as unknown as RawAd, sourceURL);
                    if (processed && !ads.find(a => a.adArchiveID === processed.adArchiveID)) {
                        ads.push(processed);
                    }
                }
            } catch {
                // chunk is not valid standalone JSON — skip
            }
        }
    } catch {
        // DOM fallback silently fails
    }
    return ads;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

await Actor.init();

const input = (await Actor.getInput<Input>()) ?? {};

const {
    search,
    country = 'BR',
    adType = 'ALL',
    activeStatus = 'ALL',
    maxItems = 100,
    endPage = 0,
    proxy,
} = input;

// Session-specific params that vary per browser/session and should be stripped
const SESSION_PARAMS = ['search_run_id', 'sort_data', 'is_targeted_country'];

function cleanAdLibraryUrl(rawUrl: string): string {
    try {
        const url = new URL(rawUrl);
        for (const param of SESSION_PARAMS) {
            // Remove exact match and bracket variants like sort_data[mode]
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

// Normalise startUrls: accept both key names and both string[] and {url}[] formats
function normaliseUrls(raw: Array<string | StartUrl> | undefined): string[] {
    if (!raw || raw.length === 0) return [];
    return raw
        .map((item) => (typeof item === 'string' ? item : item.url))
        .filter(Boolean)
        .map(cleanAdLibraryUrl);
}

const startURLs = normaliseUrls(input.startURLs ?? input.startUrls);

if (startURLs.length === 0 && !search) {
    throw new Error('Provide either "startUrls" or "search" (+ country + adType) in the input.');
}

// Build initial request list
const initialURLs: string[] =
    startURLs.length > 0
        ? startURLs
        : [buildSearchURL({ search, country, adType, activeStatus })];

crawleeLog.info(`Starting scrape for ${initialURLs.length} URL(s). maxItems=${maxItems}, endPage=${endPage}`);
crawleeLog.info(`Initial URLs: ${JSON.stringify(initialURLs)}`);

// Proxy setup
let proxyConfiguration: ProxyConfiguration | undefined;
if (proxy?.useApifyProxy !== false) {
    proxyConfiguration = await Actor.createProxyConfiguration({
        groups: proxy?.apifyProxyGroups ?? ['RESIDENTIAL'],
        countryCode: country.toUpperCase(),
    });
} else if (proxy?.proxyUrls && proxy.proxyUrls.length > 0) {
    proxyConfiguration = await Actor.createProxyConfiguration({
        proxyUrls: proxy.proxyUrls,
    });
}

let totalScraped = 0;
const seenAdIDs = new Set<string>();

const crawler = new PlaywrightCrawler({
    proxyConfiguration,
    maxRequestsPerCrawl: endPage > 0 ? endPage * initialURLs.length + initialURLs.length : undefined,
    requestHandlerTimeoutSecs: 120,
    navigationTimeoutSecs: 90,

    maxSessionRotations: 10,
    sessionPoolOptions: {
        maxPoolSize: 20,
        sessionOptions: { maxUsageCount: 3 },
    },

    // Anti-detection settings
    launchContext: {
        launchOptions: {
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-features=IsolateOrigins,site-per-process',
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
            // Override automation-detection properties
            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                delete (window as any).cdc_adoQpoasnfa76pfcZLmcfl_Array;
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                delete (window as any).cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                delete (window as any).cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            });
            await page.setExtraHTTPHeaders({
                'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'Upgrade-Insecure-Requests': '1',
            });
            // Intercept the main document response: if Facebook returns 403, fulfill
            // with status 200 so crawlee's block-detection does not abort the request.
            await page.route('**/ads/library/**', async (route) => {
                const response = await route.fetch();
                if (response.status() === 403) {
                    await route.fulfill({
                        status: 200,
                        headers: Object.fromEntries(Object.entries(response.headers())),
                        body: await response.body(),
                    });
                } else {
                    await route.continue();
                }
            });

            // Register the GraphQL response collector BEFORE navigation so we
            // don't miss responses that fire during/right after page load.
            const collectedAds: RawAd[] = [];
            const seenNetworkUrls: string[] = [];
            const responseHandler = async (response: Response) => {
                const url = response.url();
                const status = response.status();
                // Log ALL network requests for diagnosis (first 30)
                if (seenNetworkUrls.length < 30) {
                    seenNetworkUrls.push(`${status} ${url.slice(0, 120)}`);
                }
                if (!url.includes(GRAPHQL_URL_PATTERN)) return;
                if (status < 200 || status >= 300) return;
                try {
                    const text = await response.text();
                    // Log every GraphQL hit regardless of content, for diagnosis
                    if (seenNetworkUrls.length < 50 && text.length > 10) {
                        seenNetworkUrls.push(`GQL[${status}] len=${text.length} has_ad=${text.includes('adArchiveID') || text.includes('ad_archive_id')}`);
                    }
                    if (!text.includes('adArchiveID') && !text.includes('ad_archive_id')) return;
                    const found = extractAdsFromGraphQL(text);
                    if (found.length > 0) {
                        collectedAds.push(...found);
                    }
                } catch {
                    // response body may have been consumed already
                }
            };
            page.on('response', responseHandler);

            // Store collector on request userData so requestHandler can access it
            request.userData['collectedAds'] = collectedAds;
            request.userData['responseHandler'] = responseHandler;
            request.userData['seenNetworkUrls'] = seenNetworkUrls;

            // Random delay before navigation to appear more human
            await page.waitForTimeout(1000 + Math.floor(Math.random() * 2000));
        },
    ],

    async requestHandler({ request, page, log, session }) {
        const sourceURL = request.url;
        log.info(`Processing URL: ${sourceURL}`);

        // Check if we got a 403 page (our route interceptor converted it to 200)
        const pageTitle = await page.title().catch(() => '');
        const pageUrl = page.url();
        if (pageUrl.includes('login') || pageTitle.toLowerCase().includes('log in') || pageTitle === '') {
            log.warning(`Possible redirect to login page or empty page — retiring session`);
            session?.retire();
            throw new Error('Blocked: redirected to login or empty page');
        }

        // Retrieve the ad collector registered in preNavigationHook
        const collectedAds: RawAd[] = (request.userData['collectedAds'] as RawAd[]) ?? [];
        const responseHandler = request.userData['responseHandler'] as ((r: Response) => Promise<void>) | undefined;

        // Facebook sometimes does a /rd_verify challenge that strips URL params.
        // If the current URL lost its query params, navigate back to the original URL.
        const currentUrl = page.url();
        const hasQueryParams = currentUrl.includes('q=') || currentUrl.includes('search_type=');
        if (!hasQueryParams && sourceURL.includes('q=')) {
            log.info('URL params lost after challenge redirect — re-navigating to original URL');
            if (responseHandler) page.off('response', responseHandler);
            // Re-register collector for the second navigation
            const collectedAds2: RawAd[] = [];
            const responseHandler2 = async (response: Response) => {
                if (!response.url().includes(GRAPHQL_URL_PATTERN)) return;
                if (response.status() < 200 || response.status() >= 300) return;
                try {
                    const text = await response.text();
                    if (!text.includes('adArchiveID') && !text.includes('ad_archive_id')) return;
                    const found = extractAdsFromGraphQL(text);
                    if (found.length > 0) collectedAds2.push(...found);
                } catch { /* consumed */ }
            };
            page.on('response', responseHandler2);
            await page.goto(sourceURL, { waitUntil: 'domcontentloaded', timeout: 60000 });
            await page.waitForTimeout(6000);
            page.off('response', responseHandler2);
            collectedAds.push(...collectedAds2);
        } else {
            // Wait for ad cards to appear in the DOM, or fall back to networkidle
            const adSelectors = [
                '[data-testid="ad-archive-item"]',
                'div[class*="x8gbvx8"]',       // FB ad card container class pattern
                'div[class*="_7jyr"]',           // legacy FB ad card
                'div[aria-label*="Ad"]',
                'div[role="article"]',
            ];
            let adsAppeared = false;
            for (const sel of adSelectors) {
                try {
                    await page.waitForSelector(sel, { timeout: 15000 });
                    log.info(`Ad selector found: ${sel}`);
                    adsAppeared = true;
                    break;
                } catch { /* try next */ }
            }
            if (!adsAppeared) {
                log.warning('No ad selector appeared — waiting for networkidle as fallback');
                try {
                    await page.waitForLoadState('networkidle', { timeout: 20000 });
                } catch { /* ignore */ }
            }
            // Scroll down to trigger lazy-loaded GraphQL requests for more ads
            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight / 2));
            await page.waitForTimeout(2000);
            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
            await page.waitForTimeout(3000);
            if (responseHandler) page.off('response', responseHandler);
        }

        const seenNetworkUrls = (request.userData['seenNetworkUrls'] as string[]) ?? [];
        log.info(`Page title: "${pageTitle}" | URL after load: ${page.url()}`);
        log.info(`GraphQL ads collected via network: ${collectedAds.length}`);
        log.info(`GraphQL responses seen: ${JSON.stringify(seenNetworkUrls)}`);
        if (collectedAds.length === 0) {
            const fullText = await page.evaluate(() => document.body?.innerText?.slice(0, 800) ?? '').catch(() => '');
            log.info(`Full page text (800 chars): ${fullText.replace(/\n+/g, ' ')}`);
        }

        // If network interception found no ads, try DOM extraction
        if (collectedAds.length === 0) {
            log.warning('No ads found via network interception, trying DOM fallback...');
            const domAds = await extractAdsFromDOM(page, sourceURL);
            if (domAds.length > 0) {
                log.info(`DOM fallback found ${domAds.length} ads`);
                for (const ad of domAds) {
                    if (seenAdIDs.has(ad.adArchiveID)) continue;
                    seenAdIDs.add(ad.adArchiveID);
                    await Actor.pushData(ad);
                    totalScraped++;
                    if (maxItems > 0 && totalScraped >= maxItems) break;
                }
                return;
            }
        }

        // Process and save intercepted ads
        for (const raw of collectedAds) {
            if (maxItems > 0 && totalScraped >= maxItems) break;
            const processed = processAd(raw, sourceURL);
            if (!processed) continue;
            if (seenAdIDs.has(processed.adArchiveID)) continue;
            seenAdIDs.add(processed.adArchiveID);
            await Actor.pushData(processed);
            totalScraped++;
        }

        log.info(`Total scraped so far: ${totalScraped}`);

        if (maxItems > 0 && totalScraped >= maxItems) {
            log.info(`Reached maxItems (${maxItems}), stopping.`);
            return;
        }

        // Pagination: try to find and click "See more ads" / "Load more" button
        const currentPageNum = (request.userData['pageNum'] as number) ?? 1;
        if (endPage > 0 && currentPageNum >= endPage) {
            log.info(`Reached endPage (${endPage}), stopping pagination.`);
            return;
        }

        const loadMoreSelectors = [
            'div[role="feed"] ~ div button',
            'button[data-testid="load-more"]',
            '[aria-label="See more ads"]',
            'button:has-text("See more")',
            'button:has-text("Load more")',
        ];

        let clicked = false;
        for (const selector of loadMoreSelectors) {
            try {
                const btn = await page.$(selector);
                if (btn) {
                    const isVisible = await btn.isVisible();
                    if (isVisible) {
                        await btn.click();
                        log.info(`Clicked "load more" button with selector: ${selector}`);
                        clicked = true;
                        break;
                    }
                }
            } catch {
                // Try next selector
            }
        }

        if (clicked) {
            // Enqueue same URL as next page with incremented counter
            const nextURL = new URL(sourceURL);
            await crawler.addRequests([
                {
                    url: nextURL.toString(),
                    userData: { pageNum: currentPageNum + 1 },
                    uniqueKey: `${nextURL.toString()}_page${currentPageNum + 1}`,
                },
            ]);
        } else {
            log.info('No "load more" button found — reached end of results or pagination not needed.');
        }
    },

    errorHandler({ request, log, session }, error) {
        const msg = error.message ?? '';
        if (msg.includes('403') || msg.includes('blocked')) {
            log.warning(`Blocked (403) on ${request.url} — retiring session and retrying`);
            session?.retire();
        }
    },

    failedRequestHandler({ request, log }) {
        log.error(`Request failed after all retries: ${request.url}`, { errorMessages: request.errorMessages });
    },
});

await crawler.run(
    initialURLs.map((url) => ({
        url,
        userData: { pageNum: 1 },
    })),
);

crawleeLog.info(`Scraping complete. Total ads scraped: ${totalScraped}`);

await Actor.exit();
