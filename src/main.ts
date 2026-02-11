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
    // Free-form object passed through to every output record — useful for
    // identifying the scrape job when chaining multiple Apify actors.
    customData?: Record<string, unknown>;
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
    // body can be object with text/markup or plain string
    body?: { text?: string; markup?: { __html?: string } } | string;
    title?: string;
    link_url?: string;
    link_description?: string;
    link_caption?: string;
    caption?: string;
    cta_type?: string;
    cta_text?: string;
    display_format?: string;
    page_name?: string;
    page_id?: string;
    page_is_deleted?: boolean;
    page_profile_picture_url?: string;
    page_profile_uri?: string;
    page_like_count?: number;
    page_categories?: string[];
    images?: Array<{
        original_image_url?: string;
        resized_image_url?: string;
        watermarked_resized_image_url?: string;
    }>;
    videos?: Array<{
        video_hd_url?: string;
        video_sd_url?: string;
        video_preview_image_url?: string;
        watermarked_video_hd_url?: string;
        watermarked_video_sd_url?: string;
    }>;
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
    extra_images?: Array<{ original_image_url?: string; resized_image_url?: string }>;
    extra_videos?: Array<{ video_hd_url?: string; video_sd_url?: string }>;
    extra_texts?: Array<{ text?: string }>;
    extra_links?: string[];
}

interface ImpressionsWithIndex {
    impressions_text?: string | null;
    impressions_index?: number;
}

interface RawAd {
    // camelCase (from Facebook internal GraphQL)
    adid?: string;
    adArchiveID?: string;
    archiveTypes?: string[];
    categories?: Array<number | string>;
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
    // snake_case (from public actor / page HTML JSON blobs)
    ad_archive_id?: string;
    page_id?: string;
    page_name?: string;
    page_is_deleted?: boolean;
    ad_delivery_start_time?: string;
    ad_delivery_stop_time?: string | null;
    ad_snapshot_url?: string;
    ad_creative_bodies?: string[];
    ad_creative_link_titles?: string[];
    ad_creative_link_descriptions?: string[];
    publisher_platforms?: string[];
    publisher_platform?: string[];
    estimated_audience_size?: SpendRange;
    is_active?: boolean;
    start_date?: number;
    end_date?: number | null;
    collation_count?: number;
    collation_id?: string;
    impressions_with_index?: ImpressionsWithIndex;
    gated_type?: string;
    contains_digital_created_media?: boolean;
    contains_sensitive_content?: boolean;
    total_active_time?: number | null;
    ad_library_url?: string;
    url?: string;
    total?: number;
    position?: number;
    ads_count?: number;
    start_date_formatted?: string;
    end_date_formatted?: string;
}

interface ProcessedAd {
    adArchiveID: string;
    pageName: string | null;
    pageID: string | null;
    pageProfilePictureURL: string | null;
    pageProfileURI: string | null;
    pageCategories: string[];
    pageLikeCount: number | null;
    pageIsDeleted: boolean;
    entityType: string | null;
    startDate: string | null;
    endDate: string | null;
    isActive: boolean;
    currency: string | null;
    spend: SpendRange | null;
    impressions: SpendRange | null;
    impressionsWithIndex: ImpressionsWithIndex | null;
    estimatedAudienceSize: SpendRange | null;
    publisherPlatforms: string[];
    adLibraryURL: string | null;
    categories: Array<number | string>;
    collationCount: number | null;
    collationID: string | null;
    instagramActorName: string | null;
    // Creative fields
    adCreativeBodies: string[];
    adCreativeLinkTitles: string[];
    adCreativeLinkDescriptions: string[];
    adCreativeLinkCaptions: string[];
    adCreativeImages: string[];
    adCreativeVideos: string[];
    adCreativeVideoPreviewImages: string[];
    ctaText: string | null;
    ctaType: string | null;
    linkURL: string | null;
    displayFormat: string | null;
    extraTexts: string[];
    extraLinks: string[];
    // Distribution
    demographicDistribution: DemographicEntry[];
    regionDistribution: RegionEntry[];
    // Meta
    scrapedAt: string;
    sourceURL: string;
    customData: Record<string, unknown> | null;
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


function processAd(raw: RawAd, sourceURL: string, customData: Record<string, unknown> | null = null): ProcessedAd | null {
    const id = raw.adArchiveID ?? raw.adid ?? raw.ad_archive_id;
    if (!id) return null;

    const snap = raw.snapshot ?? {};
    const images: string[] = [];
    const videos: string[] = [];
    const videoPreviews: string[] = [];

    // Extract images from snapshot.images
    for (const img of snap.images ?? []) {
        const url = img.original_image_url ?? img.resized_image_url;
        if (url) images.push(url);
    }
    // Extract videos from snapshot.videos
    for (const vid of snap.videos ?? []) {
        const url = vid.video_hd_url ?? vid.video_sd_url;
        if (url) videos.push(url);
        if (vid.video_preview_image_url) videoPreviews.push(vid.video_preview_image_url);
    }
    // Extract from carousel cards
    for (const card of snap.cards ?? []) {
        const img = card.original_image_url ?? card.resized_image_url;
        if (img) images.push(img);
        const vid = card.video_hd_url ?? card.video_sd_url;
        if (vid) videos.push(vid);
    }
    // Extra images/videos
    for (const img of snap.extra_images ?? []) {
        const url = img.original_image_url ?? img.resized_image_url;
        if (url) images.push(url);
    }
    for (const vid of snap.extra_videos ?? []) {
        const url = vid.video_hd_url ?? vid.video_sd_url;
        if (url) videos.push(url);
    }

    // Body text: snapshot.body can be { text } or { markup.__html } or plain string
    const bodies: string[] = [];
    const snapBody = snap.body;
    if (snapBody) {
        if (typeof snapBody === 'string') {
            if (snapBody) bodies.push(snapBody);
        } else if (snapBody.text) {
            bodies.push(snapBody.text);
        } else if (snapBody.markup?.__html) {
            bodies.push(snapBody.markup.__html.replace(/<[^>]+>/g, ''));
        }
    }
    if (raw.ad_creative_bodies) bodies.push(...raw.ad_creative_bodies);

    // Link titles
    const linkTitles: string[] = [];
    if (snap.title) linkTitles.push(snap.title);
    if (raw.ad_creative_link_titles) linkTitles.push(...raw.ad_creative_link_titles);

    // Link descriptions
    const linkDescriptions: string[] = [];
    if (snap.link_description) linkDescriptions.push(snap.link_description);
    if (raw.ad_creative_link_descriptions) linkDescriptions.push(...raw.ad_creative_link_descriptions);

    // Link captions
    const linkCaptions: string[] = [];
    if (snap.link_caption) linkCaptions.push(snap.link_caption);
    if (snap.caption) linkCaptions.push(snap.caption);

    // Extra texts
    const extraTexts: string[] = (snap.extra_texts ?? []).map(e => e.text ?? '').filter(Boolean);

    // Publisher platforms — try all possible field names
    const platforms: string[] = (
        raw.publisherPlatform ??
        raw.publisher_platform ??
        raw.publisher_platforms ??
        []
    );

    // Dates — prefer epoch (startDate/start_date), fall back to formatted strings
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

    // Page info — prefer snapshot fields as they tend to be more complete
    const pageName = snap.page_name ?? raw.pageName ?? raw.page_name ?? null;
    const pageID = snap.page_id
        ? String(snap.page_id)
        : raw.pageID
            ? String(raw.pageID)
            : (raw.page_id ?? null);
    const pageIsDeleted = snap.page_is_deleted ?? raw.pageIsDeleted ?? raw.page_is_deleted ?? false;
    const pageProfilePictureURL = snap.page_profile_picture_url ?? raw.pageProfilePictureURL ?? null;
    const pageProfileURI = snap.page_profile_uri ?? null;
    const pageCategories: string[] = snap.page_categories ?? raw.pageCategories ?? [];
    const pageLikeCount = snap.page_like_count ?? null;

    const adLibraryURL = raw.ad_library_url ?? (id ? `https://www.facebook.com/ads/library/?id=${id}` : null);

    return {
        adArchiveID: String(id),
        pageName,
        pageID,
        pageProfilePictureURL,
        pageProfileURI,
        pageCategories,
        pageLikeCount,
        pageIsDeleted,
        entityType: raw.entityType ?? null,
        startDate,
        endDate,
        isActive: raw.isActive ?? raw.is_active ?? false,
        currency: raw.currency ?? null,
        spend: raw.spend ?? null,
        impressions: raw.impressions ?? null,
        impressionsWithIndex: raw.impressions_with_index ?? null,
        estimatedAudienceSize: raw.estimated_audience_size ?? null,
        publisherPlatforms: [...new Set(platforms)],
        adLibraryURL,
        categories: raw.categories ?? [],
        collationCount: raw.collationCount ?? raw.collation_count ?? null,
        collationID: raw.collationID ?? raw.collation_id ?? null,
        instagramActorName: raw.instagramActorName ?? null,
        adCreativeBodies: [...new Set(bodies)],
        adCreativeLinkTitles: [...new Set(linkTitles)],
        adCreativeLinkDescriptions: [...new Set(linkDescriptions)],
        adCreativeLinkCaptions: [...new Set(linkCaptions)],
        adCreativeImages: [...new Set(images)],
        adCreativeVideos: [...new Set(videos)],
        adCreativeVideoPreviewImages: [...new Set(videoPreviews)],
        ctaText: snap.cta_text ?? null,
        ctaType: snap.cta_type ?? null,
        linkURL: snap.link_url ?? null,
        displayFormat: snap.display_format ?? null,
        extraTexts: [...new Set(extraTexts)],
        extraLinks: [...new Set(snap.extra_links ?? [])],
        demographicDistribution: raw.demographicDistribution ?? [],
        regionDistribution: raw.regionDistribution ?? [],
        scrapedAt: new Date().toISOString(),
        sourceURL,
        customData,
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
 * Extracts ads from the live page by walking the Facebook Relay/Flux store in memory.
 * Facebook stores ad data in window.__bbox.require calls and in the Relay record store.
 * We use page.evaluate() to walk the in-memory objects and find ad records.
 */
async function extractAdsFromDOM(page: Page, sourceURL: string, customData: Record<string, unknown> | null = null): Promise<ProcessedAd[]> {
    const ads: ProcessedAd[] = [];
    try {
        // Ask the browser to walk every object in the Relay/bootstrap store
        // and collect anything that looks like an ad archive record.
        const rawAds = await page.evaluate((): unknown[] => {
            const results: unknown[] = [];
            const seen = new Set<string>();

            function isAdLike(o: unknown): o is Record<string, unknown> {
                if (!o || typeof o !== 'object' || Array.isArray(o)) return false;
                const r = o as Record<string, unknown>;
                return Boolean(r['adArchiveID'] || r['adid'] || r['ad_archive_id']);
            }

            function walk(obj: unknown, depth: number): void {
                if (depth > 8 || obj === null || typeof obj !== 'object') return;
                if (Array.isArray(obj)) {
                    for (const item of obj) walk(item, depth + 1);
                    return;
                }
                const rec = obj as Record<string, unknown>;
                const id = String(rec['adArchiveID'] ?? rec['adid'] ?? rec['ad_archive_id'] ?? '');
                if (id && isAdLike(rec)) {
                    if (!seen.has(id)) {
                        seen.add(id);
                        results.push(rec);
                    }
                    return; // don't recurse into an ad object itself
                }
                for (const val of Object.values(rec)) walk(val, depth + 1);
            }

            // 1. Walk window.__bbox (Facebook's server-side bootstrap data)
            try {
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const bbox = (window as any).__bbox;
                if (bbox) walk(bbox, 0);
            } catch { /* ignore */ }

            // 2. Walk window.require.s (Bootloader module registry)
            try {
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const reqS = (window as any).require?.s?.entries;
                if (reqS) walk(reqS, 0);
            } catch { /* ignore */ }

            // 3. Walk all inline <script> tags that contain JSON-like data
            try {
                const scripts = Array.from(document.querySelectorAll('script:not([src])'));
                for (const script of scripts) {
                    const text = (script.textContent ?? '').trim();
                    // Only process JSON-like scripts (start with { or [)
                    if (!text.startsWith('{') && !text.startsWith('[')) continue;
                    try {
                        const parsed: unknown = JSON.parse(text);
                        walk(parsed, 0);
                    } catch { /* not pure JSON */ }
                }
            } catch { /* ignore */ }

            return results;
        });

        for (const raw of rawAds) {
            const processed = processAd(raw as RawAd, sourceURL, customData);
            if (processed && !ads.find(a => a.adArchiveID === processed.adArchiveID)) {
                ads.push(processed);
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
    customData = null,
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
crawleeLog.info(`Proxy input: ${JSON.stringify(proxy)}`);
if (proxy?.useApifyProxy !== false) {
    const groups = proxy?.apifyProxyGroups ?? ['RESIDENTIAL'];
    crawleeLog.info(`Using Apify proxy, groups: ${JSON.stringify(groups)}, countryCode: ${country.toUpperCase()}`);
    proxyConfiguration = await Actor.createProxyConfiguration({
        groups,
        countryCode: country.toUpperCase(),
    });
} else if (proxy?.proxyUrls && proxy.proxyUrls.length > 0) {
    crawleeLog.info(`Using custom proxy URLs: ${proxy.proxyUrls.length} URL(s)`);
    proxyConfiguration = await Actor.createProxyConfiguration({
        proxyUrls: proxy.proxyUrls,
    });
} else {
    crawleeLog.warning('No proxy configured — Facebook will likely block datacenter IPs!');
}
crawleeLog.info(`Proxy configuration created: ${proxyConfiguration ? 'yes' : 'no (undefined)'}`);

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

        log.info(`Page title: "${pageTitle}" | URL after load: ${page.url()}`);
        log.info(`GraphQL ads collected via network: ${collectedAds.length}`);

        // Always try DOM/memory extraction — it may have richer data even when GraphQL worked
        log.info('Extracting ads from in-memory Relay store (window.__bbox)...');
        const domAds = await extractAdsFromDOM(page, sourceURL, customData);
        log.info(`In-memory extraction found ${domAds.length} ads`);
        // Merge: add DOM ads not already found via GraphQL
        for (const domAd of domAds) {
            if (!collectedAds.find(r => {
                const rid = r.adArchiveID ?? r.adid ?? r.ad_archive_id;
                return String(rid) === domAd.adArchiveID;
            })) {
                // Push as a pre-processed ad directly
                if (seenAdIDs.has(domAd.adArchiveID)) continue;
                seenAdIDs.add(domAd.adArchiveID);
                await Actor.pushData(domAd);
                totalScraped++;
                if (maxItems > 0 && totalScraped >= maxItems) break;
            }
        }
        if (maxItems > 0 && totalScraped >= maxItems) {
            log.info(`Reached maxItems (${maxItems}) from DOM extraction, stopping.`);
            return;
        }

        // Process and save intercepted GraphQL ads (not already saved via DOM)
        for (const raw of collectedAds) {
            if (maxItems > 0 && totalScraped >= maxItems) break;
            const processed = processAd(raw, sourceURL, customData);
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
