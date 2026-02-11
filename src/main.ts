import { Actor } from 'apify';
import { log as crawleeLog, gotScraping } from 'crawlee';

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
    proxy?: {
        useApifyProxy?: boolean;
        apifyProxyGroups?: string[];
        proxyUrls?: string[];
    };
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
    // snake_case fields
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
    demographicDistribution: DemographicEntry[];
    regionDistribution: RegionEntry[];
    scrapedAt: string;
    sourceURL: string;
    customData: Record<string, unknown> | null;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

const AD_LIBRARY_BASE = 'https://www.facebook.com/ads/library/';
const ASYNC_SEARCH_URL = 'https://www.facebook.com/ads/library/async/search_ads/';

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

function processAd(raw: RawAd, sourceURL: string, customData: Record<string, unknown> | null = null): ProcessedAd | null {
    const id = raw.adArchiveID ?? raw.adid ?? raw.ad_archive_id;
    if (!id) return null;

    const snap = raw.snapshot ?? {};
    const images: string[] = [];
    const videos: string[] = [];
    const videoPreviews: string[] = [];

    for (const img of snap.images ?? []) {
        const url = img.original_image_url ?? img.resized_image_url;
        if (url) images.push(url);
    }
    for (const vid of snap.videos ?? []) {
        const url = vid.video_hd_url ?? vid.video_sd_url;
        if (url) videos.push(url);
        if (vid.video_preview_image_url) videoPreviews.push(vid.video_preview_image_url);
    }
    for (const card of snap.cards ?? []) {
        const img = card.original_image_url ?? card.resized_image_url;
        if (img) images.push(img);
        const vid = card.video_hd_url ?? card.video_sd_url;
        if (vid) videos.push(vid);
    }
    for (const img of snap.extra_images ?? []) {
        const url = img.original_image_url ?? img.resized_image_url;
        if (url) images.push(url);
    }
    for (const vid of snap.extra_videos ?? []) {
        const url = vid.video_hd_url ?? vid.video_sd_url;
        if (url) videos.push(url);
    }

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

    const linkTitles: string[] = [];
    if (snap.title) linkTitles.push(snap.title);
    if (raw.ad_creative_link_titles) linkTitles.push(...raw.ad_creative_link_titles);

    const linkDescriptions: string[] = [];
    if (snap.link_description) linkDescriptions.push(snap.link_description);
    if (raw.ad_creative_link_descriptions) linkDescriptions.push(...raw.ad_creative_link_descriptions);

    const linkCaptions: string[] = [];
    if (snap.link_caption) linkCaptions.push(snap.link_caption);
    if (snap.caption) linkCaptions.push(snap.caption);

    const extraTexts: string[] = (snap.extra_texts ?? []).map(e => e.text ?? '').filter(Boolean);

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
}

/**
 * Merges Set-Cookie headers from a response into an existing cookie map.
 * Returns the updated map.
 */
function mergeCookies(
    existing: Record<string, string>,
    setCookieHeaders: string[] | undefined,
): Record<string, string> {
    for (const cookieLine of setCookieHeaders ?? []) {
        const part = cookieLine.split(';')[0].trim();
        const eqIdx = part.indexOf('=');
        if (eqIdx > 0) {
            existing[part.slice(0, eqIdx).trim()] = part.slice(eqIdx + 1).trim();
        }
    }
    return existing;
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
 * Fetches tokens and cookies needed for the async API.
 * Strategy:
 *   1. GET facebook.com to establish a session and collect initial cookies (datr, etc.)
 *   2. GET the Ad Library page with those cookies to get lsd/dtsg tokens
 */
async function fetchFbTokens(
    searchUrl: string,
    proxyUrl: string | undefined,
): Promise<FbTokens> {
    const cookieMap: Record<string, string> = {};

    const commonHeaders = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    };

    // Step 1: GET facebook.com homepage to collect datr / initial session cookies
    crawleeLog.info('Step 1: GET facebook.com for initial cookies...');
    try {
        const homeResp = await gotScraping({
            url: 'https://www.facebook.com/',
            headers: commonHeaders,
            headerGeneratorOptions: {
                browsers: [{ name: 'chrome', minVersion: 120 }],
                operatingSystems: ['windows'],
                locales: ['en-US'],
            },
            proxyUrl,
            followRedirect: true,
            timeout: { request: 30000 },
        });
        crawleeLog.info(`homepage status: ${homeResp.statusCode}, cookies: ${(homeResp.headers['set-cookie'] as string[] | undefined)?.length ?? 0}`);
        mergeCookies(cookieMap, homeResp.headers['set-cookie'] as string[] | undefined);
    } catch (err) {
        crawleeLog.warning(`homepage GET failed: ${(err as Error).message}`);
    }

    // Small delay between requests
    await new Promise(r => setTimeout(r, 800 + Math.random() * 700));

    // Step 2: GET the Ad Library page to get lsd/dtsg tokens
    crawleeLog.info(`Step 2: GET Ad Library page for tokens...`);
    const adLibResp = await gotScraping({
        url: searchUrl,
        headers: {
            ...commonHeaders,
            'Referer': 'https://www.facebook.com/',
            'Cookie': cookieMapToHeader(cookieMap),
        },
        headerGeneratorOptions: {
            browsers: [{ name: 'chrome', minVersion: 120 }],
            operatingSystems: ['windows'],
            locales: ['en-US', 'pt-BR'],
        },
        proxyUrl,
        followRedirect: true,
        timeout: { request: 30000 },
    });

    crawleeLog.info(`Ad Library page status: ${adLibResp.statusCode}, size: ${(adLibResp.body as string).length} chars`);
    mergeCookies(cookieMap, adLibResp.headers['set-cookie'] as string[] | undefined);

    const html = adLibResp.body as string;

    // Log first 500 chars for diagnosis
    crawleeLog.info(`HTML preview: ${html.slice(0, 500).replace(/\n/g, ' ')}`);

    const { dtsg, lsd } = extractTokensFromHtml(html);
    const cookieHeader = cookieMapToHeader(cookieMap);

    crawleeLog.info(`Tokens — dtsg: ${dtsg ? 'OK (' + dtsg.slice(0, 8) + '...)' : 'MISSING'}, lsd: ${lsd ? 'OK (' + lsd + ')' : 'MISSING'}, cookies: ${cookieHeader.length} chars (${Object.keys(cookieMap).join(', ')})`);

    return { cookies: cookieHeader, dtsg, lsd };
}

// ─── Main async API scraper ────────────────────────────────────────────────────

interface SearchParams {
    search: string;
    country: string;
    adType: string;
    activeStatus: string;
}

async function scrapeViaAsyncAPI(
    searchParams: SearchParams,
    tokens: FbTokens,
    proxyUrl: string | undefined,
    maxItems: number,
    endPage: number,
    sourceURL: string,
    customData: Record<string, unknown> | null,
): Promise<ProcessedAd[]> {
    const results: ProcessedAd[] = [];
    const seenIds = new Set<string>();

    const adTypeValue = AD_TYPE_MAP[(searchParams.adType ?? 'ALL').toUpperCase()] ?? 'all';
    const activeStatusValue = ACTIVE_STATUS_MAP[(searchParams.activeStatus ?? 'ALL').toUpperCase()] ?? 'all';

    let offset = 0;
    const pageSize = 30;
    let pageNum = 0;

    while (true) {
        if (maxItems > 0 && results.length >= maxItems) break;
        if (endPage > 0 && pageNum >= endPage) break;

        // Build form data for the async search endpoint
        const formData = new URLSearchParams({
            q: searchParams.search,
            count: String(pageSize),
            active_status: activeStatusValue,
            ad_type: adTypeValue,
            countries: `["${searchParams.country.toUpperCase()}"]`,
            media_type: 'all',
            search_type: 'keyword_unordered',
            start_index: String(offset),
            ...(tokens.dtsg ? { fb_dtsg: tokens.dtsg } : {}),
            ...(tokens.lsd ? { lsd: tokens.lsd } : {}),
        });

        crawleeLog.info(`Fetching async API page ${pageNum + 1}, offset=${offset}, query="${searchParams.search}"`);

        let responseText: string;
        try {
            const resp = await gotScraping({
                url: ASYNC_SEARCH_URL,
                method: 'POST',
                body: formData.toString(),
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Cookie': tokens.cookies,
                    'Referer': sourceURL,
                    'Origin': 'https://www.facebook.com',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8',
                },
                headerGeneratorOptions: {
                    browsers: [{ name: 'chrome', minVersion: 120 }],
                    operatingSystems: ['windows'],
                },
                proxyUrl,
                followRedirect: true,
                timeout: { request: 30000 },
            });
            responseText = resp.body as string;
        } catch (err) {
            crawleeLog.error(`Async API request failed: ${(err as Error).message}`);
            break;
        }

        const adsOnPage = parseAdsFromResponseText(responseText);
        crawleeLog.info(`Page ${pageNum + 1}: found ${adsOnPage.length} ads in response (response size: ${responseText.length})`);

        if (adsOnPage.length === 0) {
            // Log first 800 chars to help diagnose what Facebook returned
            crawleeLog.info(`Response preview: ${responseText.slice(0, 800).replace(/\n/g, ' ')}`);
            crawleeLog.info('No ads returned — reached end of results or blocked.');
            break;
        }

        for (const raw of adsOnPage) {
            if (maxItems > 0 && results.length >= maxItems) break;
            const processed = processAd(raw, sourceURL, customData);
            if (!processed) continue;
            if (seenIds.has(processed.adArchiveID)) continue;
            seenIds.add(processed.adArchiveID);
            results.push(processed);
        }

        pageNum++;
        offset += pageSize;

        // Small delay between pages to be polite
        await new Promise(resolve => setTimeout(resolve, 500 + Math.floor(Math.random() * 500)));
    }

    return results;
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

crawleeLog.info(`Starting HTTP scrape for ${initialURLs.length} URL(s). maxItems=${maxItems}, endPage=${endPage}`);

// Resolve proxy URL (single URL string for got-scraping)
let proxyUrl: string | undefined;
if (proxy?.useApifyProxy !== false) {
    const groups = proxy?.apifyProxyGroups ?? ['RESIDENTIAL'];
    const proxyConfig = await Actor.createProxyConfiguration({
        groups,
        countryCode: country.toUpperCase(),
    });
    proxyUrl = await proxyConfig?.newUrl() ?? undefined;
    crawleeLog.info(`Using Apify proxy group(s): ${JSON.stringify(groups)}`);
} else if (proxy?.proxyUrls && proxy.proxyUrls.length > 0) {
    proxyUrl = proxy.proxyUrls[0];
    crawleeLog.info(`Using custom proxy URL`);
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

    // Step 1: Fetch the page HTML to get tokens/cookies
    let tokens: FbTokens;
    try {
        tokens = await fetchFbTokens(sourceURL, proxyUrl);
    } catch (err) {
        crawleeLog.error(`Failed to fetch tokens from ${sourceURL}: ${(err as Error).message}`);
        continue;
    }

    // Step 2: Call async API with pagination
    const remaining = maxItems > 0 ? maxItems - totalScraped : 0;
    const ads = await scrapeViaAsyncAPI(
        searchParams,
        tokens,
        proxyUrl,
        remaining,
        endPage,
        sourceURL,
        customData,
    );

    crawleeLog.info(`Got ${ads.length} ads from async API for URL: ${sourceURL}`);

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
