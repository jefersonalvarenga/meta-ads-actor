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
        requestHandlerTimeoutSecs: 90,
        navigationTimeoutSecs: 60,
        maxSessionRotations: 5,
        // Disable crawlee's built-in block detection — Facebook returns 403 for
        // the challenge page but we need the browser to load and execute the JS.
        sessionPoolOptions: { maxPoolSize: 10 },

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

                // Intercept the Ad Library page: if Facebook returns 403 with the
                // __rd_verify challenge, re-fulfill as 200 so crawlee does NOT abort.
                // The browser will still execute the inline JS (fetch + reload).
                await page.route('**/ads/library/**', async (route) => {
                    const response = await route.fetch();
                    if (response.status() === 403) {
                        crawleeLog.info(`Intercepted 403 on ${request.url} — re-fulfilling as 200 for JS challenge execution`);
                        await route.fulfill({
                            status: 200,
                            headers: Object.fromEntries(Object.entries(response.headers())),
                            body: await response.body(),
                        });
                    } else {
                        await route.continue();
                    }
                });
            },
        ],

        async requestHandler({ page, log, request }) {
            log.info('Browser: page loaded, waiting for challenge resolution...');

            // The __rd_verify challenge JS runs automatically:
            //   fetch('/__rd_verify_...', { method: 'POST' }).finally(() => window.location.reload())
            // Wait for up to 2 reload cycles for the challenge to clear.
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

            const html = await page.content();
            log.info(`Browser page size: ${html.length} chars`);

            // Extract cookies
            const browserCookies = await page.context().cookies();
            const cookieMap: Record<string, string> = {};
            for (const c of browserCookies) cookieMap[c.name] = c.value;
            const cookieHeader = cookieMapToHeader(cookieMap);
            log.info(`Browser cookies: ${Object.keys(cookieMap).join(', ')}`);

            // Extract lsd from HTML (needed for HTTP fallback pagination)
            const { lsd } = extractTokensFromHtml(html);

            // Extract dtsg via page.evaluate — use the browser's own fetch to POST
            // async/search_ads with all session cookies automatically included.
            // This is more reliable than HTTP because there's no IP/cookie mismatch.
            const adTypeValue = AD_TYPE_MAP[(searchUrl.includes('ad_type=') ? new URL(searchUrl).searchParams.get('ad_type') ?? 'all' : 'all')];
            const activeStatusValue = ACTIVE_STATUS_MAP[(searchUrl.includes('active_status=') ? new URL(searchUrl).searchParams.get('active_status') ?? 'all' : 'all')];
            const qValue = new URL(searchUrl).searchParams.get('q') ?? '';
            const countryValue = (new URL(searchUrl).searchParams.get('country') ?? 'BR').toUpperCase();

            // Fetch page 1 via browser's own fetch (cookies + CSRF automatically handled)
            const capturedBodies: string[] = [];
            const pageSize = 30;
            const maxPages = Math.ceil((request.userData['maxItems'] as number ?? 100) / pageSize);

            for (let page_i = 0; page_i < maxPages; page_i++) {
                const offset = page_i * pageSize;
                log.info(`Browser fetch: async/search_ads page ${page_i + 1}, offset=${offset}`);
                try {
                    const responseText = await page.evaluate(async (params: {
                        q: string; country: string; adType: string; activeStatus: string;
                        count: number; offset: number;
                    }): Promise<string> => {
                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                        const w = window as any;
                        // Get dtsg from window store
                        let dtsg = '';
                        try {
                            const bbox = w.__bbox;
                            function findToken(obj: unknown, d: number): string {
                                if (d > 6 || !obj || typeof obj !== 'object') return '';
                                const r = obj as Record<string, unknown>;
                                if (typeof r['token'] === 'string' && (r['token'] as string).startsWith('AQ')) return r['token'] as string;
                                for (const v of Object.values(r)) { const t = findToken(v, d + 1); if (t) return t; }
                                return '';
                            }
                            if (bbox) dtsg = findToken(bbox, 0);
                        } catch { /* ignore */ }
                        // Get lsd from meta tag or inline script
                        if (!dtsg) {
                            const m = document.body.innerHTML.match(/"token"\s*:\s*"(AQ[^"]{10,})"/);
                            if (m) dtsg = m[1];
                        }
                        const lsdMeta = document.querySelector('input[name="lsd"]') as HTMLInputElement | null;
                        const lsd = lsdMeta?.value ?? (document.body.innerHTML.match(/"LSD",\[\],\{"token":"([^"]+)"/))?.[1] ?? '';

                        const body = new URLSearchParams({
                            q: params.q,
                            count: String(params.count),
                            active_status: params.activeStatus,
                            ad_type: params.adType,
                            countries: `["${params.country}"]`,
                            media_type: 'all',
                            search_type: 'keyword_unordered',
                            start_index: String(params.offset),
                            ...(dtsg ? { fb_dtsg: dtsg } : {}),
                            ...(lsd ? { lsd } : {}),
                        });

                        const resp = await fetch('https://www.facebook.com/ads/library/async/search_ads/', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/x-www-form-urlencoded',
                                'X-Requested-With': 'XMLHttpRequest',
                                'Accept': '*/*',
                            },
                            credentials: 'include',
                            body: body.toString(),
                        });
                        return await resp.text();
                    }, { q: qValue, country: countryValue, adType: adTypeValue ?? 'all', activeStatus: activeStatusValue ?? 'all', count: pageSize, offset });

                    const isHtml = responseText.trimStart().startsWith('<!');
                    const hasAds = responseText.includes('adArchiveID') || responseText.includes('ad_archive_id');
                    log.info(`  Page ${page_i + 1}: ${responseText.length} chars, isHtml=${isHtml}, hasAds=${hasAds}`);

                    if (isHtml || !hasAds) {
                        log.info('  No ad data in response — stopping pagination');
                        if (page_i === 0) {
                            // Log preview to diagnose
                            log.info(`  Response preview: ${responseText.slice(0, 400).replace(/\n/g, ' ')}`);
                        }
                        break;
                    }
                    capturedBodies.push(responseText);
                } catch (err) {
                    log.error(`  Browser fetch failed on page ${page_i + 1}: ${(err as Error).message}`);
                    break;
                }
            }

            log.info(`Browser fetch complete: ${capturedBodies.length} page(s) with ad data captured`);

            result = { cookies: cookieHeader, dtsg: '', lsd, proxyUrl: pinnedProxyUrl, capturedSearchResponses: capturedBodies };
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
    proxyConfiguration = await Actor.createProxyConfiguration({
        groups,
        countryCode: country.toUpperCase(),
    });
    crawleeLog.info(`Using Apify proxy group(s): ${JSON.stringify(groups)}`);
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
            const processed = processAd(raw, sourceURL, customData);
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
