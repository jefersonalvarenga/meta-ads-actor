import { Actor } from 'apify';
import { log } from 'crawlee';

// ─── Types ────────────────────────────────────────────────────────────────────

interface Input {
    search?: string;
    country?: string;
    adType?: string;
    activeStatus?: string;
    maxItems?: number;
    accessToken?: string;
    customData?: Record<string, unknown>;
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

// ─── Facebook Ad Library API ──────────────────────────────────────────────────

const FB_API_BASE = 'https://graph.facebook.com/v21.0/ads_archive';

const AD_TYPE_MAP: Record<string, string> = {
    ALL: 'ALL',
    POLITICAL_AND_ISSUE_ADS: 'POLITICAL_AND_ISSUE_ADS',
    HOUSING: 'HOUSING',
    CREDIT: 'CREDIT',
    EMPLOYMENT: 'EMPLOYMENT',
};

const ACTIVE_STATUS_MAP: Record<string, string> = {
    ALL: 'ALL',
    ACTIVE: 'ACTIVE',
    INACTIVE: 'INACTIVE',
};

// Fields to request from the API — maps to our ProcessedAd output
const AD_FIELDS = [
    'id',
    'page_id',
    'page_name',
    'ad_snapshot_url',
    'ad_delivery_start_time',
    'ad_delivery_stop_time',
    'publisher_platforms',
    'ad_creative_link_captions',
    'ad_creative_link_descriptions',
    'ad_creative_link_titles',
    'bylines',
    'currency',
    'impressions',
    'spend',
].join(',');

interface FbApiAd {
    id?: string;
    page_id?: string;
    page_name?: string;
    ad_snapshot_url?: string;
    ad_delivery_start_time?: string;
    ad_delivery_stop_time?: string;
    publisher_platforms?: string[];
    ad_creative_link_captions?: string[];
    ad_creative_link_descriptions?: string[];
    ad_creative_link_titles?: string[];
    bylines?: string;
}

interface FbApiResponse {
    data?: FbApiAd[];
    paging?: {
        cursors?: { before?: string; after?: string };
        next?: string;
    };
    error?: { message: string; type: string; code: number };
}

function processAd(raw: FbApiAd, customData: Record<string, unknown> | null): ProcessedAd | null {
    const id = raw.id;
    if (!id) return null;

    // Extract link URL from snapshot URL or creative fields
    let linkURL: string | null = null;
    if (raw.ad_creative_link_captions?.length) {
        linkURL = raw.ad_creative_link_captions[0];
    }

    return {
        adArchiveID: id,
        pageName: raw.page_name ?? null,
        pageID: raw.page_id ?? null,
        pageProfileURI: raw.page_id ? `https://www.facebook.com/${raw.page_id}` : null,
        pageCategories: [],  // not available in basic API fields
        pageLikeCount: null, // not available without page token
        publisherPlatforms: raw.publisher_platforms ?? [],
        startDate: raw.ad_delivery_start_time ?? null,
        endDate: raw.ad_delivery_stop_time ?? null,
        ctaType: null,       // not in basic API response
        linkURL,
        scrapedAt: new Date().toISOString(),
        customData,
    };
}

async function fetchAdsPage(
    params: URLSearchParams,
    afterCursor?: string,
): Promise<FbApiResponse> {
    const url = new URL(FB_API_BASE);
    for (const [k, v] of params.entries()) url.searchParams.set(k, v);
    if (afterCursor) url.searchParams.set('after', afterCursor);

    const resp = await fetch(url.toString());
    if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`FB API HTTP ${resp.status}: ${text.slice(0, 200)}`);
    }
    return resp.json() as Promise<FbApiResponse>;
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
    accessToken,
    customData = null,
} = input;

if (!accessToken) {
    throw new Error(
        'Missing "accessToken". Provide a Facebook user access token with ads_read permission.\n' +
        'Get one at: https://developers.facebook.com/tools/explorer/'
    );
}

if (!search) {
    throw new Error('Missing "search". Provide a search keyword.');
}

log.info(`Starting FB Ad Library API scrape: search="${search}", country=${country}, maxItems=${maxItems}`);

// Build base query params
const baseParams = new URLSearchParams({
    search_terms: search,
    ad_reached_countries: `["${country.toUpperCase()}"]`,
    ad_type: AD_TYPE_MAP[adType.toUpperCase()] ?? 'ALL',
    ad_active_status: ACTIVE_STATUS_MAP[activeStatus.toUpperCase()] ?? 'ALL',
    fields: AD_FIELDS,
    limit: String(Math.min(100, maxItems > 0 ? maxItems : 100)),
    access_token: accessToken,
});

let totalScraped = 0;
let afterCursor: string | undefined;
let pageNum = 0;

do {
    pageNum++;
    log.info(`Fetching page ${pageNum} (scraped so far: ${totalScraped})`);

    let response: FbApiResponse;
    try {
        response = await fetchAdsPage(baseParams, afterCursor);
    } catch (err) {
        log.error(`API request failed: ${(err as Error).message}`);
        break;
    }

    if (response.error) {
        log.error(`FB API error: ${response.error.message} (code ${response.error.code})`);
        break;
    }

    const ads = response.data ?? [];
    log.info(`Page ${pageNum}: ${ads.length} ads received`);

    for (const raw of ads) {
        if (maxItems > 0 && totalScraped >= maxItems) break;
        const processed = processAd(raw, customData);
        if (!processed) continue;
        await Actor.pushData(processed);
        totalScraped++;
    }

    // Pagination
    afterCursor = response.paging?.cursors?.after;
    const hasNext = !!response.paging?.next && !!afterCursor;

    if (!hasNext || (maxItems > 0 && totalScraped >= maxItems)) break;

    // Small delay to be a good API citizen
    await new Promise(r => setTimeout(r, 500));

} while (true);

log.info(`Scraping complete. Total ads scraped: ${totalScraped}`);

await Actor.exit();
