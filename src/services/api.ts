import type {
  CategoryItem,
  ComingSoonItem,
  DownloadSizesResponse,
  EpgGuideResponse,
  EpgProgram,
  HomePayload,
  LiveChannelDetailResponse,
  LiveChannelSummary,
  MediaLanguagesResponse,
  MovieDetail,
  MovieSummary,
  PaginatedResponse,
  PersonCreditsResponse,
  RecommendationRequest,
  RecommendationsResponse,
  SeriesDetail,
  SeriesSummary,
  StreamVariantsResponse,
  ImdbCastResponse,
  SubtitlesResponse,
} from '../types/media';

import {
  clearPersistentApiCache,
  getPersistentApiCache,
  putPersistentApiCache,
} from '../utils/apiCacheDb';
import { clearNavPersistence } from '../utils/navPersistence';
import { sanitizeUserErrorMessage } from '../utils/networkError';

const CATALOG_CACHE_TTL_MS = 30 * 60 * 1000; // 30 minutes keeps catalogs/details loaded one-time
const LIVE_CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes keeps live channels, channel details & EPG instant
const MAX_CLIENT_CACHE_ENTRIES = 400;
const LOCAL_CACHE_PREFIX = 'cinebox_api_v1:';
const CACHE_EPOCH_KEY = 'cinebox_cache_epoch';
const LOCAL_PERSISTED_PATHS = new Set([
  '/api/home',
  '/api/coming-soon',
  '/api/movies/categories',
  '/api/series/categories',
  '/api/live/categories',
]);

const clientCatalogCache = new Map<string, { expiresAt: number; data: unknown }>();
const inFlightRequests = new Map<string, Promise<unknown>>();
const backgroundRevalidations = new Set<string>();

function getCacheEpoch(): number {
  try {
    if (typeof window !== 'undefined' && window.localStorage) {
      const raw = window.localStorage.getItem(CACHE_EPOCH_KEY);
      const parsed = raw ? Number(raw) : 0;
      return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
    }
  } catch {
    // Ignore storage access errors
  }
  return 0;
}

function bumpCacheEpoch(): void {
  try {
    if (typeof window !== 'undefined' && window.localStorage) {
      const next = getCacheEpoch() + 1;
      window.localStorage.setItem(CACHE_EPOCH_KEY, String(next));
    }
  } catch {
    // Ignore storage access errors
  }
}

function buildCacheFetchUrl(url: string): string {
  const epoch = getCacheEpoch();
  if (epoch <= 0) return url;
  return `${url}${url.includes('?') ? '&' : '?'}_v=${epoch}`;
}

function clearStorageByPrefix(storage: Storage | undefined, prefix: string): void {
  if (!storage) return;
  const toRemove: string[] = [];
  for (let i = 0; i < storage.length; i += 1) {
    const k = storage.key(i);
    if (k && k.startsWith(prefix)) {
      toRemove.push(k);
    }
  }
  for (const k of toRemove) {
    storage.removeItem(k);
  }
}

export function clearClientCatalogCache(clearNavigation = false): void {
  clientCatalogCache.clear();
  inFlightRequests.clear();
  bumpCacheEpoch();
  void clearPersistentApiCache();
  if (clearNavigation) {
    clearNavPersistence();
  }
  try {
    if (typeof window !== 'undefined') {
      clearStorageByPrefix(window.localStorage, LOCAL_CACHE_PREFIX);
      clearStorageByPrefix(window.sessionStorage, LOCAL_CACHE_PREFIX);
    }
  } catch {
    // Ignore storage access errors
  }
}

function persistLocalSnapshot(key: string, expiresAt: number, data: unknown): void {
  if (!LOCAL_PERSISTED_PATHS.has(key)) return;
  try {
    if (typeof window !== 'undefined' && window.localStorage) {
      const serialized = JSON.stringify({ expiresAt, data });
      if (serialized.length <= 512 * 1024) {
        window.localStorage.setItem(LOCAL_CACHE_PREFIX + key, serialized);
      }
    }
  } catch {
    // Ignore storage quota errors
  }
}

function getCachedEntrySync<T>(
  key: string,
  allowExpiredLocal = false,
): { data: T; isExpired: boolean } | undefined {
  const now = Date.now();
  const hit = clientCatalogCache.get(key);
  if (hit) {
    if (hit.expiresAt <= now) {
      if (allowExpiredLocal) {
        return { data: hit.data as T, isExpired: true };
      }
      clientCatalogCache.delete(key);
    } else {
      clientCatalogCache.delete(key);
      clientCatalogCache.set(key, hit);
      return { data: hit.data as T, isExpired: false };
    }
  }

  if (LOCAL_PERSISTED_PATHS.has(key)) {
    try {
      if (typeof window !== 'undefined' && window.localStorage) {
        const raw = window.localStorage.getItem(LOCAL_CACHE_PREFIX + key);
        if (raw) {
          const parsed = JSON.parse(raw) as { expiresAt: number; data: T };
          if (parsed && parsed.data !== undefined) {
            const expired = parsed.expiresAt <= now;
            if (!expired || allowExpiredLocal) {
              const ttl = expired ? now + 30_000 : parsed.expiresAt;
              clientCatalogCache.set(key, { expiresAt: ttl, data: parsed.data });
              return { data: parsed.data, isExpired: expired };
            }
            window.localStorage.removeItem(LOCAL_CACHE_PREFIX + key);
          }
        }
      }
    } catch {
      // Ignore storage access/parse errors
    }
  }
  return undefined;
}

function setCachedEntrySync(key: string, data: unknown, ttlMs = CATALOG_CACHE_TTL_MS): number {
  const expiresAt = Date.now() + ttlMs;
  if (clientCatalogCache.has(key)) {
    clientCatalogCache.delete(key);
  }
  while (clientCatalogCache.size >= MAX_CLIENT_CACHE_ENTRIES) {
    const oldestKey = clientCatalogCache.keys().next().value;
    if (oldestKey === undefined) break;
    clientCatalogCache.delete(oldestKey);
  }
  clientCatalogCache.set(key, { expiresAt, data });
  persistLocalSnapshot(key, expiresAt, data);
  return expiresAt;
}

function awaitWithAbortSignal<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise;
  if (signal.aborted) {
    return Promise.reject(new DOMException('Aborted', 'AbortError'));
  }
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      reject(new DOMException('Aborted', 'AbortError'));
    };
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (val) => {
        signal.removeEventListener('abort', onAbort);
        resolve(val);
      },
      (err) => {
        signal.removeEventListener('abort', onAbort);
        reject(err);
      },
    );
  });
}

async function requestJson<T>(
  url: string,
  init?: RequestInit & { timeoutMs?: number },
): Promise<T> {
  const { timeoutMs = 20_000, ...fetchInit } = init || {};

  const timeoutController = new AbortController();
  const externalSignal = fetchInit.signal;
  const onExternalAbort = () => timeoutController.abort();
  if (externalSignal) {
    if (externalSignal.aborted) {
      throw new DOMException('Aborted', 'AbortError');
    }
    externalSignal.addEventListener('abort', onExternalAbort, { once: true });
  }

  let timedOut = false;
  const timer =
    typeof window !== 'undefined' && timeoutMs > 0
      ? window.setTimeout(() => {
          timedOut = true;
          timeoutController.abort();
        }, timeoutMs)
      : null;

  let res: Response;
  try {
    res = await fetch(url, {
      redirect: 'follow',
      credentials: 'same-origin',
      ...fetchInit,
      signal: timeoutController.signal,
    });
  } catch (err) {
    if (timer !== null && typeof window !== 'undefined') window.clearTimeout(timer);
    if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);
    if (err instanceof DOMException && err.name === 'AbortError' && !timedOut) {
      throw err;
    }
    throw new Error(
      sanitizeUserErrorMessage(
        timedOut ? 'Request timed out' : err instanceof Error ? err.message : undefined,
      ),
    );
  }

  if (timer !== null && typeof window !== 'undefined') window.clearTimeout(timer);
  if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);

  let data: unknown;
  let jsonParseFailed = false;
  const rawText = await res.text();
  try {
    data = rawText ? JSON.parse(rawText) : null;
  } catch {
    jsonParseFailed = true;
    data = null;
  }

  if (!res.ok) {
    const rawMsg =
      data && typeof (data as { error?: unknown }).error === 'string'
        ? (data as { error: string }).error
        : `Request failed with status ${res.status}`;
    throw new Error(sanitizeUserErrorMessage(rawMsg));
  }

  if (jsonParseFailed || data === null || typeof data !== 'object') {
    throw new Error(sanitizeUserErrorMessage('Invalid response from server'));
  }

  if (
    typeof (data as { error?: unknown }).error === 'string' &&
    (data as { error: string }).error.trim().length > 0
  ) {
    throw new Error(sanitizeUserErrorMessage((data as { error: string }).error));
  }
  return data as T;
}

function getStaleOrFreshMemoryCache<T>(key: string): T | undefined {
  const hit = clientCatalogCache.get(key);
  if (hit && hit.data !== undefined) {
    return hit.data as T;
  }
  return undefined;
}

export async function getCachedApiResponse<T>(key: string): Promise<T | null> {
  const memHit = getStaleOrFreshMemoryCache<T>(key);
  if (memHit !== undefined) return memHit;
  const idbHit = await getPersistentApiCache<T>(key);
  if (idbHit && idbHit.record.data !== undefined) {
    setCachedEntrySync(key, idbHit.record.data, CATALOG_CACHE_TTL_MS);
    return idbHit.record.data;
  }
  return null;
}

export function getCachedHomeSync(): HomePayload | undefined {
  const mem = getCachedEntrySync<HomePayload>('/api/home', true);
  if (mem !== undefined) return mem.data;
  try {
    if (typeof window !== 'undefined' && window.localStorage) {
      const raw = window.localStorage.getItem(LOCAL_CACHE_PREFIX + '/api/home');
      if (raw) {
        const parsed = JSON.parse(raw) as { expiresAt: number; data: HomePayload };
        if (parsed && parsed.data !== undefined) {
          return parsed.data;
        }
      }
    }
  } catch {
    // Ignore storage access/parse errors
  }
  return undefined;
}

export function getCachedSeriesDetailSync(id: string): SeriesDetail | undefined {
  return getStaleOrFreshMemoryCache<SeriesDetail>(`/api/series/${encodeURIComponent(id)}`);
}

export function getCachedSeriesDetail(id: string): Promise<SeriesDetail | null> {
  return getCachedApiResponse<SeriesDetail>(`/api/series/${encodeURIComponent(id)}`);
}

export function getCachedMovieDetailSync(id: string): MovieDetail | undefined {
  return getStaleOrFreshMemoryCache<MovieDetail>(`/api/movies/${encodeURIComponent(id)}`);
}

export function getCachedMovieDetail(id: string): Promise<MovieDetail | null> {
  return getCachedApiResponse<MovieDetail>(`/api/movies/${encodeURIComponent(id)}`);
}

function triggerBackgroundRevalidate<T>(
  cacheKey: string,
  ttlMs: number,
  timeoutMs = 20_000,
): void {
  if (backgroundRevalidations.has(cacheKey)) return;
  if (typeof navigator !== 'undefined' && navigator.onLine === false) return;
  backgroundRevalidations.add(cacheKey);
  void requestJson<T>(buildCacheFetchUrl(cacheKey), {
    timeoutMs,
  })
    .then((data) => {
      const expiresAt = setCachedEntrySync(cacheKey, data, ttlMs);
      void putPersistentApiCache(cacheKey, data, expiresAt);
    })
    .catch(() => {
      // Keep stale cache intact on background refresh error
    })
    .finally(() => {
      backgroundRevalidations.delete(cacheKey);
    });
}

async function requestJsonCached<T>(
  cacheKey: string,
  signal?: AbortSignal,
  ttlMs = CATALOG_CACHE_TTL_MS,
  options?: { timeoutMs?: number },
): Promise<T> {
  if (signal?.aborted) {
    throw new DOMException('Aborted', 'AbortError');
  }
  const allowStaleLocal = LOCAL_PERSISTED_PATHS.has(cacheKey);
  const cached = getCachedEntrySync<T>(cacheKey, allowStaleLocal);
  if (cached !== undefined) {
    if (cached.isExpired) {
      triggerBackgroundRevalidate<T>(cacheKey, ttlMs, options?.timeoutMs ?? 20_000);
    }
    return cached.data;
  }

  let pending = inFlightRequests.get(cacheKey) as Promise<T> | undefined;
  if (!pending) {
    pending = (async () => {
      const idbHit = await getPersistentApiCache<T>(cacheKey);
      if (idbHit && !idbHit.isExpired) {
        const remainingMs = Math.max(1000, idbHit.record.expiresAt - Date.now());
        setCachedEntrySync(cacheKey, idbHit.record.data, remainingMs);
        return idbHit.record.data;
      }

      // Stale-while-revalidate: return persisted data immediately and refresh silently in background
      if (idbHit && idbHit.record.data !== undefined) {
        setCachedEntrySync(cacheKey, idbHit.record.data, 60_000);
        if (typeof navigator === 'undefined' || navigator.onLine !== false) {
          triggerBackgroundRevalidate<T>(cacheKey, ttlMs, options?.timeoutMs ?? 20_000);
        }
        return idbHit.record.data;
      }

      try {
        const data = await requestJson<T>(buildCacheFetchUrl(cacheKey), {
          timeoutMs: options?.timeoutMs ?? 22_000,
        });
        const expiresAt = setCachedEntrySync(cacheKey, data, ttlMs);
        void putPersistentApiCache(cacheKey, data, expiresAt);
        return data;
      } catch (err) {
        // Stale-if-error fallback so mobile WebView cold boots offline or on spotty cellular still work
        if (idbHit) {
          setCachedEntrySync(cacheKey, idbHit.record.data, 60_000);
          return idbHit.record.data;
        }
        throw err;
      }
    })().finally(() => {
      if (inFlightRequests.get(cacheKey) === pending) {
        inFlightRequests.delete(cacheKey);
      }
    });
    inFlightRequests.set(cacheKey, pending);
  }

  return awaitWithAbortSignal(pending, signal);
}

function buildHeroFromMovies(movies: MovieSummary[]): MovieDetail[] {
  return movies.slice(0, 6).map((m) => ({
    ...m,
    backdrop: m.poster,
    plot: '',
    cast: '',
    director: '',
    genre: m.categoryName || '',
    releaseDate: m.year || '',
    duration: '',
    youtubeTrailer: '',
    tmdbId: '',
    playUrl: `/api/play/vod/${encodeURIComponent(m.streamId)}.${m.extension || 'mp4'}`,
    related: [],
  }));
}

async function fetchProgressiveFallbackHome(
  signal?: AbortSignal,
  onUpdate?: (payload: HomePayload) => void,
): Promise<HomePayload> {
  const [newestMoviesRes, newestSeriesRes, topMoviesRes, topSeriesRes] = await Promise.allSettled([
    fetchMovies({ sort: 'newest', page: 1, limit: 24 }, signal),
    fetchSeriesList({ sort: 'newest', page: 1, limit: 24 }, signal),
    fetchMovies({ sort: 'rating', page: 1, limit: 18 }, signal),
    fetchSeriesList({ sort: 'rating', page: 1, limit: 18 }, signal),
  ]);

  if (signal?.aborted) {
    throw new DOMException('Aborted', 'AbortError');
  }

  const latestMovies =
    newestMoviesRes.status === 'fulfilled' ? newestMoviesRes.value.items : [];
  const latestSeries =
    newestSeriesRes.status === 'fulfilled' ? newestSeriesRes.value.items : [];
  const trendingMovies =
    topMoviesRes.status === 'fulfilled' && topMoviesRes.value.items.length > 0
      ? topMoviesRes.value.items
      : latestMovies.slice(0, 18);
  const popularSeries =
    topSeriesRes.status === 'fulfilled' && topSeriesRes.value.items.length > 0
      ? topSeriesRes.value.items
      : latestSeries.slice(0, 18);

  if (
    latestMovies.length === 0 &&
    latestSeries.length === 0 &&
    trendingMovies.length === 0 &&
    popularSeries.length === 0
  ) {
    throw new Error('Unable to reach the streaming service right now.');
  }

  const totalMovies =
    newestMoviesRes.status === 'fulfilled'
      ? newestMoviesRes.value.total
      : topMoviesRes.status === 'fulfilled'
        ? topMoviesRes.value.total
        : latestMovies.length;
  const totalSeries =
    newestSeriesRes.status === 'fulfilled'
      ? newestSeriesRes.value.total
      : topSeriesRes.status === 'fulfilled'
        ? topSeriesRes.value.total
        : latestSeries.length;

  const mergedMovies =
    trendingMovies.length > 0 ? trendingMovies : latestMovies;

  const basePayload: HomePayload = {
    heroItems: buildHeroFromMovies(latestMovies.length > 0 ? latestMovies : mergedMovies),
    trendingMovies: mergedMovies,
    popularSeries: popularSeries.length > 0 ? popularSeries : latestSeries,
    featuredLive: [],
    stats: {
      movieCount: totalMovies,
      seriesCount: totalSeries,
      liveCount: 0,
      epgChannelCount: 0,
      catchupCount: 0,
    },
  };

  // Non-blocking background live highlights enrichment so Discover never waits 30s on live_streams
  void fetchLiveChannels({ page: 1, limit: 18 }, signal)
    .then((liveRes) => {
      if (signal?.aborted || !liveRes.items.length) return;
      const enriched: HomePayload = {
        ...basePayload,
        featuredLive: liveRes.items,
        stats: {
          ...basePayload.stats,
          liveCount: liveRes.total || liveRes.items.length,
        },
      };
      const expiresAt = setCachedEntrySync('/api/home', enriched, 5 * 60 * 1000);
      void putPersistentApiCache('/api/home', enriched, expiresAt);
      onUpdate?.(enriched);
    })
    .catch(() => {
      // Live channels can be loaded lazily on Live TV tab
    });

  return basePayload;
}

export async function fetchHomeProgressive(
  signal?: AbortSignal,
  onUpdate?: (payload: HomePayload) => void,
): Promise<HomePayload> {
  if (signal?.aborted) {
    throw new DOMException('Aborted', 'AbortError');
  }

  // 1. Instant sync / IndexedDB cache hit
  const syncHit = getCachedEntrySync<HomePayload>('/api/home', true);
  if (syncHit !== undefined) {
    if (syncHit.isExpired) {
      triggerBackgroundRevalidate<HomePayload>('/api/home', CATALOG_CACHE_TTL_MS, 14_000);
    }
    return syncHit.data;
  }

  const idbHit = await getPersistentApiCache<HomePayload>('/api/home');
  if (idbHit && idbHit.record.data !== undefined) {
    const ttl = idbHit.isExpired
      ? 60_000
      : Math.max(1000, idbHit.record.expiresAt - Date.now());
    setCachedEntrySync('/api/home', idbHit.record.data, ttl);
    if (idbHit.isExpired) {
      triggerBackgroundRevalidate<HomePayload>('/api/home', CATALOG_CACHE_TTL_MS, 14_000);
    }
    return idbHit.record.data;
  }

  // 2. Cold boot: call local /api/home AND if still pending at 1.6s,
  //    race with lightweight local /api/movies + /api/series calls so Discover renders fast.
  const localHomePromise = requestJson<HomePayload>(buildCacheFetchUrl('/api/home'), {
    signal,
    timeoutMs: 20_000,
  }).then((data) => {
    const expiresAt = setCachedEntrySync('/api/home', data, CATALOG_CACHE_TTL_MS);
    void putPersistentApiCache('/api/home', data, expiresAt);
    return data;
  });

  return await new Promise<HomePayload>((resolve, reject) => {
    let settled = false;
    let fallbackStarted = false;

    const finishWith = (payload: HomePayload) => {
      if (!settled) {
        settled = true;
        resolve(payload);
      } else {
        onUpdate?.(payload);
      }
    };

    const startFallback = () => {
      if (fallbackStarted || settled || signal?.aborted) return;
      fallbackStarted = true;
      void fetchProgressiveFallbackHome(signal, onUpdate)
        .then((fallbackPayload) => {
          const expiresAt = setCachedEntrySync('/api/home', fallbackPayload, 5 * 60 * 1000);
          void putPersistentApiCache('/api/home', fallbackPayload, expiresAt);
          finishWith(fallbackPayload);
        })
        .catch((err) => {
          if (!settled) {
            localHomePromise.catch(() => {
              if (!settled) {
                settled = true;
                reject(err);
              }
            });
          }
        });
    };

    const headStartTimer =
      typeof window !== 'undefined'
        ? window.setTimeout(startFallback, 1600)
        : null;

    localHomePromise
      .then((fullHome) => {
        if (headStartTimer !== null && typeof window !== 'undefined') {
          window.clearTimeout(headStartTimer);
        }
        finishWith(fullHome);
      })
      .catch(() => {
        if (headStartTimer !== null && typeof window !== 'undefined') {
          window.clearTimeout(headStartTimer);
        }
        startFallback();
      });
  });
}

export function fetchHome(signal?: AbortSignal): Promise<HomePayload> {
  return fetchHomeProgressive(signal);
}

let catalogWarmupStarted = false;
export function warmCatalogTabsInBackground(): void {
  if (catalogWarmupStarted) return;
  if (typeof navigator !== 'undefined' && navigator.onLine === false) return;
  catalogWarmupStarted = true;

  void (async () => {
    const steps: Array<() => Promise<unknown>> = [
      () => fetchMovieCategories(),
      () => fetchMovies({ sort: 'newest', page: 1, limit: 36 }),
      () => fetchSeriesCategories(),
      () => fetchSeriesList({ sort: 'newest', page: 1, limit: 36 }),
      () => fetchLiveCategories(),
      () => fetchLiveChannels({ page: 1, limit: 36 }),
    ];
    for (const runStep of steps) {
      try {
        await runStep();
      } catch {
        // Ignore non-critical background warmup errors
      }
    }
  })();
}

export function fetchComingSoon(signal?: AbortSignal): Promise<{ items: ComingSoonItem[] }> {
  return requestJsonCached<{ items: ComingSoonItem[] }>(
    '/api/coming-soon',
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchRecommendations(
  payload: RecommendationRequest,
  signal?: AbortSignal,
): Promise<RecommendationsResponse> {
  return requestJson<RecommendationsResponse>('/api/recommendations', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal,
  });
}

export function fetchMovieCategories(signal?: AbortSignal): Promise<{ categories: CategoryItem[] }> {
  return requestJsonCached<{ categories: CategoryItem[] }>(
    '/api/movies/categories',
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchMovies(
  params: {
    categoryId?: string;
    genre?: string;
    q?: string;
    sort?: 'newest' | 'rating' | 'az';
    page?: number;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<PaginatedResponse<MovieSummary>> {
  const qs = new URLSearchParams();
  if (params.categoryId) qs.set('categoryId', params.categoryId);
  if (params.genre) qs.set('genre', params.genre);
  if (params.q) qs.set('q', params.q);
  if (params.sort) qs.set('sort', params.sort);
  if (params.page) qs.set('page', String(params.page));
  if (params.limit) qs.set('limit', String(params.limit));
  return requestJsonCached<PaginatedResponse<MovieSummary>>(
    `/api/movies?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchMovieDetail(id: string, signal?: AbortSignal): Promise<MovieDetail> {
  return requestJsonCached<MovieDetail>(
    `/api/movies/${encodeURIComponent(id)}`,
    signal,
    CATALOG_CACHE_TTL_MS,
    { timeoutMs: 18_000 },
  );
}

export function fetchSeriesCategories(signal?: AbortSignal): Promise<{ categories: CategoryItem[] }> {
  return requestJsonCached<{ categories: CategoryItem[] }>(
    '/api/series/categories',
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchSeriesList(
  params: {
    categoryId?: string;
    genre?: string;
    q?: string;
    sort?: 'newest' | 'rating' | 'az';
    page?: number;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<PaginatedResponse<SeriesSummary>> {
  const qs = new URLSearchParams();
  if (params.categoryId) qs.set('categoryId', params.categoryId);
  if (params.genre) qs.set('genre', params.genre);
  if (params.q) qs.set('q', params.q);
  if (params.sort) qs.set('sort', params.sort);
  if (params.page) qs.set('page', String(params.page));
  if (params.limit) qs.set('limit', String(params.limit));
  return requestJsonCached<PaginatedResponse<SeriesSummary>>(
    `/api/series?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchSeriesDetail(id: string, signal?: AbortSignal): Promise<SeriesDetail> {
  return requestJsonCached<SeriesDetail>(
    `/api/series/${encodeURIComponent(id)}`,
    signal,
    CATALOG_CACHE_TTL_MS,
    { timeoutMs: 18_000 },
  );
}

export function fetchTrailerPreview(
  kind: 'movie' | 'series',
  id: string,
  signal?: AbortSignal,
): Promise<{ youtubeTrailer: string }> {
  return requestJsonCached<{ youtubeTrailer: string }>(
    `/api/trailer/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchLiveCategories(signal?: AbortSignal): Promise<{ categories: CategoryItem[] }> {
  return requestJsonCached<{ categories: CategoryItem[] }>(
    '/api/live/categories',
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchLiveChannels(
  params: {
    categoryId?: string;
    genre?: string;
    q?: string;
    hasEpg?: boolean;
    hasCatchup?: boolean;
    page?: number;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<PaginatedResponse<LiveChannelSummary>> {
  const qs = new URLSearchParams();
  if (params.categoryId) qs.set('categoryId', params.categoryId);
  if (params.genre) qs.set('genre', params.genre);
  if (params.q) qs.set('q', params.q);
  if (params.hasEpg) qs.set('hasEpg', 'true');
  if (params.hasCatchup) qs.set('hasCatchup', 'true');
  if (params.page) qs.set('page', String(params.page));
  if (params.limit) qs.set('limit', String(params.limit));
  return requestJsonCached<PaginatedResponse<LiveChannelSummary>>(
    `/api/live?${qs.toString()}`,
    signal,
    LIVE_CACHE_TTL_MS,
    { timeoutMs: 32_000 },
  );
}

export function fetchLiveChannelDetail(
  streamId: string,
  signal?: AbortSignal,
): Promise<LiveChannelDetailResponse> {
  return requestJsonCached<LiveChannelDetailResponse>(
    `/api/live/${encodeURIComponent(streamId)}`,
    signal,
    LIVE_CACHE_TTL_MS,
    { timeoutMs: 28_000 },
  );
}

export function fetchChannelEpg(
  streamId: string,
  signal?: AbortSignal,
): Promise<{ programs: EpgProgram[]; serverTimestamp: number }> {
  return requestJsonCached<{ programs: EpgProgram[]; serverTimestamp: number }>(
    `/api/live/${encodeURIComponent(streamId)}/epg`,
    signal,
    LIVE_CACHE_TTL_MS,
    { timeoutMs: 24_000 },
  );
}

export function fetchEpgGuide(
  params: {
    categoryId?: string;
    q?: string;
    catchupOnly?: boolean;
    offsetHours?: number;
    windowHours?: number;
    page?: number;
    limit?: number;
  },
  signal?: AbortSignal,
): Promise<EpgGuideResponse> {
  const qs = new URLSearchParams();
  if (params.categoryId) qs.set('categoryId', params.categoryId);
  if (params.q) qs.set('q', params.q);
  if (params.catchupOnly) qs.set('catchupOnly', 'true');
  if (params.offsetHours !== undefined) qs.set('offsetHours', String(params.offsetHours));
  if (params.windowHours !== undefined) qs.set('windowHours', String(params.windowHours));
  if (params.page) qs.set('page', String(params.page));
  if (params.limit) qs.set('limit', String(params.limit));
  return requestJsonCached<EpgGuideResponse>(
    `/api/epg/guide?${qs.toString()}`,
    signal,
    LIVE_CACHE_TTL_MS,
    { timeoutMs: 32_000 },
  );
}

export function omniSearch(
  q: string,
  signal?: AbortSignal,
): Promise<{
  movies: MovieSummary[];
  series: SeriesSummary[];
  live: LiveChannelSummary[];
}> {
  return requestJsonCached(
    `/api/search?q=${encodeURIComponent(q)}`,
    signal,
    LIVE_CACHE_TTL_MS,
    { timeoutMs: 24_000 },
  );
}

export function fetchMediaLanguages(
  params: {
    kind: 'movie' | 'episode';
    id: string;
    extension?: string;
    seriesId?: string;
    seasonNum?: number;
    episodeNum?: number;
    title?: string;
    skipProbe?: boolean;
    sourceUrl?: string;
  },
  signal?: AbortSignal,
): Promise<MediaLanguagesResponse> {
  const qs = new URLSearchParams();
  if (params.extension) qs.set('ext', params.extension);
  if (params.seriesId) qs.set('seriesId', params.seriesId);
  if (params.seasonNum) qs.set('seasonNum', String(params.seasonNum));
  if (params.episodeNum) qs.set('episodeNum', String(params.episodeNum));
  if (params.title) qs.set('title', params.title);
  if (params.skipProbe) qs.set('skipProbe', '1');
  if (params.sourceUrl && /^https?:\/\//i.test(params.sourceUrl)) {
    qs.set('sourceUrl', params.sourceUrl);
  }
  return requestJsonCached<MediaLanguagesResponse>(
    `/api/media/languages/${encodeURIComponent(params.kind)}/${encodeURIComponent(
      params.id,
    )}?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function searchByPerson(
  params: {
    name: string;
    imdbId?: string;
    excludeKind?: 'movie' | 'series';
    excludeId?: string;
  },
  signal?: AbortSignal,
): Promise<PersonCreditsResponse> {
  const qs = new URLSearchParams();
  qs.set('name', params.name);
  if (params.imdbId) qs.set('imdbId', params.imdbId);
  if (params.excludeKind) qs.set('excludeKind', params.excludeKind);
  if (params.excludeId) qs.set('excludeId', params.excludeId);
  return requestJsonCached<PersonCreditsResponse>(
    `/api/search/person?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchImdbCast(
  params: {
    title?: string;
    year?: string | null;
    cast?: string;
    director?: string;
    kind?: string;
    id?: string;
    tmdbId?: string;
  },
  signal?: AbortSignal,
): Promise<ImdbCastResponse> {
  const qs = new URLSearchParams();
  if (params.title) qs.set('title', params.title);
  if (params.year) qs.set('year', params.year);
  if (params.cast) qs.set('cast', params.cast);
  if (params.director) qs.set('director', params.director);
  if (params.kind) qs.set('kind', params.kind);
  if (params.id) qs.set('id', params.id);
  if (params.tmdbId) qs.set('tmdbId', params.tmdbId);
  return requestJsonCached<ImdbCastResponse>(
    `/api/imdb/cast?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchImdbPoster(
  title: string,
  year?: string | null,
  signal?: AbortSignal,
): Promise<{ poster: string; imdbId?: string }> {
  const qs = new URLSearchParams();
  qs.set('title', title);
  if (year) qs.set('year', year);
  return requestJsonCached<{ poster: string; imdbId?: string }>(
    `/api/imdb/poster?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchChannelLogo(
  name: string,
  category?: string,
  signal?: AbortSignal,
): Promise<{ logo: string | null }> {
  const qs = new URLSearchParams();
  qs.set('name', name);
  if (category) qs.set('category', category);
  return requestJsonCached<{ logo: string | null }>(
    `/api/channel-logo?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchSubtitles(
  params: {
    kind: string;
    streamId: string;
    season?: number;
    episode?: number;
    title?: string;
    year?: string | null;
    seriesId?: string;
    imdbId?: string;
    q?: string;
  },
  signal?: AbortSignal,
): Promise<SubtitlesResponse> {
  const qs = new URLSearchParams();
  if (params.season !== undefined) qs.set('season', String(params.season));
  if (params.episode !== undefined) qs.set('episode', String(params.episode));
  if (params.title) qs.set('title', params.title);
  if (params.year) qs.set('year', params.year);
  if (params.seriesId) qs.set('seriesId', params.seriesId);
  if (params.imdbId) qs.set('imdbId', params.imdbId);
  if (params.q) qs.set('q', params.q);
  return requestJsonCached<SubtitlesResponse>(
    `/api/subtitles/list/${encodeURIComponent(params.kind)}/${encodeURIComponent(
      params.streamId,
    )}?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

export function fetchStreamVariants(
  kind: 'movie' | 'episode' | 'series' | 'live' | 'catchup' | 'vod',
  id: string,
  ext = 'mp4',
  signal?: AbortSignal,
): Promise<StreamVariantsResponse & { directUrl?: string }> {
  const apiKind = kind === 'episode' ? 'series' : kind === 'movie' ? 'vod' : kind;
  const qs = new URLSearchParams();
  if (ext) qs.set('ext', ext);
  return requestJsonCached<StreamVariantsResponse & { directUrl?: string }>(
    `/api/play/variants/${encodeURIComponent(apiKind)}/${encodeURIComponent(id)}?${qs.toString()}`,
    signal,
    LIVE_CACHE_TTL_MS,
    { timeoutMs: 12_000 },
  );
}

export function fetchDownloadSizes(
  params: {
    kind: 'movie' | 'episode' | 'series' | 'vod';
    id: string;
    ext?: string;
    durationSec?: number;
    quick?: boolean;
    sourceUrl?: string;
  },
  signal?: AbortSignal,
): Promise<DownloadSizesResponse> {
  const apiKind = params.kind === 'episode' ? 'series' : params.kind === 'movie' ? 'vod' : params.kind;
  const qs = new URLSearchParams();
  if (params.ext) qs.set('ext', params.ext);
  if (params.durationSec && Number.isFinite(params.durationSec) && params.durationSec > 0) {
    qs.set('durationSec', String(Math.round(params.durationSec)));
  }
  if (params.quick) qs.set('quick', '1');
  if (params.sourceUrl && /^https?:\/\//i.test(params.sourceUrl)) {
    qs.set('sourceUrl', params.sourceUrl);
  }
  return requestJsonCached<DownloadSizesResponse>(
    `/api/play/download-sizes/${encodeURIComponent(apiKind)}/${encodeURIComponent(
      params.id,
    )}?${qs.toString()}`,
    signal,
    CATALOG_CACHE_TTL_MS,
  );
}

const recentStreamPreloads = new Map<
  string,
  {
    expiresAt: number;
    promise: Promise<{ ok: boolean; mode?: string; sessionId?: string; playlistUrl?: string; skipped?: string }>;
  }
>();

export function preloadStreamMedia(
  params: {
    kind: 'movie' | 'episode' | 'series' | 'live' | 'vod';
    id: string;
    ext?: string;
    quality?: string;
    startSec?: number;
    durationSec?: number;
    sourceUrl?: string;
  },
  _signal?: AbortSignal,
): Promise<{ ok: boolean; mode?: string; sessionId?: string; playlistUrl?: string; skipped?: string }> {
  const apiKind = params.kind === 'episode' ? 'series' : params.kind === 'vod' ? 'movie' : params.kind;
  const qs = new URLSearchParams();
  if (params.ext) qs.set('ext', params.ext);
  if (params.quality) qs.set('quality', params.quality);
  if (params.startSec && params.startSec > 0) qs.set('startSec', String(Math.floor(params.startSec)));
  if (params.durationSec && params.durationSec > 0) qs.set('durationSec', String(Math.floor(params.durationSec)));
  if (params.sourceUrl && /^https?:\/\//i.test(params.sourceUrl)) {
    qs.set('sourceUrl', params.sourceUrl);
  }

  const dedupeKey = `${apiKind}:${params.id}:${qs.toString()}`;
  const now = Date.now();
  const cached = recentStreamPreloads.get(dedupeKey);
  if (cached && cached.expiresAt > now) {
    return cached.promise;
  }

  const promise = fetch(
    `/api/play/preload/${encodeURIComponent(apiKind)}/${encodeURIComponent(params.id)}?${qs.toString()}`,
    { method: 'GET', cache: 'no-store' },
  )
    .then((r) => r.json())
    .catch(() => ({ ok: false }));

  recentStreamPreloads.set(dedupeKey, {
    expiresAt: now + 4000,
    promise,
  });
  if (recentStreamPreloads.size > 24) {
    for (const [k, entry] of recentStreamPreloads.entries()) {
      if (entry.expiresAt <= now) recentStreamPreloads.delete(k);
    }
  }
  return promise;
}


