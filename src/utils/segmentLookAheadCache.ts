import type {
  FragmentLoaderConstructor,
  FragmentLoaderContext,
  HlsConfig,
  Loader,
  LoaderCallbacks,
  LoaderConfiguration,
  LoaderContext,
  LoaderStats,
} from 'hls.js';

export interface CachedSegmentEntry {
  key: string;
  url: string;
  sn?: number;
  data: ArrayBuffer;
  byteLength: number;
  contentType: string;
  fetchedAt: number;
}

export interface SegmentLookAheadOptions {
  maxEntries?: number;
  maxTotalBytes?: number;
}

export interface PrefetchSegmentOptions {
  sn?: number;
  rangeStart?: number;
  rangeEnd?: number;
  signal?: AbortSignal;
}

/**
 * Bounded in-memory look-ahead segment cache for smooth initialization and
 * zero-latency segment transitions. Coalesces concurrent fetches for identical
 * segment keys so player loaders and look-ahead prefetchers share a single request.
 */
export class SegmentLookAheadCache {
  private readonly maxEntries: number;
  private readonly maxTotalBytes: number;
  private readonly entries = new Map<string, CachedSegmentEntry>();
  private readonly inFlight = new Map<
    string,
    { promise: Promise<CachedSegmentEntry | null>; controller: AbortController }
  >();
  private totalBytes = 0;
  private disposed = false;

  constructor(options: SegmentLookAheadOptions = {}) {
    this.maxEntries = Math.max(1, options.maxEntries ?? 4);
    this.maxTotalBytes = Math.max(512 * 1024, options.maxTotalBytes ?? 16 * 1024 * 1024);
  }

  buildKey(rawUrl: string, rangeStart?: number, rangeEnd?: number): string {
    let normalizedUrl = rawUrl;
    if (typeof window !== 'undefined') {
      try {
        normalizedUrl = new URL(rawUrl, window.location.origin).href;
      } catch {
        normalizedUrl = rawUrl;
      }
    }
    if (typeof rangeStart === 'number' && Number.isFinite(rangeStart) && rangeStart >= 0) {
      const endPart =
        typeof rangeEnd === 'number' && Number.isFinite(rangeEnd) && rangeEnd >= rangeStart
          ? String(rangeEnd)
          : '';
      return `${normalizedUrl}|${rangeStart}-${endPart}`;
    }
    return normalizedUrl;
  }

  get(key: string): CachedSegmentEntry | undefined {
    if (this.disposed) return undefined;
    const hit = this.entries.get(key);
    if (!hit) return undefined;
    // Refresh LRU order
    this.entries.delete(key);
    this.entries.set(key, hit);
    return hit;
  }

  has(key: string): boolean {
    return !this.disposed && this.entries.has(key);
  }

  isInFlight(key: string): boolean {
    return !this.disposed && this.inFlight.has(key);
  }

  awaitInFlight(key: string): Promise<CachedSegmentEntry | null> | null {
    if (this.disposed) return null;
    return this.inFlight.get(key)?.promise ?? null;
  }

  get size(): number {
    return this.entries.size;
  }

  get cachedBytes(): number {
    return this.totalBytes;
  }

  private setEntry(entry: CachedSegmentEntry): void {
    if (this.disposed) return;
    if (entry.byteLength <= 0 || entry.byteLength > this.maxTotalBytes) {
      return;
    }

    const existing = this.entries.get(entry.key);
    if (existing) {
      this.totalBytes = Math.max(0, this.totalBytes - existing.byteLength);
      this.entries.delete(entry.key);
    }

    while (
      this.entries.size >= this.maxEntries ||
      (this.entries.size > 0 && this.totalBytes + entry.byteLength > this.maxTotalBytes)
    ) {
      const oldestKey = this.entries.keys().next().value;
      if (oldestKey === undefined) break;
      const oldest = this.entries.get(oldestKey);
      this.entries.delete(oldestKey);
      if (oldest) {
        this.totalBytes = Math.max(0, this.totalBytes - oldest.byteLength);
      }
    }

    this.entries.set(entry.key, entry);
    this.totalBytes += entry.byteLength;
  }

  pruneBeforeSn(minSn: number): void {
    if (this.disposed || !Number.isFinite(minSn)) return;
    for (const [key, entry] of this.entries.entries()) {
      if (typeof entry.sn === 'number' && entry.sn < minSn) {
        this.entries.delete(key);
        this.totalBytes = Math.max(0, this.totalBytes - entry.byteLength);
      }
    }
  }

  prefetch(rawUrl: string, options: PrefetchSegmentOptions = {}): Promise<CachedSegmentEntry | null> {
    if (this.disposed || !rawUrl || options.signal?.aborted) {
      return Promise.resolve(null);
    }

    const key = this.buildKey(rawUrl, options.rangeStart, options.rangeEnd);
    const cached = this.get(key);
    if (cached) {
      return Promise.resolve(cached);
    }

    const active = this.inFlight.get(key);
    if (active) {
      return active.promise;
    }

    const controller = new AbortController();
    const onExternalAbort = () => {
      controller.abort();
    };
    if (options.signal) {
      options.signal.addEventListener('abort', onExternalAbort, { once: true });
    }

    const headers: Record<string, string> = {};
    if (
      typeof options.rangeStart === 'number' &&
      Number.isFinite(options.rangeStart) &&
      options.rangeStart >= 0
    ) {
      const endStr =
        typeof options.rangeEnd === 'number' &&
        Number.isFinite(options.rangeEnd) &&
        options.rangeEnd >= options.rangeStart
          ? String(Math.floor(options.rangeEnd))
          : '';
      headers.Range = `bytes=${Math.floor(options.rangeStart)}-${endStr}`;
    }

    let sameOrigin = rawUrl.startsWith('/');
    if (typeof window !== 'undefined') {
      try {
        sameOrigin = new URL(rawUrl, window.location.origin).origin === window.location.origin;
      } catch {
        // fallback to prefix check
      }
    }

    const promise = fetch(rawUrl, {
      method: 'GET',
      headers,
      credentials: sameOrigin ? 'include' : 'omit',
      signal: controller.signal,
    })
      .then(async (res) => {
        if (this.disposed || controller.signal.aborted || !res.ok) {
          return null;
        }
        const data = await res.arrayBuffer();
        if (this.disposed || controller.signal.aborted || data.byteLength === 0) {
          return null;
        }
        const entry: CachedSegmentEntry = {
          key,
          url: rawUrl,
          sn: options.sn,
          data,
          byteLength: data.byteLength,
          contentType: res.headers.get('content-type') || 'application/octet-stream',
          fetchedAt: Date.now(),
        };
        this.setEntry(entry);
        return entry;
      })
      .catch(() => null)
      .finally(() => {
        if (options.signal) {
          options.signal.removeEventListener('abort', onExternalAbort);
        }
        const current = this.inFlight.get(key);
        if (current && current.controller === controller) {
          this.inFlight.delete(key);
        }
      });

    this.inFlight.set(key, { promise, controller });
    return promise;
  }

  clear(): void {
    this.disposed = true;
    for (const active of this.inFlight.values()) {
      try {
        active.controller.abort();
      } catch {
        // ignore
      }
    }
    this.inFlight.clear();
    this.entries.clear();
    this.totalBytes = 0;
  }
}

export async function primeHlsPlaylistLookAhead(
  cache: SegmentLookAheadCache,
  playlistUrl: string,
  options: { maxSegments?: number; signal?: AbortSignal; depth?: number } = {},
): Promise<void> {
  const maxSegments = Math.max(1, options.maxSegments ?? 2);
  const depth = options.depth ?? 0;
  if (depth > 2 || options.signal?.aborted) return;

  try {
    let sameOrigin = playlistUrl.startsWith('/');
    if (typeof window !== 'undefined') {
      try {
        sameOrigin = new URL(playlistUrl, window.location.origin).origin === window.location.origin;
      } catch {
        // ignore
      }
    }

    const res = await fetch(playlistUrl, {
      method: 'GET',
      credentials: sameOrigin ? 'include' : 'omit',
      cache: 'no-store',
      signal: options.signal,
    });
    if (!res.ok || options.signal?.aborted) return;

    const manifestText = await res.text();
    if (options.signal?.aborted) return;

    const baseHref =
      typeof window !== 'undefined'
        ? new URL(playlistUrl, window.location.origin).href
        : playlistUrl;
    const baseUrl = new URL(baseHref);
    const lines = manifestText
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter(Boolean);

    if (lines.some((l) => l.startsWith('#EXT-X-STREAM-INF'))) {
      const variantLine = lines.find((l) => !l.startsWith('#'));
      if (variantLine) {
        const variantUrl = new URL(variantLine, baseUrl).href;
        await primeHlsPlaylistLookAhead(cache, variantUrl, {
          maxSegments,
          signal: options.signal,
          depth: depth + 1,
        });
      }
      return;
    }

    const targets: Array<{ url: string; sn?: number }> = [];
    const initMatch = manifestText.match(/#EXT-X-MAP:URI="([^"]+)"/i);
    if (initMatch?.[1]) {
      targets.push({ url: new URL(initMatch[1], baseUrl).href });
    }

    const mediaSeqMatch = manifestText.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/i);
    let snCursor = mediaSeqMatch ? Number(mediaSeqMatch[1]) : 0;
    let mediaCount = 0;

    for (const line of lines) {
      if (line.startsWith('#')) continue;
      targets.push({ url: new URL(line, baseUrl).href, sn: snCursor });
      snCursor += 1;
      mediaCount += 1;
      if (mediaCount >= maxSegments) break;
    }

    // Fetch init + first segment first for fast first frame, then warm next segment immediately
    for (const target of targets) {
      if (options.signal?.aborted) return;
      await cache.prefetch(target.url, {
        sn: target.sn,
        signal: options.signal,
      });
    }
  } catch {
    // Non-blocking initialization warmup
  }
}

export function createHlsLookAheadFragmentLoader(
  BaseLoaderCtor:
    | FragmentLoaderConstructor
    | (new (config: HlsConfig) => Loader<LoaderContext>),
  cache: SegmentLookAheadCache,
  sessionSignal?: AbortSignal,
): FragmentLoaderConstructor {
  return class LookAheadFragmentLoader implements Loader<FragmentLoaderContext> {
    private readonly baseLoader: Loader<FragmentLoaderContext>;
    private aborted = false;
    private destroyed = false;
    context: FragmentLoaderContext | null = null;
    stats: LoaderStats;

    constructor(config: HlsConfig) {
      this.baseLoader = new (BaseLoaderCtor as FragmentLoaderConstructor)(config);
      this.stats = this.baseLoader.stats ?? {
        aborted: false,
        loaded: 0,
        retry: 0,
        total: 0,
        chunkCount: 0,
        bwEstimate: 0,
        loading: { start: 0, first: 0, end: 0 },
        parsing: { start: 0, end: 0 },
        buffering: { start: 0, first: 0, end: 0 },
      };
    }

    getCacheAge(): number | null {
      return this.baseLoader.getCacheAge ? this.baseLoader.getCacheAge() : null;
    }

    getResponseHeader(name: string): string | null {
      return this.baseLoader.getResponseHeader ? this.baseLoader.getResponseHeader(name) : null;
    }

    load(
      context: FragmentLoaderContext,
      config: LoaderConfiguration,
      callbacks: LoaderCallbacks<FragmentLoaderContext>,
    ): void {
      this.context = context;
      this.aborted = false;

      const key = cache.buildKey(context.url, context.rangeStart, context.rangeEnd);
      const cached = cache.get(key);
      if (cached) {
        const now = typeof performance !== 'undefined' ? performance.now() : Date.now();
        this.stats.aborted = false;
        this.stats.loaded = cached.byteLength;
        this.stats.total = cached.byteLength;
        this.stats.chunkCount = 1;
        this.stats.loading = { start: now, first: now + 1, end: now + 2 };
        queueMicrotask(() => {
          if (this.aborted || this.destroyed || sessionSignal?.aborted) return;
          callbacks.onSuccess(
            { url: context.url, data: cached.data.slice(0), code: 200 },
            this.stats,
            context,
            null,
          );
        });
        return;
      }

      const pending = cache.awaitInFlight(key);
      if (pending) {
        const startTime = typeof performance !== 'undefined' ? performance.now() : Date.now();
        this.stats.loading = { start: startTime, first: 0, end: 0 };
        void pending.then((entry) => {
          if (this.aborted || this.destroyed || sessionSignal?.aborted) return;
          if (entry && entry.byteLength > 0) {
            const endTime = typeof performance !== 'undefined' ? performance.now() : Date.now();
            this.stats.aborted = false;
            this.stats.loaded = entry.byteLength;
            this.stats.total = entry.byteLength;
            this.stats.chunkCount = 1;
            this.stats.loading = {
              start: startTime,
              first: Math.max(startTime + 1, endTime - 1),
              end: Math.max(startTime + 2, endTime),
            };
            callbacks.onSuccess(
              { url: context.url, data: entry.data.slice(0), code: 200 },
              this.stats,
              context,
              null,
            );
          } else {
            this.baseLoader.load(context, config, callbacks);
          }
        });
        return;
      }

      this.baseLoader.load(context, config, callbacks);
    }

    abort(): void {
      this.aborted = true;
      this.stats.aborted = true;
      this.baseLoader.abort();
    }

    destroy(): void {
      this.aborted = true;
      this.destroyed = true;
      this.baseLoader.destroy();
    }
  };
}

