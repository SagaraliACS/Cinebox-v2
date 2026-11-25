import zlib from 'node:zlib';
import type { EpgProgram, LiveChannelSummary } from './types.ts';
import { inferCountryCode, normalizeChannelName } from './channelLogos.ts';

interface XmltvIndex {
  byXmlId: Map<string, string>;
  byNormName: Map<string, string[]>;
  byCompactName: Map<string, string[]>;
  programmesByChannel: Map<string, EpgProgram[]>;
  expiresAt: number;
}

interface CachedChannelFallback {
  programs: EpgProgram[];
  source: 'provider' | 'external' | 'none';
  expiresAt: number;
}

const FEED_CACHE_TTL_MS = 4 * 60 * 60 * 1000;
const MAX_CACHED_FEEDS = 8;
const CHANNEL_HIT_TTL_MS = 45 * 60 * 1000;
const CHANNEL_MISS_TTL_MS = 20 * 60 * 1000;
const MAX_CHANNEL_FALLBACK_CACHE = 800;

const feedCache = new Map<string, XmltvIndex>();
const inFlightFeeds = new Map<string, Promise<XmltvIndex | null>>();
const channelFallbackCache = new Map<string, CachedChannelFallback>();

const COUNTRY_TO_FEEDS: Record<string, string[]> = {
  GB: [
    'https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz',
    'https://epg.pw/xmltv/epg_GB.xml',
  ],
  US: [
    'https://epgshare01.online/epgshare01/epg_ripper_US2.xml.gz',
    'https://epg.pw/xmltv/epg_US.xml',
  ],
  IN: [
    'https://epgshare01.online/epgshare01/epg_ripper_IN1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_IN4.xml.gz',
    'https://epg.pw/xmltv/epg_IN.xml',
  ],
  PK: [
    'https://epgshare01.online/epgshare01/epg_ripper_PK1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_IN1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz',
  ],
  BD: [
    'https://epgshare01.online/epgshare01/epg_ripper_IN1.xml.gz',
  ],
  AE: [
    'https://epgshare01.online/epgshare01/epg_ripper_AE1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_SA1.xml.gz',
  ],
  SA: [
    'https://epgshare01.online/epgshare01/epg_ripper_SA1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_AE1.xml.gz',
  ],
  EG: [
    'https://epgshare01.online/epgshare01/epg_ripper_EG1.xml.gz',
    'https://epgshare01.online/epgshare01/epg_ripper_AE1.xml.gz',
  ],
  CA: [
    'https://epgshare01.online/epgshare01/epg_ripper_CA2.xml.gz',
    'https://epg.pw/xmltv/epg_CA.xml',
  ],
  AU: [
    'https://epgshare01.online/epgshare01/epg_ripper_AU1.xml.gz',
    'https://epg.pw/xmltv/epg_AU.xml',
  ],
  NZ: ['https://epgshare01.online/epgshare01/epg_ripper_NZ1.xml.gz'],
  DE: ['https://epgshare01.online/epgshare01/epg_ripper_DE1.xml.gz'],
  FR: ['https://epgshare01.online/epgshare01/epg_ripper_FR1.xml.gz'],
  ES: ['https://epgshare01.online/epgshare01/epg_ripper_ES1.xml.gz'],
  IT: ['https://epgshare01.online/epgshare01/epg_ripper_IT1.xml.gz'],
  PT: ['https://epgshare01.online/epgshare01/epg_ripper_PT1.xml.gz'],
  NL: ['https://epgshare01.online/epgshare01/epg_ripper_NL1.xml.gz'],
  BE: ['https://epgshare01.online/epgshare01/epg_ripper_BE2.xml.gz'],
  CH: ['https://epgshare01.online/epgshare01/epg_ripper_CH1.xml.gz'],
  AT: ['https://epgshare01.online/epgshare01/epg_ripper_AT1.xml.gz'],
  SE: ['https://epgshare01.online/epgshare01/epg_ripper_SE1.xml.gz'],
  NO: ['https://epgshare01.online/epgshare01/epg_ripper_NO1.xml.gz'],
  DK: ['https://epgshare01.online/epgshare01/epg_ripper_DK1.xml.gz'],
  FI: ['https://epgshare01.online/epgshare01/epg_ripper_FI1.xml.gz'],
  PL: ['https://epgshare01.online/epgshare01/epg_ripper_PL1.xml.gz'],
  RO: ['https://epgshare01.online/epgshare01/epg_ripper_RO1.xml.gz'],
  GR: ['https://epgshare01.online/epgshare01/epg_ripper_GR1.xml.gz'],
  BG: ['https://epgshare01.online/epgshare01/epg_ripper_BG1.xml.gz'],
  TR: ['https://epgshare01.online/epgshare01/epg_ripper_TR1.xml.gz'],
  ZA: ['https://epgshare01.online/epgshare01/epg_ripper_ZA1.xml.gz'],
  BR: ['https://epgshare01.online/epgshare01/epg_ripper_BR1.xml.gz'],
  MX: ['https://epgshare01.online/epgshare01/epg_ripper_MX1.xml.gz'],
  AR: ['https://epgshare01.online/epgshare01/epg_ripper_AR1.xml.gz'],
  ID: ['https://epgshare01.online/epgshare01/epg_ripper_ID1.xml.gz'],
  MY: ['https://epgshare01.online/epgshare01/epg_ripper_MY1.xml.gz'],
  SG: ['https://epgshare01.online/epgshare01/epg_ripper_SG1.xml.gz'],
  PH: ['https://epgshare01.online/epgshare01/epg_ripper_PH1.xml.gz'],
  TH: ['https://epgshare01.online/epgshare01/epg_ripper_TH1.xml.gz'],
  VN: ['https://epgshare01.online/epgshare01/epg_ripper_VN1.xml.gz'],
  KR: ['https://epgshare01.online/epgshare01/epg_ripper_KR1.xml.gz'],
  JP: ['https://epgshare01.online/epgshare01/epg_ripper_JP1.xml.gz'],
  IL: ['https://epgshare01.online/epgshare01/epg_ripper_IL1.xml.gz'],
  HR: ['https://epgshare01.online/epgshare01/epg_ripper_HR1.xml.gz'],
  RS: ['https://epgshare01.online/epgshare01/epg_ripper_RS1.xml.gz'],
  UA: ['https://epgshare01.online/epgshare01/epg_ripper_UA1.xml.gz'],
};

function decodeXmlEntities(text: string): string {
  if (!text) return '';
  return text
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&amp;/g, '&')
    .replace(/&apos;/g, "'")
    .replace(/&#0*39;/g, "'")
    .replace(/&#x0*27;/gi, "'")
    .replace(/&quot;/g, '"')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&#(\d+);/g, (_m, dec) => {
      const code = Number(dec);
      return Number.isFinite(code) && code > 0 ? String.fromCodePoint(code) : '';
    })
    .replace(/&#x([0-9a-f]+);/gi, (_m, hex) => {
      const code = parseInt(hex, 16);
      return Number.isFinite(code) && code > 0 ? String.fromCodePoint(code) : '';
    })
    .replace(/\s+/g, ' ')
    .trim();
}

function formatUnixClock(unixSec: number): string {
  const d = new Date(unixSec * 1000);
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  return `${hh}:${mm}`;
}

function parseXmltvTimestamp(raw: string): number {
  const m = raw.trim().match(/^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(?:\s*([+-])(\d{2})(\d{2}))?/);
  if (!m) return 0;
  const year = Number(m[1]);
  const month = Number(m[2]) - 1;
  const day = Number(m[3]);
  const hour = Number(m[4]);
  const minute = Number(m[5]);
  const second = Number(m[6]);
  let utcMs = Date.UTC(year, month, day, hour, minute, second);
  if (m[7]) {
    const sign = m[7] === '+' ? 1 : -1;
    const offHours = Number(m[8] || 0);
    const offMins = Number(m[9] || 0);
    const offsetMs = sign * (offHours * 60 + offMins) * 60 * 1000;
    utcMs -= offsetMs;
  }
  return Math.floor(utcMs / 1000);
}

function parseAttr(attrStr: string, attrName: string): string {
  const re = new RegExp(`\\b${attrName}\\s*=\\s*(?:"([^"]*)"|'([^']*)')`, 'i');
  const m = attrStr.match(re);
  return decodeXmlEntities((m?.[1] ?? m?.[2] ?? '').trim());
}

function addToIndexMap(map: Map<string, string[]>, key: string, value: string): void {
  if (!key) return;
  const existing = map.get(key);
  if (!existing) {
    map.set(key, [value]);
  } else if (!existing.includes(value)) {
    existing.push(value);
  }
}

function parseXmltvDocument(xml: string, nowSec: number): XmltvIndex {
  const byXmlId = new Map<string, string>();
  const byNormName = new Map<string, string[]>();
  const byCompactName = new Map<string, string[]>();
  const programmesByChannel = new Map<string, EpgProgram[]>();

  // 1. Index <channel id="..."> display names
  const channelRegex = /<channel\b([^>]*)>([\s\S]*?)<\/channel>/gi;
  let chMatch: RegExpExecArray | null;
  while ((chMatch = channelRegex.exec(xml)) !== null) {
    const channelId = parseAttr(chMatch[1], 'id');
    if (!channelId) continue;
    byXmlId.set(channelId.toLowerCase(), channelId);

    const body = chMatch[2];
    const dnRegex = /<display-name\b[^>]*>([\s\S]*?)<\/display-name>/gi;
    let dnMatch: RegExpExecArray | null;
    while ((dnMatch = dnRegex.exec(body)) !== null) {
      const displayName = decodeXmlEntities(dnMatch[1]);
      if (!displayName) continue;
      const norm = normalizeChannelName(displayName);
      if (!norm) continue;
      addToIndexMap(byNormName, norm, channelId);
      const compact = norm.replace(/\s+/g, '');
      if (compact.length >= 3) {
        addToIndexMap(byCompactName, compact, channelId);
      }
    }
  }

  // 2. Index <programme ...> within [now - 6h, now + 36h] so RAM stays tight
  const minStopSec = nowSec - 6 * 3600;
  const maxStartSec = nowSec + 36 * 3600;
  const progRegex = /<programme\b([^>]*)>([\s\S]*?)<\/programme>/gi;
  let progMatch: RegExpExecArray | null;

  while ((progMatch = progRegex.exec(xml)) !== null) {
    const attrs = progMatch[1];
    const startRaw = parseAttr(attrs, 'start');
    const stopRaw = parseAttr(attrs, 'stop');
    const startTs = parseXmltvTimestamp(startRaw);
    const stopTs = parseXmltvTimestamp(stopRaw);
    if (!startTs || !stopTs || stopTs <= startTs) continue;
    if (stopTs < minStopSec || startTs > maxStartSec) continue;

    const channelId = parseAttr(attrs, 'channel');
    if (!channelId) continue;

    const body = progMatch[2];
    const titleMatch = body.match(/<title\b[^>]*>([\s\S]*?)<\/title>/i);
    const title = titleMatch ? decodeXmlEntities(titleMatch[1]) : '';
    if (!title) continue;

    const descMatch = body.match(/<desc\b[^>]*>([\s\S]*?)<\/desc>/i);
    const description = descMatch ? decodeXmlEntities(descMatch[1]) : '';

    const list = programmesByChannel.get(channelId);
    const item: EpgProgram = {
      id: `ext_${channelId}_${startTs}`,
      title,
      description,
      startTimestamp: startTs,
      stopTimestamp: stopTs,
      startLabel: formatUnixClock(startTs),
      stopLabel: formatUnixClock(stopTs),
      isNowPlaying: startTs <= nowSec && stopTs > nowSec,
      hasArchive: false,
      archiveDurationMin: Math.max(1, Math.round((stopTs - startTs) / 60)),
    };
    if (!list) {
      programmesByChannel.set(channelId, [item]);
    } else {
      list.push(item);
    }
  }

  for (const list of programmesByChannel.values()) {
    list.sort((a, b) => a.startTimestamp - b.startTimestamp);
  }

  return {
    byXmlId,
    byNormName,
    byCompactName,
    programmesByChannel,
    expiresAt: Date.now() + FEED_CACHE_TTL_MS,
  };
}

async function loadXmltvFeed(url: string): Promise<XmltvIndex | null> {
  const now = Date.now();
  const hit = feedCache.get(url);
  if (hit && hit.expiresAt > now) {
    feedCache.delete(url);
    feedCache.set(url, hit);
    return hit;
  }

  const pending = inFlightFeeds.get(url);
  if (pending) return pending;

  const task = (async (): Promise<XmltvIndex | null> => {
    try {
      const resp = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Accept-Encoding': 'gzip, deflate',
        },
        redirect: 'follow',
        signal: AbortSignal.timeout(8000),
      });
      if (!resp.ok) return null;

      const buf = Buffer.from(await resp.arrayBuffer());
      if (buf.length === 0 || buf.length > 24 * 1024 * 1024) return null;

      const isGzipMagic = buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b;
      const xmlBuf = isGzipMagic ? zlib.gunzipSync(buf) : buf;
      const xml = xmlBuf.toString('utf8');

      // Cache even empty stub feeds briefly so we don't keep re-fetching empty files (e.g. PK1)
      const index = xml.includes('<programme')
        ? parseXmltvDocument(xml, Math.floor(Date.now() / 1000))
        : {
            byXmlId: new Map<string, string>(),
            byNormName: new Map<string, string[]>(),
            byCompactName: new Map<string, string[]>(),
            programmesByChannel: new Map<string, EpgProgram[]>(),
            expiresAt: Date.now() + CHANNEL_MISS_TTL_MS,
          };

      while (feedCache.size >= MAX_CACHED_FEEDS) {
        const oldest = feedCache.keys().next().value;
        if (!oldest) break;
        feedCache.delete(oldest);
      }
      feedCache.set(url, index);
      return index;
    } catch {
      return null;
    } finally {
      inFlightFeeds.delete(url);
    }
  })();

  inFlightFeeds.set(url, task);
  return task;
}

function buildChannelNameCandidates(rawName: string): string[] {
  const candidates: string[] = [];
  const push = (val: string) => {
    if (val && val.length >= 2 && !candidates.includes(val)) {
      candidates.push(val);
    }
  };

  // Strip pipe-separated provider group prefixes like "PL | Canal+ | BBC First FHD"
  const pipeParts = rawName
    .split('|')
    .map((p) => p.trim())
    .filter(Boolean);
  const tailName = pipeParts.length >= 2 ? pipeParts[pipeParts.length - 1] : rawName;

  for (const source of [tailName, rawName]) {
    const norm = normalizeChannelName(source);
    push(norm);
    const withoutTv = norm.replace(/\b(tv|channel|network)\b/g, '').replace(/\s+/g, ' ').trim();
    push(withoutTv);
  }

  return candidates;
}

function matchChannelInIndex(index: XmltvIndex, channel: LiveChannelSummary): EpgProgram[] {
  const tryChannelIds = (ids: string[] | undefined): EpgProgram[] => {
    if (!ids) return [];
    for (const id of ids) {
      const progs = index.programmesByChannel.get(id);
      if (progs && progs.length > 0) return progs;
    }
    return [];
  };

  // 1. Direct epgChannelId match
  if (channel.epgChannelId) {
    const matchedId = index.byXmlId.get(channel.epgChannelId.toLowerCase());
    if (matchedId) {
      const progs = index.programmesByChannel.get(matchedId);
      if (progs && progs.length > 0) return progs;
    }
  }

  // 2. Normalized name lookup (handles multi-pipe prefixes, quality tags, and "east" feed qualifier)
  const baseCandidates = buildChannelNameCandidates(channel.name);
  if (baseCandidates.length === 0) return [];

  const allCandidates = [...baseCandidates];
  if (baseCandidates[0] && !/\b(east|west|pacific)\b/.test(baseCandidates[0])) {
    allCandidates.push(`${baseCandidates[0]} east`);
  }

  for (const cand of allCandidates) {
    const exactMatch = tryChannelIds(index.byNormName.get(cand));
    if (exactMatch.length > 0) return exactMatch;

    const compact = cand.replace(/\s+/g, '');
    if (compact.length >= 4) {
      const compactMatch = tryChannelIds(index.byCompactName.get(compact));
      if (compactMatch.length > 0) return compactMatch;
    }
  }

  return [];
}

function selectCandidateFeeds(channel: LiveChannelSummary): string[] {
  const urls: string[] = [];
  const pushUnique = (u: string) => {
    if (!urls.includes(u)) urls.push(u);
  };

  const upper = `${channel.name} ${channel.categoryName}`.toUpperCase();
  if (/\bBEIN\b/.test(upper)) {
    pushUnique('https://epgshare01.online/epgshare01/epg_ripper_BEIN1.xml.gz');
  }
  if (/\bAL\s*JAZEERA\b/.test(upper)) {
    pushUnique('https://epgshare01.online/epgshare01/epg_ripper_ALJAZEERA1.xml.gz');
  }

  const country = inferCountryCode(channel.name, channel.categoryName);
  if (country && COUNTRY_TO_FEEDS[country]) {
    for (const feed of COUNTRY_TO_FEEDS[country]) {
      pushUnique(feed);
    }
  }

  if (
    /\b(SPORT|SPORTS|ESPN|FOX SPORTS|SKY SPORTS|TNT SPORTS|DAZN|EUROSPORT|NBA|NFL|MLB|UFC|SONY TEN|STAR SPORTS|PTV SPORTS)\b/.test(
      upper,
    )
  ) {
    pushUnique('https://epgshare01.online/epgshare01/epg_ripper_US_SPORTS1.xml.gz');
    pushUnique('https://epgshare01.online/epgshare01/epg_ripper_THESPORTPLUS1.xml.gz');
  }

  if (urls.length === 0) {
    pushUnique('https://epgshare01.online/epgshare01/epg_ripper_UK1.xml.gz');
    pushUnique('https://epgshare01.online/epgshare01/epg_ripper_US2.xml.gz');
    pushUnique('https://epgshare01.online/epgshare01/epg_ripper_IN1.xml.gz');
  }

  return urls.slice(0, 3);
}

export async function findFallbackChannelEpg(
  channel: LiveChannelSummary,
  allChannels: LiveChannelSummary[],
  getProviderEpg: (streamId: string, nowSec: number) => Promise<EpgProgram[]>,
  nowSec: number,
): Promise<{ programs: EpgProgram[]; source: 'provider' | 'external' | 'none' }> {
  const cacheKey = `${channel.streamId}:${channel.name.toLowerCase()}`;
  const cached = channelFallbackCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return {
      programs: cached.programs.map((p) => ({
        ...p,
        isNowPlaying: p.startTimestamp <= nowSec && p.stopTimestamp > nowSec,
      })),
      source: cached.source,
    };
  }

  const targetNorm = normalizeChannelName(channel.name);

  // 1. Check sibling provider channels with the exact same normalized name that DO carry an epgChannelId
  if (targetNorm.length >= 2) {
    const siblings = allChannels
      .filter(
        (c) =>
          c.streamId !== channel.streamId &&
          Boolean(c.epgChannelId) &&
          normalizeChannelName(c.name) === targetNorm,
      )
      .slice(0, 2);

    for (const sib of siblings) {
      const sibPrograms = await getProviderEpg(sib.streamId, nowSec).catch(() => []);
      if (sibPrograms.length > 0) {
        storeChannelFallback(cacheKey, sibPrograms, 'provider', CHANNEL_HIT_TTL_MS);
        return { programs: sibPrograms, source: 'provider' };
      }
    }
  }

  // 2. Check regional/thematic XMLTV feeds strictly matching this channel's region/topic
  const candidateFeeds = selectCandidateFeeds(channel);
  const deadline = Date.now() + 9500;

  for (const feedUrl of candidateFeeds) {
    if (Date.now() >= deadline) break;
    const index = await loadXmltvFeed(feedUrl);
    if (!index) continue;
    const found = matchChannelInIndex(index, channel);
    if (found.length > 0) {
      const programs = refreshProgramFlags(found, nowSec);
      storeChannelFallback(cacheKey, programs, 'external', CHANNEL_HIT_TTL_MS);
      return { programs, source: 'external' };
    }
  }

  storeChannelFallback(cacheKey, [], 'none', CHANNEL_MISS_TTL_MS);
  return { programs: [], source: 'none' };
}

function refreshProgramFlags(programs: EpgProgram[], nowSec: number): EpgProgram[] {
  return programs.map((p) => ({
    ...p,
    isNowPlaying: p.startTimestamp <= nowSec && p.stopTimestamp > nowSec,
  }));
}

function storeChannelFallback(
  key: string,
  programs: EpgProgram[],
  source: 'provider' | 'external' | 'none',
  ttlMs: number,
): void {
  if (channelFallbackCache.size >= MAX_CHANNEL_FALLBACK_CACHE) {
    const oldest = channelFallbackCache.keys().next().value;
    if (oldest) channelFallbackCache.delete(oldest);
  }
  channelFallbackCache.set(key, {
    programs,
    source,
    expiresAt: Date.now() + ttlMs,
  });
}

export function clearEpgFallbackCaches(): void {
  channelFallbackCache.clear();
}
