import { useEffect, useMemo, useRef, useState } from 'react';
import {
  ChevronLeft,
  ChevronRight,
  Clock,
  Film,
  Info,
  Play,
  Radio,
  Star,
  Volume2,
  VolumeX,
  X,
} from 'lucide-react';
import { extractYouTubeId, proxiedImageUrl } from '../utils/format';
import { MediaImage } from './MediaImage';

export interface VodHeroSlide {
  kind: 'movie' | 'series';
  id: string;
  kicker: string;
  title: string;
  categoryName?: string;
  language?: string;
  rating?: number;
  year?: string | null;
  metaText?: string;
  qualityTag?: string | null;
  extension?: string;
  genre?: string;
  plot?: string;
  director?: string;
  cast?: string;
  releaseDate?: string;
  poster: string;
  backdrop: string;
  youtubeTrailer?: string;
  primaryActionLabel: string;
  secondaryActionLabel: string;
  onPrimaryAction: () => void;
  onSecondaryAction: () => void;
}

export interface LiveHeroSlide {
  kind: 'live';
  id: string;
  kicker: string;
  title: string;
  channelNumber?: number;
  categoryName?: string;
  logo: string;
  hasArchive?: boolean;
  hasEpg?: boolean;
  nowPlayingTitle?: string;
  nowPlayingTime?: string;
  nowPlayingDescription?: string;
  nowPlayingProgress?: number;
  nextProgramTitle?: string;
  nextProgramTime?: string;
  primaryActionLabel: string;
  secondaryActionLabel?: string;
  onPrimaryAction: () => void;
  onSecondaryAction?: () => void;
}

export type HeroSlide = VodHeroSlide | LiveHeroSlide;

interface SpotlightHeroSliderProps {
  slides: HeroSlide[];
  ariaLabel: string;
  hoverPreviewEnabled?: boolean;
  compact?: boolean;
}

function formatCastList(raw?: string): string {
  if (!raw) return '';
  const names = raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  if (names.length === 0) return '';
  if (names.length <= 3) return names.join(', ');
  return `${names.slice(0, 3).join(', ')}`;
}

export function SpotlightHeroSlider({
  slides,
  ariaLabel,
  compact = false,
}: SpotlightHeroSliderProps) {
  const [heroIdx, setHeroIdx] = useState(0);
  const [heroPaused, setHeroPaused] = useState(false);
  const [trailerOpen, setTrailerOpen] = useState(false);
  const [trailerMuted, setTrailerMuted] = useState(true);
  const trailerIframeRef = useRef<HTMLIFrameElement | null>(null);

  const safeIdx = slides.length > 0 ? heroIdx % slides.length : 0;
  const activeSlide = slides[safeIdx] || slides[0];

  useEffect(() => {
    setHeroIdx(0);
    setTrailerOpen(false);
    setTrailerMuted(true);
  }, [slides.length, slides[0]?.id]);

  useEffect(() => {
    setTrailerOpen(false);
    setTrailerMuted(true);
  }, [safeIdx]);

  useEffect(() => {
    if (slides.length <= 1 || heroPaused || trailerOpen) return;
    const timer = window.setInterval(() => {
      setHeroIdx((prev) => (prev + 1) % slides.length);
    }, 8500);
    return () => window.clearInterval(timer);
  }, [slides.length, heroPaused, trailerOpen, safeIdx]);

  // Preload next slide image so slide transitions render instantaneously
  useEffect(() => {
    if (slides.length <= 1 || typeof window === 'undefined') return;
    const nextSlide = slides[(safeIdx + 1) % slides.length];
    if (!nextSlide) return;
    const rawImg =
      nextSlide.kind === 'live'
        ? nextSlide.logo
        : nextSlide.backdrop || nextSlide.poster;
    const width = nextSlide.kind === 'live' ? 240 : 1280;
    const src = proxiedImageUrl(rawImg, { width });
    if (!src) return;
    const preloader = new window.Image();
    preloader.decoding = 'async';
    preloader.src = src;
  }, [slides, safeIdx]);

  const vodSlide = activeSlide?.kind === 'live' ? null : activeSlide;
  const youtubeTrailerId = useMemo(
    () => extractYouTubeId(vodSlide?.youtubeTrailer),
    [vodSlide?.youtubeTrailer],
  );

  if (!activeSlide) return null;

  const handlePrev = () => {
    if (slides.length <= 1) return;
    setHeroIdx((prev) => (prev - 1 + slides.length) % slides.length);
  };

  const handleNext = () => {
    if (slides.length <= 1) return;
    setHeroIdx((prev) => (prev + 1) % slides.length);
  };

  const handleToggleTrailerMute = () => {
    setTrailerMuted((prev) => {
      const next = !prev;
      const win = trailerIframeRef.current?.contentWindow;
      if (win) {
        try {
          win.postMessage(
            JSON.stringify({
              event: 'command',
              func: next ? 'mute' : 'unMute',
              args: [],
            }),
            '*',
          );
        } catch {
          // Ignore cross-origin restrictions
        }
      }
      return next;
    });
  };

  const minHeightClass = compact
    ? 'min-h-[340px] sm:min-h-[380px] lg:min-h-[410px]'
    : 'min-h-[430px] sm:min-h-[470px] lg:min-h-[515px]';

  const hasWideBackdrop =
    vodSlide !== null &&
    Boolean(vodSlide.backdrop) &&
    vodSlide.backdrop !== vodSlide.poster;

  const castSummary = vodSlide ? formatCastList(vodSlide.cast) : '';
  const directorSummary = vodSlide?.director?.trim() || '';
  const rawQualityBadge =
    vodSlide?.qualityTag?.trim().toUpperCase() ||
    (vodSlide?.extension && vodSlide.extension.length <= 4
      ? vodSlide.extension.toUpperCase()
      : '');
  const qualityBadge =
    rawQualityBadge && rawQualityBadge !== vodSlide?.metaText?.trim().toUpperCase()
      ? rawQualityBadge
      : '';

  return (
    <section
      aria-roledescription="carousel"
      aria-label={ariaLabel}
      onMouseEnter={() => setHeroPaused(true)}
      onMouseLeave={() => setHeroPaused(false)}
    >
      <div className="relative overflow-hidden rounded-2xl border border-slate-300/80 dark:border-white/[0.1] bg-[#0A0D14] text-white shadow-[0_20px_48px_-22px_rgba(15,23,42,0.38)] dark:shadow-[0_24px_60px_-24px_rgba(0,0,0,0.85)]">
        {/* Main Stage */}
        <div className={`relative ${minHeightClass} flex flex-col justify-between`}>
          {/* Background Media Layer */}
          {activeSlide.kind === 'live' ? (
            <div
              aria-hidden="true"
              className="pointer-events-none absolute inset-0 z-0 overflow-hidden bg-[#0A0D14]"
            >
              <div className="absolute inset-0 bg-[radial-gradient(720px_380px_at_82%_34%,rgba(225,29,72,0.22),rgba(14,165,233,0.14),transparent_72%)]" />
              <div className="absolute inset-0 bg-gradient-to-r from-[#0A0D14] via-[#0A0D14]/88 to-transparent" />
            </div>
          ) : (
            <div className="absolute inset-0 z-0 overflow-hidden bg-[#0A0D14]">
              {slides.map((item, idx) => {
                if (item.kind === 'live') return null;
                const isCurrent = idx === safeIdx;
                const itemHasBackdrop = Boolean(item.backdrop) && item.backdrop !== item.poster;
                return (
                  <div
                    key={item.id}
                    aria-hidden={!isCurrent}
                    className={`absolute inset-0 transition-all duration-700 ease-out ${
                      isCurrent
                        ? 'opacity-100 scale-100'
                        : 'opacity-0 scale-[1.03] pointer-events-none'
                    }`}
                  >
                    <MediaImage
                      src={item.backdrop || item.poster}
                      alt={item.title}
                      title={item.title}
                      year={item.year ?? null}
                      variant="backdrop"
                      loading={idx === 0 ? 'eager' : 'lazy'}
                      className="h-full w-full bg-[#0A0D14]"
                      imgClassName={
                        itemHasBackdrop
                          ? 'h-full w-full object-cover object-center sm:object-[72%_25%]'
                          : 'h-full w-full object-cover object-center scale-110 blur-2xl opacity-55'
                      }
                    />
                  </div>
                );
              })}

              {/* Calibrated Directional Scrims: keep artwork bright on the right, text crisp on the left */}
              <div
                aria-hidden="true"
                className="absolute inset-0 bg-gradient-to-t from-[#0A0D14] via-[#0A0D14]/75 to-[#0A0D14]/25 sm:bg-gradient-to-r sm:from-[#0A0D14] sm:via-[#0A0D14]/82 sm:via-[46%] sm:to-[#0A0D14]/10"
              />
              <div
                aria-hidden="true"
                className="hidden sm:block absolute inset-0 bg-gradient-to-t from-[#0A0D14] via-transparent to-[#0A0D14]/35"
              />

              {/* Inline Cinema Trailer Overlay */}
              {trailerOpen && youtubeTrailerId && (
                <div className="absolute inset-0 z-20 bg-black">
                  <iframe
                    ref={trailerIframeRef}
                    title={`${activeSlide.title} official trailer`}
                    src={`https://www.youtube-nocookie.com/embed/${encodeURIComponent(
                      youtubeTrailerId,
                    )}?autoplay=1&mute=1&controls=1&playsinline=1&rel=0&modestbranding=1&enablejsapi=1`}
                    allow="autoplay; encrypted-media; picture-in-picture"
                    allowFullScreen
                    className="h-full w-full border-0"
                  />
                  <div className="pointer-events-none absolute inset-x-0 top-0 z-30 flex items-center justify-between gap-3 bg-gradient-to-b from-black/85 via-black/45 to-transparent px-4 sm:px-6 py-3">
                    <span className="text-xs font-semibold text-white truncate">
                      Trailer · {activeSlide.title}
                    </span>
                    <div className="pointer-events-auto flex items-center gap-2 shrink-0">
                      <button
                        type="button"
                        onClick={handleToggleTrailerMute}
                        aria-label={trailerMuted ? 'Unmute trailer' : 'Mute trailer'}
                        className="inline-flex h-8 items-center gap-1.5 rounded-lg border border-white/20 bg-black/75 px-2.5 text-xs font-medium text-white hover:bg-white/15 transition-colors"
                      >
                        {trailerMuted ? (
                          <VolumeX className="h-3.5 w-3.5" />
                        ) : (
                          <Volume2 className="h-3.5 w-3.5" />
                        )}
                        <span>{trailerMuted ? 'Unmute' : 'Mute'}</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => setTrailerOpen(false)}
                        aria-label="Close trailer preview"
                        className="inline-flex h-8 items-center gap-1 rounded-lg border border-white/20 bg-black/75 px-2.5 text-xs font-semibold text-white hover:bg-[#E11D48] hover:border-[#E11D48] transition-colors"
                      >
                        <X className="h-3.5 w-3.5" />
                        <span>Close</span>
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Foreground Content */}
          <div className="relative z-10 flex flex-1 items-center px-5 sm:px-8 lg:px-10 py-7 sm:py-9">
            {activeSlide.kind === 'live' ? (
              <div className="flex w-full items-center justify-between gap-8 lg:gap-12">
                <div key={activeSlide.id} className="max-w-2xl flex-1 min-w-0">
                  <div className="flex items-start gap-3.5 sm:gap-4">
                    <div className="lg:hidden h-14 w-20 shrink-0 overflow-hidden rounded-lg border border-white/15 bg-[#131824] p-2">
                      <MediaImage
                        src={activeSlide.logo}
                        alt={activeSlide.title}
                        title={activeSlide.title}
                        category={activeSlide.categoryName}
                        variant="logo"
                        className="h-full w-full"
                        imgClassName="h-full w-full object-contain"
                      />
                    </div>

                    <div className="min-w-0 flex-1">
                      <p className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs font-semibold tracking-wide text-rose-400">
                        <span className="inline-flex items-center gap-1.5">
                          <Radio className="h-3.5 w-3.5 shrink-0" />
                          <span>{activeSlide.kicker}</span>
                        </span>
                        {activeSlide.categoryName && (
                          <>
                            <span aria-hidden="true" className="text-white/30">
                              ·
                            </span>
                            <span className="text-slate-200 font-medium">
                              {activeSlide.categoryName}
                            </span>
                          </>
                        )}
                        {activeSlide.channelNumber ? (
                          <>
                            <span aria-hidden="true" className="text-white/30">
                              ·
                            </span>
                            <span className="font-mono text-slate-300 tabular-nums">
                              CH {activeSlide.channelNumber}
                            </span>
                          </>
                        ) : null}
                      </p>

                      <h1 className="mt-2 font-display text-2xl sm:text-4xl lg:text-[2.5rem] font-bold tracking-tight text-white text-balance leading-[1.08]">
                        {activeSlide.title}
                      </h1>
                    </div>
                  </div>

                  {activeSlide.nowPlayingTitle ? (
                    <div className="mt-4 max-w-xl space-y-2.5">
                      <div className="flex flex-wrap items-baseline justify-between gap-2">
                        <p className="text-sm sm:text-base font-semibold text-slate-100 line-clamp-1">
                          {activeSlide.nowPlayingTitle}
                        </p>
                        {activeSlide.nowPlayingTime && (
                          <span className="inline-flex items-center gap-1 font-mono text-xs text-slate-300 tabular-nums shrink-0">
                            <Clock className="h-3 w-3" />
                            <span>{activeSlide.nowPlayingTime}</span>
                          </span>
                        )}
                      </div>

                      {typeof activeSlide.nowPlayingProgress === 'number' && (
                        <div className="h-1.5 w-full overflow-hidden rounded-full bg-white/15">
                          <div
                            className="h-full rounded-full bg-[#E11D48] transition-all duration-300"
                            style={{
                              width: `${Math.max(4, Math.min(100, activeSlide.nowPlayingProgress))}%`,
                            }}
                          />
                        </div>
                      )}

                      {activeSlide.nowPlayingDescription && (
                        <p className="text-xs sm:text-sm leading-relaxed text-slate-300 line-clamp-2">
                          {activeSlide.nowPlayingDescription}
                        </p>
                      )}

                      {activeSlide.nextProgramTitle && (
                        <p className="text-xs text-slate-400 line-clamp-1">
                          <span className="font-semibold text-slate-200">Up Next:</span>{' '}
                          {activeSlide.nextProgramTitle}
                          {activeSlide.nextProgramTime ? ` · ${activeSlide.nextProgramTime}` : ''}
                        </p>
                      )}
                    </div>
                  ) : (
                    <p className="mt-3 text-sm sm:text-[15px] leading-relaxed text-slate-300 max-w-xl">
                      Live broadcast stream
                      {activeSlide.categoryName ? ` in ${activeSlide.categoryName}` : ''}
                      {activeSlide.hasArchive ? ' · Catch-up replay available' : ''}
                      {activeSlide.hasEpg ? ' · Interactive EPG guide supported' : ''}.
                    </p>
                  )}

                  <div className="mt-6 flex flex-wrap items-center gap-3">
                    <button
                      type="button"
                      onClick={activeSlide.onPrimaryAction}
                      className="inline-flex h-10 sm:h-11 items-center gap-2 rounded-lg bg-[#E11D48] hover:bg-[#BE123C] px-5 sm:px-6 text-sm font-semibold text-white shadow-lg shadow-[#E11D48]/25 transition-colors whitespace-nowrap shrink-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                    >
                      <Play className="h-4 w-4 fill-current" />
                      <span>{activeSlide.primaryActionLabel}</span>
                    </button>

                    {activeSlide.onSecondaryAction && activeSlide.secondaryActionLabel && (
                      <button
                        type="button"
                        onClick={activeSlide.onSecondaryAction}
                        className="inline-flex h-10 sm:h-11 items-center gap-2 rounded-lg border border-white/20 bg-white/10 hover:bg-white/18 px-4 sm:px-5 text-sm font-semibold text-white transition-colors whitespace-nowrap shrink-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                      >
                        <Info className="h-4 w-4" />
                        <span>{activeSlide.secondaryActionLabel}</span>
                      </button>
                    )}
                  </div>
                </div>

                {/* Desktop Live Channel Showcase */}
                <div className="hidden lg:flex flex-col items-end shrink-0">
                  <button
                    type="button"
                    onClick={activeSlide.onPrimaryAction}
                    aria-label={`Watch ${activeSlide.title} live`}
                    className="group relative flex w-72 xl:w-80 flex-col overflow-hidden rounded-xl border border-white/15 bg-[#121722] shadow-[0_24px_48px_-14px_rgba(0,0,0,0.85)] transition-transform duration-300 hover:-translate-y-0.5 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                  >
                    <div className="relative flex aspect-[16/10] w-full items-center justify-center p-6">
                      <MediaImage
                        src={activeSlide.logo}
                        alt={activeSlide.title}
                        title={activeSlide.title}
                        category={activeSlide.categoryName}
                        variant="logo"
                        className="h-full w-full"
                        imgClassName="h-full w-full object-contain transition-transform duration-300 group-hover:scale-105"
                      />
                    </div>
                    <div className="flex items-center justify-between gap-2 border-t border-white/[0.08] bg-[#0D111B] px-4 py-2.5 text-[11px] font-medium text-slate-300">
                      <span className="truncate">
                        {activeSlide.hasArchive
                          ? 'Live + Catch-up'
                          : activeSlide.hasEpg
                            ? 'Live + EPG'
                            : 'Live Stream'}
                      </span>
                      <span className="font-mono text-rose-400 font-semibold shrink-0">
                        Tune In
                      </span>
                    </div>
                  </button>
                </div>
              </div>
            ) : (
              <div className="flex w-full items-center justify-between gap-8 lg:gap-12">
                <div key={activeSlide.id} className="max-w-2xl flex-1 min-w-0">
                  {/* Kicker & Context */}
                  <p className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs font-semibold tracking-wide text-rose-400">
                    <span>{activeSlide.kicker}</span>
                    {activeSlide.categoryName && (
                      <>
                        <span aria-hidden="true" className="text-white/30">
                          ·
                        </span>
                        <span className="text-slate-200 font-medium">
                          {activeSlide.categoryName}
                        </span>
                      </>
                    )}
                    {activeSlide.language && (
                      <>
                        <span aria-hidden="true" className="text-white/30">
                          ·
                        </span>
                        <span className="text-sky-300 font-medium">
                          {activeSlide.language}
                        </span>
                      </>
                    )}
                  </p>

                  {/* Title */}
                  <h1 className="mt-2.5 font-display text-2xl sm:text-4xl lg:text-[2.75rem] font-bold tracking-tight text-white text-balance leading-[1.08]">
                    {activeSlide.title}
                  </h1>

                  {/* Structured Technical & Editorial Specs */}
                  <div className="mt-3 flex flex-wrap items-center gap-x-2.5 gap-y-1.5 text-xs sm:text-sm text-slate-200 tabular-nums">
                    {typeof activeSlide.rating === 'number' && activeSlide.rating > 0 && (
                      <span className="inline-flex items-center gap-1 font-semibold text-amber-300">
                        <Star className="h-3.5 w-3.5 fill-amber-400 text-amber-400" />
                        <span>{activeSlide.rating.toFixed(1)}</span>
                      </span>
                    )}

                    {activeSlide.year && (
                      <>
                        {typeof activeSlide.rating === 'number' && activeSlide.rating > 0 && (
                          <span aria-hidden="true" className="text-white/30">
                            ·
                          </span>
                        )}
                        <span className="font-medium text-white">{activeSlide.year}</span>
                      </>
                    )}

                    {activeSlide.metaText && (
                      <>
                        <span aria-hidden="true" className="text-white/30">
                          ·
                        </span>
                        <span className="text-slate-200">{activeSlide.metaText}</span>
                      </>
                    )}

                    {qualityBadge && (
                      <>
                        <span aria-hidden="true" className="text-white/30">
                          ·
                        </span>
                        <span className="rounded border border-white/25 bg-white/[0.08] px-1.5 py-0.5 font-mono text-[10px] font-semibold tracking-wider text-white">
                          {qualityBadge}
                        </span>
                      </>
                    )}

                    {activeSlide.genre && (
                      <>
                        <span aria-hidden="true" className="text-white/30">
                          ·
                        </span>
                        <span className="text-slate-300 line-clamp-1">{activeSlide.genre}</span>
                      </>
                    )}
                  </div>

                  {/* Synopsis */}
                  {activeSlide.plot && (
                    <p className="mt-3.5 text-sm sm:text-[15px] leading-relaxed text-slate-200/90 line-clamp-2 sm:line-clamp-3 max-w-xl">
                      {activeSlide.plot}
                    </p>
                  )}

                  {/* Director & Cast Credits */}
                  {(directorSummary || castSummary) && (
                    <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-300 max-w-xl">
                      {directorSummary && (
                        <p className="truncate">
                          <span className="text-slate-400">Director:</span>{' '}
                          <span className="font-medium text-slate-100">{directorSummary}</span>
                        </p>
                      )}
                      {castSummary && (
                        <p className="truncate">
                          <span className="text-slate-400">Starring:</span>{' '}
                          <span className="font-medium text-slate-100">{castSummary}</span>
                        </p>
                      )}
                    </div>
                  )}

                  {/* Primary Action Bar */}
                  <div className="mt-5 sm:mt-6 flex flex-wrap items-center gap-2.5 sm:gap-3">
                    <button
                      type="button"
                      onClick={activeSlide.onPrimaryAction}
                      className="inline-flex h-10 sm:h-11 items-center gap-2 rounded-lg bg-[#E11D48] hover:bg-[#BE123C] px-5 sm:px-6 text-sm font-semibold text-white shadow-lg shadow-[#E11D48]/30 transition-colors whitespace-nowrap shrink-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48] focus-visible:ring-offset-2 focus-visible:ring-offset-[#0A0D14]"
                    >
                      <Play className="h-4 w-4 fill-current" />
                      <span>{activeSlide.primaryActionLabel}</span>
                    </button>

                    <button
                      type="button"
                      onClick={activeSlide.onSecondaryAction}
                      className="inline-flex h-10 sm:h-11 items-center gap-2 rounded-lg border border-white/20 bg-white/10 hover:bg-white/18 px-4 sm:px-5 text-sm font-semibold text-white transition-colors whitespace-nowrap shrink-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                    >
                      <Info className="h-4 w-4" />
                      <span>{activeSlide.secondaryActionLabel}</span>
                    </button>

                    {youtubeTrailerId && (
                      <button
                        type="button"
                        onClick={() => setTrailerOpen(true)}
                        className="inline-flex h-10 sm:h-11 items-center gap-2 rounded-lg border border-white/15 bg-black/45 hover:bg-white/15 px-4 text-sm font-medium text-slate-100 transition-colors whitespace-nowrap shrink-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                      >
                        <Film className="h-4 w-4 text-rose-400" />
                        <span>Trailer</span>
                      </button>
                    )}
                  </div>
                </div>

                {/* Portrait Poster Frame (only shown when slide has no separate widescreen backdrop so faces aren't cropped) */}
                {!hasWideBackdrop && (
                  <div className="hidden md:flex flex-col items-end shrink-0">
                    <button
                      type="button"
                      onClick={activeSlide.onSecondaryAction}
                      aria-label={`Inspect ${activeSlide.title}`}
                      className="group/poster relative aspect-[2/3] w-40 lg:w-48 overflow-hidden rounded-xl border border-white/20 bg-[#121722] shadow-[0_24px_48px_-14px_rgba(0,0,0,0.85)] transition-transform duration-300 hover:-translate-y-0.5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                    >
                      <MediaImage
                        src={activeSlide.poster || activeSlide.backdrop}
                        alt={activeSlide.title}
                        title={activeSlide.title}
                        year={activeSlide.year ?? null}
                        variant="poster"
                        className="h-full w-full"
                        imgClassName="h-full w-full object-cover transition-transform duration-300 group-hover/poster:scale-105"
                      />
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Unified Slide Selector Strip */}
          {slides.length > 1 && (
            <div className="relative z-10 border-t border-white/[0.1] bg-[#07090F]/85 backdrop-blur-md">
              <div className="flex items-center justify-between gap-4 px-5 sm:px-8 lg:px-10 py-2.5 sm:py-3">
                {/* Tablet / Desktop Named Slide Tabs */}
                <div className="hidden md:grid flex-1 grid-cols-2 lg:grid-cols-4 xl:grid-flow-col xl:auto-cols-fr gap-2 min-w-0">
                  {slides.map((item, idx) => {
                    const isActive = idx === safeIdx;
                    const subLabel =
                      item.kind === 'live'
                        ? item.nowPlayingTitle || item.categoryName || 'Live'
                        : [item.year, item.categoryName].filter(Boolean).join(' · ') || 'Featured';
                    return (
                      <button
                        key={item.id}
                        type="button"
                        onClick={() => setHeroIdx(idx)}
                        aria-label={`Show slide ${idx + 1}: ${item.title}`}
                        aria-current={isActive ? 'true' : undefined}
                        className={`group relative flex items-center gap-2.5 rounded-lg px-3 py-2 text-left transition-colors min-w-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48] ${
                          isActive
                            ? 'bg-white/[0.12] text-white'
                            : 'bg-transparent text-slate-400 hover:bg-white/[0.06] hover:text-slate-200'
                        }`}
                      >
                        <span
                          aria-hidden="true"
                          className={`absolute inset-x-2.5 top-0 h-0.5 rounded-full transition-colors ${
                            isActive ? 'bg-[#E11D48]' : 'bg-transparent'
                          }`}
                        />
                        <span
                          className={`font-mono text-[11px] font-semibold tabular-nums shrink-0 ${
                            isActive ? 'text-rose-400' : 'text-slate-500 group-hover:text-slate-300'
                          }`}
                        >
                          {String(idx + 1).padStart(2, '0')}
                        </span>
                        <div className="min-w-0 flex-1">
                          <p className="truncate text-xs font-semibold leading-tight">
                            {item.title}
                          </p>
                          <p className="mt-0.5 truncate text-[10px] text-slate-400 leading-tight">
                            {subLabel}
                          </p>
                        </div>
                      </button>
                    );
                  })}
                </div>

                {/* Mobile Compact Progress Bars */}
                <div className="flex md:hidden items-center gap-1.5 flex-1 min-w-0 overflow-x-auto [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
                  {slides.map((item, idx) => (
                    <button
                      key={item.id}
                      type="button"
                      onClick={() => setHeroIdx(idx)}
                      aria-label={`Show slide ${idx + 1}: ${item.title}`}
                      aria-current={idx === safeIdx ? 'true' : undefined}
                      className={`h-1.5 shrink-0 rounded-full transition-all duration-300 ${
                        idx === safeIdx
                          ? 'w-7 bg-[#E11D48]'
                          : 'w-2 bg-white/25 hover:bg-white/50'
                      }`}
                    />
                  ))}
                </div>

                {/* Counter & Arrow Controls */}
                <div className="flex items-center gap-2 shrink-0">
                  <span className="md:hidden font-mono text-xs font-medium text-slate-300 tabular-nums">
                    {String(safeIdx + 1).padStart(2, '0')} / {String(slides.length).padStart(2, '0')}
                  </span>
                  <div className="flex items-center gap-1.5">
                    <button
                      type="button"
                      onClick={handlePrev}
                      aria-label="Previous spotlight slide"
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/15 bg-white/[0.06] text-slate-200 transition-colors hover:border-white/30 hover:bg-white/15 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                    >
                      <ChevronLeft className="h-4 w-4" />
                    </button>
                    <button
                      type="button"
                      onClick={handleNext}
                      aria-label="Next spotlight slide"
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/15 bg-white/[0.06] text-slate-200 transition-colors hover:border-white/30 hover:bg-white/15 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
                    >
                      <ChevronRight className="h-4 w-4" />
                    </button>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </section>
  );
}
