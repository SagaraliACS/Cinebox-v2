import { useEffect, useState } from 'react';
import { Clapperboard, Film } from 'lucide-react';
import { fetchChannelLogo, fetchImdbPoster } from '../services/api';
import { proxiedImageUrl } from '../utils/format';

interface MediaImageProps {
  src?: string;
  alt: string;
  title?: string;
  year?: string | null;
  category?: string;
  variant?: 'poster' | 'backdrop' | 'logo';
  width?: number;
  className?: string;
  imgClassName?: string;
  loading?: 'lazy' | 'eager';
}

const imdbPosterCache = new Map<string, string | null>();
const channelLogoCache = new Map<string, string | null>();

const DEFAULT_WIDTH_BY_VARIANT: Record<'poster' | 'backdrop' | 'logo', number> = {
  poster: 380,
  logo: 240,
  backdrop: 1280,
};

export function MediaImage({
  src,
  alt,
  title,
  year,
  category,
  variant = 'poster',
  width,
  className = '',
  imgClassName = 'h-full w-full object-cover',
  loading = 'lazy',
}: MediaImageProps) {
  const [failedOriginal, setFailedOriginal] = useState(false);
  const [fallbackUrl, setFallbackUrl] = useState<string | null>(null);
  const [failedFallback, setFailedFallback] = useState(false);
  const [failedAppIcon, setFailedAppIcon] = useState(false);
  const [isLoaded, setIsLoaded] = useState(false);

  const targetWidth = width ?? DEFAULT_WIDTH_BY_VARIANT[variant];
  const proxiedOriginal = proxiedImageUrl(src, { width: targetWidth });
  const queryTitle = (title || alt || '').trim();

  useEffect(() => {
    setFailedOriginal(false);
    setFailedFallback(false);
    setFallbackUrl(null);
    setFailedAppIcon(false);
    setIsLoaded(false);
  }, [src, queryTitle, category]);

  // If original source is missing or fails, fetch IMDb poster (for movies/series) or authentic channel logo (for logos)
  useEffect(() => {
    const needsFallback = (!proxiedOriginal || failedOriginal) && !failedFallback && !fallbackUrl;
    if (!needsFallback || queryTitle.length < 2) return;

    if (variant === 'logo') {
      const cacheKey = `${queryTitle.toLowerCase()}__${(category || '').toLowerCase()}`;
      if (channelLogoCache.has(cacheKey)) {
        const cached = channelLogoCache.get(cacheKey);
        if (cached) {
          setFallbackUrl(cached);
        } else {
          setFailedFallback(true);
        }
        return;
      }

      const controller = new AbortController();
      fetchChannelLogo(queryTitle, category, controller.signal)
        .then((res) => {
          channelLogoCache.set(cacheKey, res.logo || null);
          if (res.logo) {
            setFallbackUrl(res.logo);
          } else {
            setFailedFallback(true);
          }
        })
        .catch((err) => {
          if (err?.name === 'AbortError') return;
          setFailedFallback(true);
        });

      return () => controller.abort();
    }

    const cacheKey = `${queryTitle.toLowerCase()}_${year || ''}`;
    if (imdbPosterCache.has(cacheKey)) {
      const cached = imdbPosterCache.get(cacheKey);
      if (cached) {
        setFallbackUrl(cached);
      } else {
        setFailedFallback(true);
      }
      return;
    }

    const controller = new AbortController();
    fetchImdbPoster(queryTitle, year, controller.signal)
      .then((res) => {
        imdbPosterCache.set(cacheKey, res.poster || null);
        if (res.poster) {
          setFallbackUrl(res.poster);
        } else {
          setFailedFallback(true);
        }
      })
      .catch((err) => {
        if (err?.name === 'AbortError') return;
        setFailedFallback(true);
      });

    return () => controller.abort();
  }, [proxiedOriginal, failedOriginal, variant, queryTitle, year, category, failedFallback, fallbackUrl]);

  const activeSrc =
    !failedOriginal && proxiedOriginal
      ? proxiedOriginal
      : fallbackUrl
        ? proxiedImageUrl(fallbackUrl, { width: targetWidth })
        : null;

  const FallbackIcon = variant === 'backdrop' ? Clapperboard : Film;
  const hasTransparentBg = /\bbg-transparent\b/.test(className);

  return (
    <div
      className={`relative flex items-center justify-center overflow-hidden ${
        hasTransparentBg
          ? ''
          : 'bg-gradient-to-br from-slate-100 via-slate-50 to-indigo-50/70 dark:from-[#141926] dark:via-[#0F131D] dark:to-[#090B10]'
      } ${className}`}
    >
      {activeSrc ? (
        <img
          src={activeSrc}
          alt={alt}
          loading={loading}
          decoding="async"
          fetchPriority={loading === 'eager' ? 'high' : 'auto'}
          referrerPolicy="no-referrer"
          onLoad={() => setIsLoaded(true)}
          onError={() => {
            setIsLoaded(false);
            if (!failedOriginal && proxiedOriginal) {
              setFailedOriginal(true);
            } else {
              setFailedFallback(true);
            }
          }}
          className={`${imgClassName} transition-opacity duration-200 ${
            isLoaded ? 'opacity-100' : 'opacity-0'
          }`}
        />
      ) : variant === 'logo' ? (
        <div className="flex h-full w-full items-center justify-center select-none">
          {!failedAppIcon ? (
            <img
              src="/icon.svg"
              alt={alt}
              loading={loading}
              decoding="async"
              referrerPolicy="no-referrer"
              onError={() => setFailedAppIcon(true)}
              className="h-[58%] w-auto aspect-square object-contain rounded-xl opacity-95 drop-shadow-sm transition-transform duration-300 group-hover:scale-[1.06]"
            />
          ) : (
            <svg
              viewBox="0 0 512 512"
              fill="none"
              aria-label={alt}
              className="h-[58%] w-auto aspect-square rounded-xl opacity-95 drop-shadow-sm"
            >
              <rect width="512" height="512" rx="112" fill="#131826" />
              <rect
                x="96"
                y="112"
                width="320"
                height="288"
                rx="56"
                fill="#E11D48"
                fillOpacity="0.16"
                stroke="#FB7185"
                strokeOpacity="0.4"
                strokeWidth="8"
              />
              <path
                d="M214 188.5C214 179.9 223.4 174.6 230.8 179L335.2 241.5C342.4 245.8 342.4 256.2 335.2 260.5L230.8 323C223.4 327.4 214 322.1 214 313.5V188.5Z"
                fill="#E11D48"
              />
            </svg>
          )}
        </div>
      ) : (
        <div className="flex h-full w-full flex-col items-center justify-center p-3 text-center select-none">
          <FallbackIcon
            className="mb-1.5 h-6 w-6 shrink-0 text-slate-400 dark:text-slate-600"
            strokeWidth={1.6}
          />
          <span className="line-clamp-2 text-[11px] font-medium leading-snug text-slate-500 dark:text-slate-400">
            {alt}
          </span>
        </div>
      )}
    </div>
  );
}
