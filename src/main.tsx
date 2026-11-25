import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { registerSW } from 'virtual:pwa-register';
import App from './App.tsx';
import './index.css';

if (typeof window !== 'undefined' && 'serviceWorker' in navigator) {
  const hadController = Boolean(navigator.serviceWorker.controller);
  let refreshing = false;
  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (!hadController || refreshing) return;
    try {
      const key = 'cinebox_sw_reloaded_at';
      const last = Number(window.sessionStorage.getItem(key) || 0);
      if (Date.now() - last < 15_000) return;
      window.sessionStorage.setItem(key, String(Date.now()));
    } catch {
      // Ignore sessionStorage restrictions
    }
    refreshing = true;
    window.location.reload();
  });
  registerSW({ immediate: true });
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
