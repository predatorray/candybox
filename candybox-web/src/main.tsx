import '@fontsource/inter/400.css';
import '@fontsource/inter/500.css';
import '@fontsource/inter/600.css';
import '@fontsource/inter/700.css';

import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import App from './App';
import { ThemeProvider } from './theme/ThemeProvider';

// 5s polling for read-only views matches WEB_DASHBOARD_PLAN.md. Keep the cache aggressively warm
// across tab navigation — the data is small, network-cheap, and operators expect "now-ish" values.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchInterval: 5_000,
      staleTime: 4_000,
      refetchOnWindowFocus: true,
      retry: 1,
    },
  },
});

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    {/* basename is /ui because the admin API mounts the SPA there; tests use the dev server
        at / so we read from the runtime path the bundle was served at, not a hardcoded prefix. */}
    <BrowserRouter basename={document.querySelector('base')?.getAttribute('href') ?? '/ui/'}>
      <QueryClientProvider client={queryClient}>
        <ThemeProvider>
          <App />
        </ThemeProvider>
      </QueryClientProvider>
    </BrowserRouter>
  </StrictMode>,
);
