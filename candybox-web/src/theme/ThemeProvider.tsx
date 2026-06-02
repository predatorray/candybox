import { createContext, useContext, useMemo, useState, useEffect, ReactNode } from 'react';
import { CssBaseline, ThemeProvider as MuiThemeProvider } from '@mui/material';
import { buildTheme, type Density, type Mode } from './theme';

type Settings = {
  mode: Mode;
  density: Density;
  setMode: (m: Mode) => void;
  setDensity: (d: Density) => void;
  toggleMode: () => void;
};

const Ctx = createContext<Settings | null>(null);

// The persisted keys are stable across versions; bump them only if the shape changes.
const MODE_KEY = 'candybox.theme.mode';
const DENSITY_KEY = 'candybox.theme.density';

function readPersisted<T extends string>(key: string, allowed: readonly T[], fallback: T): T {
  if (typeof window === 'undefined') return fallback;
  const raw = window.localStorage.getItem(key);
  return (allowed as readonly string[]).includes(raw ?? '') ? (raw as T) : fallback;
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setMode] = useState<Mode>(() => readPersisted(MODE_KEY, ['dark', 'light'], 'dark'));
  const [density, setDensity] = useState<Density>(() =>
    readPersisted(DENSITY_KEY, ['comfortable', 'compact'], 'comfortable'),
  );

  useEffect(() => {
    window.localStorage.setItem(MODE_KEY, mode);
  }, [mode]);
  useEffect(() => {
    window.localStorage.setItem(DENSITY_KEY, density);
  }, [density]);

  const theme = useMemo(() => buildTheme(mode, density), [mode, density]);
  const value = useMemo<Settings>(
    () => ({
      mode,
      density,
      setMode,
      setDensity,
      toggleMode: () => setMode((m) => (m === 'dark' ? 'light' : 'dark')),
    }),
    [mode, density],
  );

  return (
    <Ctx.Provider value={value}>
      <MuiThemeProvider theme={theme}>
        <CssBaseline enableColorScheme />
        {children}
      </MuiThemeProvider>
    </Ctx.Provider>
  );
}

export function useThemeSettings(): Settings {
  const v = useContext(Ctx);
  if (!v) throw new Error('useThemeSettings must be used inside ThemeProvider');
  return v;
}
