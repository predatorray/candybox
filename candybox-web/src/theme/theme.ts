import { createTheme, type Theme } from '@mui/material/styles';

export type Mode = 'light' | 'dark';
export type Density = 'comfortable' | 'compact';

// Color choices: a deep slate base (dark mode default) with a warm "candy" accent. Avoids the
// generic Material primary blue so the dashboard doesn't read as "AI-generated MUI demo". The
// accent is consciously borrowed from the project's candy/syrup vocabulary.
const PALETTE = {
  light: {
    primary: '#7b3aed',
    secondary: '#ef476f',
    background: '#f6f6f8',
    paper: '#ffffff',
  },
  dark: {
    primary: '#a48cff',
    secondary: '#ff6b9a',
    background: '#0e0f13',
    paper: '#171922',
  },
} as const;

export function buildTheme(mode: Mode, density: Density): Theme {
  const palette = PALETTE[mode];
  const compact = density === 'compact';
  return createTheme({
    palette: {
      mode,
      primary: { main: palette.primary },
      secondary: { main: palette.secondary },
      background: { default: palette.background, paper: palette.paper },
    },
    shape: { borderRadius: 10 },
    typography: {
      fontFamily:
        'Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif',
      h1: { fontWeight: 700, letterSpacing: '-0.02em' },
      h2: { fontWeight: 700, letterSpacing: '-0.02em' },
      h3: { fontWeight: 600, letterSpacing: '-0.01em' },
      h4: { fontWeight: 600, letterSpacing: '-0.01em' },
      h5: { fontWeight: 600 },
      h6: { fontWeight: 600 },
      button: { textTransform: 'none', fontWeight: 600 },
    },
    components: {
      MuiAppBar: {
        defaultProps: { elevation: 0, color: 'transparent' },
        styleOverrides: {
          root: ({ theme }) => ({
            backdropFilter: 'saturate(180%) blur(6px)',
            background:
              theme.palette.mode === 'dark'
                ? 'rgba(14,15,19,0.7)'
                : 'rgba(246,246,248,0.75)',
            borderBottom: `1px solid ${theme.palette.divider}`,
          }),
        },
      },
      MuiCard: {
        defaultProps: { elevation: 0 },
        styleOverrides: {
          root: ({ theme }) => ({
            border: `1px solid ${theme.palette.divider}`,
          }),
        },
      },
      MuiButton: { defaultProps: { disableElevation: true } },
      MuiTable: { defaultProps: { size: compact ? 'small' : 'medium' } },
      MuiTableCell: {
        styleOverrides: {
          root: ({ theme }) => ({
            borderBottom: `1px solid ${theme.palette.divider}`,
          }),
          head: { fontWeight: 600 },
        },
      },
      MuiListItemButton: {
        styleOverrides: {
          root: { borderRadius: 8, marginInline: 6 },
        },
      },
      MuiTooltip: {
        defaultProps: { arrow: true },
      },
    },
  });
}
