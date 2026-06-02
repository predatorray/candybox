import { ReactNode } from 'react';
import {
  AppBar,
  Box,
  Drawer,
  IconButton,
  Stack,
  Toolbar,
  Tooltip,
  Typography,
  useMediaQuery,
  useTheme,
} from '@mui/material';
import DarkModeIcon from '@mui/icons-material/DarkMode';
import LightModeIcon from '@mui/icons-material/LightMode';
import DensityMediumIcon from '@mui/icons-material/DensityMedium';
import DensitySmallIcon from '@mui/icons-material/DensitySmall';
import GitHubIcon from '@mui/icons-material/GitHub';
import { NavRail } from './NavRail';
import { CandyLogo } from './CandyLogo';
import { useThemeSettings } from '../theme/ThemeProvider';

const DRAWER_WIDTH = 232;

export function AppShell({ children }: { children: ReactNode }) {
  const { mode, density, toggleMode, setDensity } = useThemeSettings();
  const theme = useTheme();
  // On narrow viewports the rail collapses to icons; below sm it disappears entirely (users tap a
  // hamburger via a future enhancement — keep v1 desktop-first since this is an operator tool).
  const wide = useMediaQuery(theme.breakpoints.up('md'));

  return (
    <Box sx={{ display: 'flex', minHeight: '100vh', bgcolor: 'background.default' }}>
      <AppBar
        position="fixed"
        sx={{
          width: wide ? `calc(100% - ${DRAWER_WIDTH}px)` : '100%',
          ml: wide ? `${DRAWER_WIDTH}px` : 0,
          zIndex: (t) => t.zIndex.drawer + 1,
        }}
      >
        <Toolbar sx={{ gap: 1 }}>
          {!wide && <CandyLogo size={50} />}
          <Typography variant="h6" sx={{ flexGrow: 1, fontWeight: 600 }}>
            Candybox
          </Typography>
          <Tooltip
            title={density === 'compact' ? 'Switch to comfortable density' : 'Switch to compact density'}
          >
            <IconButton
              onClick={() => setDensity(density === 'compact' ? 'comfortable' : 'compact')}
              aria-label="toggle density"
            >
              {density === 'compact' ? <DensityMediumIcon /> : <DensitySmallIcon />}
            </IconButton>
          </Tooltip>
          <Tooltip title={mode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}>
            <IconButton onClick={toggleMode} aria-label="toggle color mode">
              {mode === 'dark' ? <LightModeIcon /> : <DarkModeIcon />}
            </IconButton>
          </Tooltip>
          <Tooltip title="Source on GitHub">
            <IconButton
              component="a"
              href="https://github.com/predatorray/candybox"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="GitHub repository"
            >
              <GitHubIcon />
            </IconButton>
          </Tooltip>
        </Toolbar>
      </AppBar>

      {wide && (
        <Drawer
          variant="permanent"
          sx={{
            width: DRAWER_WIDTH,
            flexShrink: 0,
            '& .MuiDrawer-paper': {
              width: DRAWER_WIDTH,
              boxSizing: 'border-box',
              borderRight: 1,
              borderColor: 'divider',
              bgcolor: 'background.paper',
            },
          }}
        >
          <Toolbar sx={{ gap: 1, px: 2 }}>
            <CandyLogo size={28} />
            <Stack>
              <Typography variant="subtitle2" sx={{ fontWeight: 700, lineHeight: 1.1 }}>
                candybox
              </Typography>
              <Typography variant="caption" color="text.secondary" sx={{ lineHeight: 1.1 }}>
                operator console
              </Typography>
            </Stack>
          </Toolbar>
          <NavRail />
        </Drawer>
      )}

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: { xs: 2, md: 3 },
          width: '100%',
          maxWidth: '100%',
          mt: '64px',
        }}
      >
        {children}
      </Box>
    </Box>
  );
}
