import { List, ListItemButton, ListItemIcon, ListItemText } from '@mui/material';
import { NavLink } from 'react-router-dom';
import HubIcon from '@mui/icons-material/Hub';
import InventoryIcon from '@mui/icons-material/Inventory2';
import LayersIcon from '@mui/icons-material/Layers';
import InsightsIcon from '@mui/icons-material/Insights';

// Single source of truth for the route map so adding a page is one line.
const ITEMS = [
  { to: 'cluster', label: 'Cluster', icon: <HubIcon /> },
  { to: 'boxes', label: 'Boxes', icon: <InventoryIcon /> },
  { to: 'lsm', label: 'LSM internals', icon: <LayersIcon /> },
  { to: 'metrics', label: 'Metrics', icon: <InsightsIcon /> },
] as const;

export function NavRail() {
  return (
    <List sx={{ pt: 1 }}>
      {ITEMS.map((item) => (
        <ListItemButton
          key={item.to}
          component={NavLink}
          to={item.to}
          sx={{
            // react-router's NavLink toggles the `active` class on a matched link; piping that
            // through MUI's selected styling avoids hand-rolling a `useMatch` per item.
            '&.active': {
              bgcolor: 'action.selected',
              color: 'primary.main',
              '& .MuiListItemIcon-root': { color: 'primary.main' },
            },
          }}
        >
          <ListItemIcon sx={{ minWidth: 36 }}>{item.icon}</ListItemIcon>
          <ListItemText primary={item.label} primaryTypographyProps={{ fontWeight: 500 }} />
        </ListItemButton>
      ))}
    </List>
  );
}
