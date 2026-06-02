import { Card, CardContent, Stack, Typography } from '@mui/material';
import { ReactNode } from 'react';

/**
 * The "big number" tile used at the top of cluster/boxes/lsm pages. Keeps a consistent visual
 * rhythm — every page starts with a 3–4 column row of these before the detail tables.
 */
export function StatCard({
  label,
  value,
  hint,
  icon,
}: {
  label: string;
  value: ReactNode;
  hint?: string;
  icon?: ReactNode;
}) {
  return (
    <Card sx={{ flex: 1, minWidth: 160 }}>
      <CardContent sx={{ '&:last-child': { pb: 2 } }}>
        <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mb: 1 }}>
          <Typography variant="overline" color="text.secondary" sx={{ letterSpacing: '0.1em' }}>
            {label}
          </Typography>
          {icon && <Stack color="text.secondary">{icon}</Stack>}
        </Stack>
        <Typography variant="h4" sx={{ fontVariantNumeric: 'tabular-nums', lineHeight: 1.1 }}>
          {value}
        </Typography>
        {hint && (
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
            {hint}
          </Typography>
        )}
      </CardContent>
    </Card>
  );
}
