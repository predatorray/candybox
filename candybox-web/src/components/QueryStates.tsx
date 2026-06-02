import { Alert, Box, CircularProgress, Stack, Typography } from '@mui/material';
import { ReactNode } from 'react';

/**
 * Common render helpers for TanStack Query states. Every page handles loading / error / empty the
 * same way, so we encode the pattern once and let the page focus on the data row layout.
 */

export function LoadingRow({ label = 'Loading…' }: { label?: string }) {
  return (
    <Stack direction="row" alignItems="center" spacing={2} sx={{ py: 4, justifyContent: 'center' }}>
      <CircularProgress size={20} />
      <Typography variant="body2" color="text.secondary">
        {label}
      </Typography>
    </Stack>
  );
}

export function ErrorBanner({ error }: { error: unknown }) {
  const message = error instanceof Error ? error.message : String(error);
  return (
    <Alert severity="error" sx={{ my: 2 }}>
      {message}
    </Alert>
  );
}

export function EmptyState({ icon, title, hint }: { icon: ReactNode; title: string; hint?: string }) {
  return (
    <Box
      sx={{
        py: 8,
        textAlign: 'center',
        color: 'text.secondary',
        border: 1,
        borderStyle: 'dashed',
        borderColor: 'divider',
        borderRadius: 2,
      }}
    >
      <Box sx={{ fontSize: 40, opacity: 0.6, mb: 1 }}>{icon}</Box>
      <Typography variant="subtitle1">{title}</Typography>
      {hint && (
        <Typography variant="body2" sx={{ mt: 0.5 }}>
          {hint}
        </Typography>
      )}
    </Box>
  );
}
