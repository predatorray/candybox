import { useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  CardContent,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  FormControlLabel,
  IconButton,
  InputAdornment,
  Snackbar,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TableSortLabel,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import SearchIcon from '@mui/icons-material/Search';
import InventoryIcon from '@mui/icons-material/Inventory2';
import { Link as RouterLink } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, createBox, deleteBox, fetchBoxes, type BoxRow } from '../api/client';
import { PageHeader } from '../components/PageHeader';
import { LoadingRow, ErrorBanner, EmptyState } from '../components/QueryStates';
import { formatBytes, formatCount } from '../lib/format';

type SortKey = 'name' | 'owner' | 'candyCount' | 'sizeBytes';

export function BoxesPage() {
  const q = useQuery({ queryKey: ['boxes'], queryFn: fetchBoxes });
  const queryClient = useQueryClient();
  const [filter, setFilter] = useState('');
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>({
    key: 'name',
    dir: 'asc',
  });
  const [createOpen, setCreateOpen] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<BoxRow | null>(null);
  const [toast, setToast] = useState<{ kind: 'success' | 'error'; text: string } | null>(null);

  // After every successful mutation we invalidate both the boxes list (so the table refreshes) and
  // the cluster snapshot (so the per-node owned-count moves in lockstep). Doing it here keeps the
  // page authoritative about its own caches and avoids each dialog needing to know what's stale.
  const invalidateAfterMutation = () => {
    void queryClient.invalidateQueries({ queryKey: ['boxes'] });
    void queryClient.invalidateQueries({ queryKey: ['cluster'] });
  };

  const createMutation = useMutation({
    mutationFn: (name: string) => createBox(name),
    onSuccess: (_data, name) => {
      setCreateOpen(false);
      invalidateAfterMutation();
      setToast({ kind: 'success', text: `Box "${name}" created.` });
    },
    onError: (err: ApiError) => setToast({ kind: 'error', text: messageOf(err) }),
  });

  const deleteMutation = useMutation({
    mutationFn: ({ name, force }: { name: string; force: boolean }) => deleteBox(name, force),
    onSuccess: (_data, { name }) => {
      setConfirmDelete(null);
      invalidateAfterMutation();
      setToast({ kind: 'success', text: `Box "${name}" deleted.` });
    },
    onError: (err: ApiError) => setToast({ kind: 'error', text: messageOf(err) }),
  });

  // Filter + sort live entirely on the client; this scales fine for thousands of boxes because
  // boxes are the coarse-grained partition unit. If a cluster grows past that we'll page the API,
  // but v1 keeps the snappy "type-to-narrow" feel that makes the operator console feel responsive.
  const rows: BoxRow[] = useMemo(() => {
    if (!q.data) return [];
    const f = filter.trim().toLowerCase();
    const filtered = f ? q.data.boxes.filter((b) => b.name.toLowerCase().includes(f)) : q.data.boxes;
    const sorted = [...filtered].sort((a, b) => {
      const av = a[sort.key];
      const bv = b[sort.key];
      // Push nullish to the end regardless of direction so "—" rows don't crowd the top.
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (av < bv) return sort.dir === 'asc' ? -1 : 1;
      if (av > bv) return sort.dir === 'asc' ? 1 : -1;
      return 0;
    });
    return sorted;
  }, [q.data, filter, sort]);

  const handleSort = (key: SortKey) => () =>
    setSort((s) => ({ key, dir: s.key === key && s.dir === 'asc' ? 'desc' : 'asc' }));

  return (
    <>
      <PageHeader
        title="Boxes"
        subtitle="Buckets registered in coordination. Click a row to drill into its objects."
        actions={
          <Stack direction="row" spacing={1.5} alignItems="center">
            <TextField
              size="small"
              placeholder="Filter by name…"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon fontSize="small" />
                  </InputAdornment>
                ),
              }}
            />
            <Button
              variant="contained"
              size="small"
              startIcon={<AddIcon />}
              onClick={() => setCreateOpen(true)}
            >
              Create box
            </Button>
          </Stack>
        }
      />

      <Card>
        <CardContent sx={{ p: 0, '&:last-child': { pb: 0 } }}>
          {q.isLoading && <LoadingRow label="Loading boxes…" />}
          {q.error && <ErrorBanner error={q.error} />}
          {q.data && rows.length === 0 && !q.isLoading && (
            <EmptyState
              icon={<InventoryIcon fontSize="inherit" />}
              title={filter ? 'No boxes match your filter' : 'No boxes yet'}
              hint={
                filter
                  ? 'Clear the filter or check for typos.'
                  : 'Click "Create box" above, or use `candybox create-box <name>`.'
              }
            />
          )}
          {rows.length > 0 && (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <SortableHeader name="Name" col="name" sort={sort} onClick={handleSort('name')} />
                    <SortableHeader
                      name="Owner"
                      col="owner"
                      sort={sort}
                      onClick={handleSort('owner')}
                    />
                    <SortableHeader
                      name="Objects"
                      col="candyCount"
                      sort={sort}
                      onClick={handleSort('candyCount')}
                      align="right"
                    />
                    <SortableHeader
                      name="Size"
                      col="sizeBytes"
                      sort={sort}
                      onClick={handleSort('sizeBytes')}
                      align="right"
                    />
                    <TableCell align="right" sx={{ width: 64 }} />
                  </TableRow>
                </TableHead>
                <TableBody>
                  {rows.map((b) => (
                    <TableRow key={b.name} hover>
                      <TableCell>
                        <Typography
                          component={RouterLink}
                          to={`/boxes/${encodeURIComponent(b.name)}`}
                          variant="body2"
                          sx={{
                            fontFamily: 'ui-monospace, monospace',
                            color: 'primary.main',
                            textDecoration: 'none',
                            '&:hover': { textDecoration: 'underline' },
                          }}
                        >
                          {b.name}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography
                          variant="body2"
                          sx={{ fontFamily: 'ui-monospace, monospace', color: 'text.secondary' }}
                        >
                          {b.owner ?? '—'}
                        </Typography>
                      </TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatCount(b.candyCount)}
                      </TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatBytes(b.sizeBytes)}
                      </TableCell>
                      <TableCell align="right">
                        <Tooltip title="Delete box">
                          <IconButton
                            size="small"
                            color="error"
                            onClick={(ev) => {
                              ev.stopPropagation();
                              setConfirmDelete(b);
                            }}
                          >
                            <DeleteOutlineIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </CardContent>
      </Card>
      <Stack direction="row" justifyContent="flex-end" sx={{ mt: 1 }}>
        <Typography variant="caption" color="text.secondary">
          {q.data ? `${formatCount(rows.length)} of ${formatCount(q.data.boxes.length)} boxes` : ''}
        </Typography>
      </Stack>

      <CreateBoxDialog
        open={createOpen}
        pending={createMutation.isPending}
        onClose={() => {
          if (!createMutation.isPending) {
            setCreateOpen(false);
            createMutation.reset();
          }
        }}
        onSubmit={(name) => createMutation.mutate(name)}
        error={createMutation.error ? messageOf(createMutation.error as ApiError) : null}
      />

      <DeleteBoxDialog
        box={confirmDelete}
        pending={deleteMutation.isPending}
        onClose={() => {
          if (!deleteMutation.isPending) {
            setConfirmDelete(null);
            deleteMutation.reset();
          }
        }}
        onConfirm={(force) => {
          if (confirmDelete) {
            deleteMutation.mutate({ name: confirmDelete.name, force });
          }
        }}
        error={deleteMutation.error ? messageOf(deleteMutation.error as ApiError) : null}
      />

      <Snackbar
        open={!!toast}
        autoHideDuration={4500}
        onClose={() => setToast(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert severity={toast?.kind ?? 'success'} variant="filled" onClose={() => setToast(null)}>
          {toast?.text}
        </Alert>
      </Snackbar>
    </>
  );
}

function CreateBoxDialog({
  open,
  pending,
  onClose,
  onSubmit,
  error,
}: {
  open: boolean;
  pending: boolean;
  onClose: () => void;
  onSubmit: (name: string) => void;
  error: string | null;
}) {
  const [name, setName] = useState('');
  // Light, client-side validation mirrors what the server enforces (`BoxName.of`) — operators see
  // the "lowercase + digits + dashes" rule before the network round-trip. The server still has
  // the authoritative validator, so this is purely UX.
  const valid = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(name);
  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="xs">
      <DialogTitle>Create box</DialogTitle>
      <DialogContent>
        <DialogContentText sx={{ mb: 2 }}>
          Names must be 3–63 characters, lowercase letters, digits, or dashes, and start/end with a
          letter or digit.
        </DialogContentText>
        <TextField
          autoFocus
          fullWidth
          label="Box name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          error={name.length > 0 && !valid}
          helperText={name.length > 0 && !valid ? 'Invalid box name' : ' '}
          inputProps={{ spellCheck: false, autoCapitalize: 'off' }}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && valid && !pending) {
              onSubmit(name);
            }
          }}
        />
        {error && (
          <Alert severity="error" sx={{ mt: 1 }}>
            {error}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={pending}>
          Cancel
        </Button>
        <Button
          variant="contained"
          disabled={!valid || pending}
          onClick={() => onSubmit(name)}
        >
          {pending ? 'Creating…' : 'Create'}
        </Button>
      </DialogActions>
    </Dialog>
  );
}

function DeleteBoxDialog({
  box,
  pending,
  onClose,
  onConfirm,
  error,
}: {
  box: BoxRow | null;
  pending: boolean;
  onClose: () => void;
  onConfirm: (force: boolean) => void;
  error: string | null;
}) {
  const [force, setForce] = useState(false);
  return (
    <Dialog open={!!box} onClose={onClose} fullWidth maxWidth="xs">
      <DialogTitle>Delete box</DialogTitle>
      <DialogContent>
        <DialogContentText>
          Are you sure you want to delete <b>{box?.name}</b>? This cannot be undone.
        </DialogContentText>
        <FormControlLabel
          control={<Switch checked={force} onChange={(e) => setForce(e.target.checked)} />}
          label="Force (also delete any remaining objects)"
          sx={{ mt: 2 }}
        />
        {error && (
          <Alert severity="error" sx={{ mt: 1 }}>
            {error}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={pending}>
          Cancel
        </Button>
        <Button color="error" variant="contained" disabled={pending} onClick={() => onConfirm(force)}>
          {pending ? 'Deleting…' : 'Delete'}
        </Button>
      </DialogActions>
    </Dialog>
  );
}

function SortableHeader({
  name,
  col,
  sort,
  onClick,
  align,
}: {
  name: string;
  col: SortKey;
  sort: { key: SortKey; dir: 'asc' | 'desc' };
  onClick: () => void;
  align?: 'right';
}) {
  return (
    <TableCell align={align}>
      <TableSortLabel active={sort.key === col} direction={sort.dir} onClick={onClick}>
        {name}
      </TableSortLabel>
    </TableCell>
  );
}

function messageOf(err: unknown): string {
  if (err instanceof ApiError) {
    return err.detail || err.code || err.message;
  }
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}
