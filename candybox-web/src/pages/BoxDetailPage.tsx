import { useRef, useState, type DragEvent } from 'react';
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  Snackbar,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import { Link as RouterLink, useParams } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  ApiError,
  deleteCandy,
  fetchBox,
  fetchCandies,
  uploadCandy,
  type CandyRow,
} from '../api/client';
import { PageHeader } from '../components/PageHeader';
import { StatCard } from '../components/StatCard';
import { LoadingRow, ErrorBanner, EmptyState } from '../components/QueryStates';
import { formatBytes, formatCount, formatRelativeTime } from '../lib/format';

export function BoxDetailPage() {
  const { boxName = '' } = useParams<{ boxName: string }>();
  const [prefix, setPrefix] = useState('');
  const [uploadOpen, setUploadOpen] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState<CandyRow | null>(null);
  const [toast, setToast] = useState<{ kind: 'success' | 'error'; text: string } | null>(null);

  const queryClient = useQueryClient();

  const detail = useQuery({ queryKey: ['box', boxName], queryFn: () => fetchBox(boxName) });
  const candies = useQuery({
    queryKey: ['candies', boxName, prefix],
    queryFn: () => fetchCandies(boxName, prefix || undefined),
  });

  // After a candy-level mutation we re-fetch all candy listings for this box (the prefix-keyed
  // cache might hold an entry the user is about to scroll back into), and the box detail card so
  // its object/size totals catch up. We don't touch the cluster cache — owner/topology didn't move.
  const invalidateAfterMutation = () => {
    void queryClient.invalidateQueries({ queryKey: ['candies', boxName] });
    void queryClient.invalidateQueries({ queryKey: ['box', boxName] });
    void queryClient.invalidateQueries({ queryKey: ['boxes'] });
  };

  const uploadMutation = useMutation({
    mutationFn: ({ key, file }: { key: string; file: File }) => uploadCandy(boxName, key, file),
    onSuccess: (_data, vars) => {
      setUploadOpen(false);
      invalidateAfterMutation();
      setToast({ kind: 'success', text: `Uploaded "${vars.key}".` });
    },
    onError: (err: ApiError) => setToast({ kind: 'error', text: messageOf(err) }),
  });

  const deleteMutation = useMutation({
    mutationFn: (key: string) => deleteCandy(boxName, key),
    onSuccess: (_data, key) => {
      setConfirmDelete(null);
      invalidateAfterMutation();
      setToast({ kind: 'success', text: `Deleted "${key}".` });
    },
    onError: (err: ApiError) => setToast({ kind: 'error', text: messageOf(err) }),
  });

  const d = detail.data;

  return (
    <>
      <PageHeader
        title={boxName}
        subtitle="Box metadata + a sample of stored objects."
        actions={
          <Stack direction="row" spacing={1.5}>
            <Button
              component={RouterLink}
              to="/boxes"
              startIcon={<ArrowBackIcon />}
              variant="text"
              size="small"
            >
              All boxes
            </Button>
            <Button
              variant="contained"
              size="small"
              startIcon={<CloudUploadIcon />}
              onClick={() => setUploadOpen(true)}
            >
              Upload object
            </Button>
          </Stack>
        }
      />

      <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} sx={{ mb: 3 }}>
        <StatCard label="Objects" value={formatCount(d?.candyCount)} />
        <StatCard label="Size" value={formatBytes(d?.sizeBytes)} />
        <StatCard
          label="Manifest version"
          value={d?.manifestVersion != null ? d.manifestVersion : '—'}
          hint={d?.fencingToken != null ? `Fencing token ${d.fencingToken}` : undefined}
        />
        <StatCard
          label="Owner"
          value={
            <Typography
              variant="h6"
              sx={{ fontFamily: 'ui-monospace, monospace', wordBreak: 'break-all' }}
            >
              {d?.owner ?? '—'}
            </Typography>
          }
          hint={d?.hlc ? `HLC ${d.hlc}` : undefined}
        />
      </Stack>

      {detail.error && <ErrorBanner error={detail.error} />}

      <Card>
        <CardContent>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2} sx={{ mb: 2 }}>
            <TextField
              size="small"
              placeholder="Key prefix…"
              value={prefix}
              onChange={(e) => setPrefix(e.target.value)}
              fullWidth
            />
            {candies.data?.nextStartAfter && (
              <Chip
                size="small"
                label="More available — refine the prefix to drill in"
                color="info"
                variant="outlined"
              />
            )}
          </Stack>

          {candies.isLoading && <LoadingRow label="Loading objects…" />}
          {candies.error && <ErrorBanner error={candies.error} />}
          {candies.data && candies.data.entries.length === 0 && !candies.isLoading && (
            <EmptyState
              icon={<ContentCopyIcon fontSize="inherit" />}
              title="No objects"
              hint={
                prefix
                  ? 'No keys match this prefix. Try a different filter.'
                  : 'Click "Upload object" above to add the first one.'
              }
            />
          )}
          {candies.data && candies.data.entries.length > 0 && (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Key</TableCell>
                    <TableCell align="right">Size</TableCell>
                    <TableCell align="right">Created</TableCell>
                    <TableCell align="right" sx={{ width: 64 }} />
                  </TableRow>
                </TableHead>
                <TableBody>
                  {candies.data.entries.map((c) => (
                    <TableRow key={c.key} hover>
                      <TableCell>
                        <Typography
                          variant="body2"
                          sx={{ fontFamily: 'ui-monospace, monospace', wordBreak: 'break-all' }}
                        >
                          {c.key}
                        </Typography>
                      </TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatBytes(c.contentLength)}
                      </TableCell>
                      <TableCell
                        align="right"
                        sx={{ color: 'text.secondary', fontVariantNumeric: 'tabular-nums' }}
                      >
                        {formatRelativeTime(c.createdAtMillis)}
                      </TableCell>
                      <TableCell align="right">
                        <Tooltip title="Delete object">
                          <IconButton
                            size="small"
                            color="error"
                            onClick={() => setConfirmDelete(c)}
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

      <UploadCandyDialog
        open={uploadOpen}
        pending={uploadMutation.isPending}
        onClose={() => {
          if (!uploadMutation.isPending) {
            setUploadOpen(false);
            uploadMutation.reset();
          }
        }}
        onSubmit={(key, file) => uploadMutation.mutate({ key, file })}
        error={uploadMutation.error ? messageOf(uploadMutation.error as ApiError) : null}
      />

      <DeleteCandyDialog
        candy={confirmDelete}
        pending={deleteMutation.isPending}
        onClose={() => {
          if (!deleteMutation.isPending) {
            setConfirmDelete(null);
            deleteMutation.reset();
          }
        }}
        onConfirm={(key) => deleteMutation.mutate(key)}
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

function UploadCandyDialog({
  open,
  pending,
  onClose,
  onSubmit,
  error,
}: {
  open: boolean;
  pending: boolean;
  onClose: () => void;
  onSubmit: (key: string, file: File) => void;
  error: string | null;
}) {
  const [file, setFile] = useState<File | null>(null);
  const [key, setKey] = useState('');
  // The hidden <input> stays in the DOM so its `.value` clears between dialogs; without that, the
  // user picks the same file twice and the second pick fires no change event.
  const fileInputRef = useRef<HTMLInputElement>(null);
  // Tracks whether a drag is currently over the drop zone. The browser fires dragenter/dragleave
  // on every child, so we keep a counter to ignore the spurious "leave → enter" pair that fires
  // when the cursor moves between the zone and the icon/text nested inside it.
  const [dragDepth, setDragDepth] = useState(0);
  const dragging = dragDepth > 0;

  const handleFile = (f: File | null) => {
    setFile(f);
    // Pre-fill the key with the filename the first time the user picks one — they can override it
    // before submitting if they want a different key (e.g., a folder prefix). Drag-and-drop
    // re-uses this so the key field still auto-fills from a dropped file.
    if (f && !key) {
      setKey(f.name);
    }
  };

  const reset = () => {
    setFile(null);
    setKey('');
    setDragDepth(0);
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const onDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    setDragDepth(0);
    if (pending) return;
    // We accept a single file — taking just the first entry matches the rest of the dialog (one
    // upload per submit). Folder drops aren't supported; the entry would be a directory which
    // FileSystem APIs are needed to traverse.
    const dropped = e.dataTransfer.files?.[0];
    if (dropped) {
      handleFile(dropped);
    }
  };

  const canSubmit = !!file && key.trim().length > 0 && !pending;

  return (
    <Dialog
      open={open}
      onClose={() => {
        onClose();
        // Defer the reset until the close animation finishes; clearing immediately makes the
        // dialog flash empty before fading out.
        setTimeout(reset, 200);
      }}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle>Upload object</DialogTitle>
      <DialogContent>
        <DialogContentText sx={{ mb: 2 }}>
          The file is uploaded as a single object. Use slashes in the key to create S3-style
          prefixes (e.g. <code>album/spring.jpg</code>).
        </DialogContentText>
        <Stack spacing={2}>
          {/*
            Drop zone doubles as a click target — clicking anywhere inside opens the native file
            picker via the hidden <input>. dragOver must call preventDefault to mark the zone a
            valid drop target (the default is "reject"); the enter/leave counter handles the
            browser's spurious child-traversal events without flicker.
          */}
          <Box
            role="button"
            tabIndex={0}
            onClick={() => {
              if (!pending) fileInputRef.current?.click();
            }}
            onKeyDown={(e) => {
              if (!pending && (e.key === 'Enter' || e.key === ' ')) {
                e.preventDefault();
                fileInputRef.current?.click();
              }
            }}
            onDragEnter={(e) => {
              e.preventDefault();
              setDragDepth((d) => d + 1);
            }}
            onDragOver={(e) => {
              e.preventDefault();
              e.dataTransfer.dropEffect = pending ? 'none' : 'copy';
            }}
            onDragLeave={(e) => {
              e.preventDefault();
              setDragDepth((d) => Math.max(0, d - 1));
            }}
            onDrop={onDrop}
            sx={{
              borderRadius: 2,
              border: '2px dashed',
              borderColor: dragging ? 'primary.main' : 'divider',
              bgcolor: dragging ? 'action.hover' : 'background.default',
              p: 3,
              textAlign: 'center',
              cursor: pending ? 'not-allowed' : 'pointer',
              opacity: pending ? 0.6 : 1,
              transition: (theme) =>
                theme.transitions.create(['border-color', 'background-color'], {
                  duration: theme.transitions.duration.shortest,
                }),
              outline: 'none',
              '&:focus-visible': { borderColor: 'primary.main' },
            }}
          >
            <CloudUploadIcon
              sx={{ fontSize: 40, color: dragging ? 'primary.main' : 'text.secondary', mb: 1 }}
            />
            {file ? (
              <>
                <Typography variant="body1" sx={{ fontWeight: 500, wordBreak: 'break-all' }}>
                  {file.name}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  {formatBytes(file.size)} — click or drop to replace
                </Typography>
              </>
            ) : (
              <>
                <Typography variant="body1" sx={{ fontWeight: 500 }}>
                  {dragging ? 'Drop the file here' : 'Drag & drop a file here'}
                </Typography>
                <Typography variant="caption" color="text.secondary">
                  or click to browse
                </Typography>
              </>
            )}
            <input
              ref={fileInputRef}
              hidden
              type="file"
              onChange={(e) => handleFile(e.target.files?.[0] ?? null)}
            />
          </Box>
          <TextField
            label="Object key"
            fullWidth
            value={key}
            onChange={(e) => setKey(e.target.value)}
            inputProps={{ spellCheck: false, autoCapitalize: 'off' }}
            helperText={
              file ? `Content-Type: ${file.type || 'application/octet-stream'}` : 'Pick a file first.'
            }
          />
          {error && <Alert severity="error">{error}</Alert>}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={pending}>
          Cancel
        </Button>
        <Button
          variant="contained"
          disabled={!canSubmit}
          onClick={() => {
            if (file) onSubmit(key.trim(), file);
          }}
        >
          {pending ? 'Uploading…' : 'Upload'}
        </Button>
      </DialogActions>
    </Dialog>
  );
}

function DeleteCandyDialog({
  candy,
  pending,
  onClose,
  onConfirm,
  error,
}: {
  candy: CandyRow | null;
  pending: boolean;
  onClose: () => void;
  onConfirm: (key: string) => void;
  error: string | null;
}) {
  return (
    <Dialog open={!!candy} onClose={onClose} fullWidth maxWidth="xs">
      <DialogTitle>Delete object</DialogTitle>
      <DialogContent>
        <DialogContentText>
          Delete <b style={{ fontFamily: 'ui-monospace, monospace' }}>{candy?.key}</b>? This cannot
          be undone.
        </DialogContentText>
        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={pending}>
          Cancel
        </Button>
        <Button
          color="error"
          variant="contained"
          disabled={pending}
          onClick={() => {
            if (candy) onConfirm(candy.key);
          }}
        >
          {pending ? 'Deleting…' : 'Delete'}
        </Button>
      </DialogActions>
    </Dialog>
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
