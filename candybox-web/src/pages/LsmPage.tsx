import {
  Card,
  CardContent,
  Chip,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material';
import LayersIcon from '@mui/icons-material/Layers';
import { useQuery } from '@tanstack/react-query';
import { fetchLsm } from '../api/client';
import { PageHeader } from '../components/PageHeader';
import { StatCard } from '../components/StatCard';
import { LoadingRow, ErrorBanner, EmptyState } from '../components/QueryStates';
import { formatCount } from '../lib/format';

export function LsmPage() {
  const q = useQuery({ queryKey: ['lsm'], queryFn: fetchLsm });
  const rows = q.data?.boxes ?? [];

  // Roll-ups across boxes — gives a single number per stat-card so operators can see "is anything
  // backed up?" at a glance before scanning rows.
  const totals = rows.reduce(
    (acc, r) => ({
      sstables: acc.sstables + (r.sstableLedgerCount ?? 0),
      syrups: acc.syrups + (r.syrupLedgerCount ?? 0),
      wal: acc.wal + (r.walLedgerCount ?? 0),
      compactions: acc.compactions + (r.inFlightCompactions ?? 0),
      gc: acc.gc + (r.gcBacklog ?? 0),
    }),
    { sstables: 0, syrups: 0, wal: 0, compactions: 0, gc: 0 },
  );

  return (
    <>
      <PageHeader
        title="LSM internals"
        subtitle="Per-box manifest / ledger inventory, in-flight compactions, and GC backlog."
      />

      <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} sx={{ mb: 3 }}>
        <StatCard label="SSTable ledgers" value={formatCount(totals.sstables)} />
        <StatCard label="Syrup ledgers" value={formatCount(totals.syrups)} />
        <StatCard label="WAL ledgers" value={formatCount(totals.wal)} />
        <StatCard
          label="Compactions in-flight"
          value={formatCount(totals.compactions)}
          hint={totals.compactions ? 'Running now' : 'Idle'}
        />
        <StatCard
          label="GC backlog"
          value={formatCount(totals.gc)}
          hint={totals.gc > 0 ? 'Ledgers awaiting deletion' : 'Clear'}
        />
      </Stack>

      <Card>
        <CardContent sx={{ p: 0, '&:last-child': { pb: 0 } }}>
          {q.isLoading && <LoadingRow label="Loading LSM snapshot…" />}
          {q.error && <ErrorBanner error={q.error} />}
          {q.data && rows.length === 0 && !q.isLoading && (
            <EmptyState
              icon={<LayersIcon fontSize="inherit" />}
              title="No boxes to inspect"
              hint="Create a box to see its LSM internals here."
            />
          )}
          {rows.length > 0 && (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Box</TableCell>
                    <TableCell>Owner</TableCell>
                    <TableCell align="right">Manifest</TableCell>
                    <TableCell align="right">SSTables</TableCell>
                    <TableCell align="right">Syrups</TableCell>
                    <TableCell align="right">WAL</TableCell>
                    <TableCell align="right">Compactions</TableCell>
                    <TableCell align="right">GC backlog</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {rows.map((r) => (
                    <TableRow key={r.box} hover>
                      <TableCell>
                        <Typography variant="body2" sx={{ fontFamily: 'ui-monospace, monospace' }}>
                          {r.box}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography
                          variant="body2"
                          sx={{ fontFamily: 'ui-monospace, monospace', color: 'text.secondary' }}
                        >
                          {r.owner ?? '—'}
                        </Typography>
                      </TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {r.manifestVersion ?? '—'}
                      </TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatCount(r.sstableLedgerCount)}
                      </TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatCount(r.syrupLedgerCount)}
                      </TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatCount(r.walLedgerCount)}
                      </TableCell>
                      <TableCell align="right">
                        {r.inFlightCompactions ? (
                          <Chip
                            size="small"
                            label={r.inFlightCompactions}
                            color="warning"
                            variant="outlined"
                          />
                        ) : (
                          <Typography variant="body2" color="text.secondary">
                            0
                          </Typography>
                        )}
                      </TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatCount(r.gcBacklog)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </CardContent>
      </Card>
    </>
  );
}
