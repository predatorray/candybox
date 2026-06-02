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
import HubIcon from '@mui/icons-material/Hub';
import InventoryIcon from '@mui/icons-material/Inventory2';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import { useQuery } from '@tanstack/react-query';
import { fetchCluster, type NodeRow } from '../api/client';
import { PageHeader } from '../components/PageHeader';
import { StatCard } from '../components/StatCard';
import { LoadingRow, ErrorBanner, EmptyState } from '../components/QueryStates';
import { formatCount } from '../lib/format';

export function ClusterPage() {
  const q = useQuery({ queryKey: ['cluster'], queryFn: fetchCluster });
  const cluster = q.data;

  return (
    <>
      <PageHeader
        title="Cluster"
        subtitle="Nodes registered with coordination, with their owned-box counts."
      />

      <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} sx={{ mb: 3 }}>
        <StatCard
          label="Nodes"
          value={formatCount(cluster?.nodes.length)}
          icon={<HubIcon fontSize="small" />}
        />
        <StatCard
          label="Boxes"
          value={formatCount(cluster?.boxCount)}
          icon={<InventoryIcon fontSize="small" />}
        />
        <StatCard
          label="Ownerless"
          value={formatCount(cluster?.ownerless.length)}
          hint={cluster?.ownerless.length ? 'Boxes with no current owner' : 'All boxes have an owner'}
          icon={<WarningAmberIcon fontSize="small" />}
        />
      </Stack>

      <Card>
        <CardContent sx={{ p: 0, '&:last-child': { pb: 0 } }}>
          {q.isLoading && <LoadingRow label="Loading cluster…" />}
          {q.error && <ErrorBanner error={q.error} />}
          {cluster && cluster.nodes.length === 0 && !q.isLoading && (
            <EmptyState
              icon={<HubIcon fontSize="inherit" />}
              title="No nodes yet"
              hint={
                cluster.stub
                  ? 'The admin API is serving a stub response — Phase 4 wires real coordination reads.'
                  : 'Start at least one candybox-server and it will appear here.'
              }
            />
          )}
          {cluster && cluster.nodes.length > 0 && (
            <TableContainer>
              <Table>
                <TableHead>
                  <TableRow>
                    <TableCell>Node ID</TableCell>
                    <TableCell>Address</TableCell>
                    <TableCell align="right">Owned boxes</TableCell>
                    <TableCell align="right">Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {cluster.nodes.map((n: NodeRow) => (
                    <TableRow key={n.nodeId} hover>
                      <TableCell>
                        <Typography variant="body2" sx={{ fontFamily: 'ui-monospace, monospace' }}>
                          {n.nodeId}
                        </Typography>
                      </TableCell>
                      <TableCell>{n.address ?? '—'}</TableCell>
                      <TableCell align="right" sx={{ fontVariantNumeric: 'tabular-nums' }}>
                        {formatCount(n.ownedBoxCount)}
                      </TableCell>
                      <TableCell align="right">
                        <Chip
                          size="small"
                          label={n.ready ? 'Ready' : 'Not ready'}
                          color={n.ready ? 'success' : 'warning'}
                          variant="outlined"
                        />
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
