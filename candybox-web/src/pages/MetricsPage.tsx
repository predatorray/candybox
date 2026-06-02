import { useMemo, useState } from 'react';
import {
  Card,
  CardContent,
  Chip,
  Grid,
  Stack,
  ToggleButton,
  ToggleButtonGroup,
  Typography,
  useTheme,
} from '@mui/material';
import InsightsIcon from '@mui/icons-material/Insights';
import { useQuery } from '@tanstack/react-query';
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { fetchTimeseries, type MetricSeries } from '../api/client';
import { PageHeader } from '../components/PageHeader';
import { LoadingRow, ErrorBanner, EmptyState } from '../components/QueryStates';

// Curated metric set surfaced in v1 — Phase 6 adds the matching backend rolling window. Picking a
// short list deliberately: a dashboard that shows 60 metrics is a dashboard nobody reads.
const METRIC_GROUPS = {
  Traffic: ['candybox_request_count', 'candybox_request_latency_ms'],
  Storage: ['candybox_owned_boxes', 'candybox_syrup_bytes_total'],
  LSM: ['candybox_sstable_count', 'candybox_compactions_inflight'],
} as const;
type GroupName = keyof typeof METRIC_GROUPS;

export function MetricsPage() {
  const theme = useTheme();
  const [group, setGroup] = useState<GroupName>('Traffic');
  const names = METRIC_GROUPS[group];

  const q = useQuery({
    queryKey: ['timeseries', names.join(',')],
    queryFn: () => fetchTimeseries([...names]),
    // 5s refetch is set globally; metrics are the page that benefits most from it.
  });

  // Recharts wants a flat array of points: merge all series by timestamp on the client. The
  // rolling window from the backend is short (a few minutes), so this is cheap.
  const merged = useMemo(() => mergeSeries(q.data?.series ?? []), [q.data]);

  return (
    <>
      <PageHeader
        title="Metrics"
        subtitle="Rolling in-memory window from each node's /metrics. No persistence across restarts."
        actions={
          <ToggleButtonGroup
            size="small"
            exclusive
            value={group}
            onChange={(_, v) => v && setGroup(v as GroupName)}
          >
            {(Object.keys(METRIC_GROUPS) as GroupName[]).map((g) => (
              <ToggleButton key={g} value={g} sx={{ px: 2 }}>
                {g}
              </ToggleButton>
            ))}
          </ToggleButtonGroup>
        }
      />

      <Grid container spacing={2}>
        {names.map((name, idx) => (
          <Grid item xs={12} md={6} key={name}>
            <Card>
              <CardContent>
                <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 1 }}>
                  <Typography variant="subtitle1" sx={{ fontFamily: 'ui-monospace, monospace' }}>
                    {name}
                  </Typography>
                  <Chip size="small" label={`window ${q.data?.windowSeconds ?? '—'}s`} variant="outlined" />
                </Stack>
                <div style={{ height: 220 }}>
                  {q.isLoading && <LoadingRow />}
                  {q.error && <ErrorBanner error={q.error} />}
                  {q.data && merged[name]?.length ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={merged[name]} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
                        <CartesianGrid stroke={theme.palette.divider} strokeDasharray="3 3" />
                        <XAxis
                          dataKey="t"
                          stroke={theme.palette.text.secondary}
                          tickFormatter={(t) => new Date(t).toLocaleTimeString().slice(0, 5)}
                          fontSize={11}
                        />
                        <YAxis stroke={theme.palette.text.secondary} fontSize={11} width={48} />
                        <Tooltip
                          contentStyle={{
                            background: theme.palette.background.paper,
                            border: `1px solid ${theme.palette.divider}`,
                            borderRadius: 8,
                          }}
                          labelFormatter={(t: number) => new Date(t).toLocaleTimeString()}
                        />
                        <Line
                          type="monotone"
                          dataKey="v"
                          stroke={idx % 2 ? theme.palette.secondary.main : theme.palette.primary.main}
                          strokeWidth={2}
                          dot={false}
                          isAnimationActive={false}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  ) : (
                    !q.isLoading &&
                    !q.error && (
                      <EmptyState
                        icon={<InsightsIcon fontSize="inherit" />}
                        title="No samples yet"
                        hint="The rolling window will populate after a few seconds."
                      />
                    )
                  )}
                </div>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>
    </>
  );
}

function mergeSeries(series: MetricSeries[]): Record<string, { t: number; v: number }[]> {
  const out: Record<string, { t: number; v: number }[]> = {};
  for (const s of series) {
    // If two series share the same metric name (different labels), keep them keyed by name and
    // overlay points. v1 only renders one line per chart, but the data shape leaves room for more.
    const existing = out[s.name] ?? [];
    out[s.name] = existing.concat(s.samples).sort((a, b) => a.t - b.t);
  }
  return out;
}
