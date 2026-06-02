import { Route, Routes, Navigate } from 'react-router-dom';
import { AppShell } from './components/AppShell';
import { ClusterPage } from './pages/ClusterPage';
import { BoxesPage } from './pages/BoxesPage';
import { BoxDetailPage } from './pages/BoxDetailPage';
import { LsmPage } from './pages/LsmPage';
import { MetricsPage } from './pages/MetricsPage';

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Navigate to="cluster" replace />} />
        <Route path="cluster" element={<ClusterPage />} />
        <Route path="boxes" element={<BoxesPage />} />
        <Route path="boxes/:boxName" element={<BoxDetailPage />} />
        <Route path="lsm" element={<LsmPage />} />
        <Route path="metrics" element={<MetricsPage />} />
        <Route path="*" element={<Navigate to="cluster" replace />} />
      </Routes>
    </AppShell>
  );
}
