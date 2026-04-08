import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import Dashboard from './pages/Dashboard'
import SmartMoney from './pages/SmartMoney'
import MarketScanner from './pages/MarketScanner'
import Paper from './pages/Paper'
import PipelineMonitor from './pages/PipelineMonitor'
import MarketDetail from './pages/MarketDetail'
import Backtest from './pages/Backtest'
import MarketMonitor from './pages/MarketMonitor'
import SignalLab from './pages/SignalLab'
import LiveReadiness from './pages/LiveReadiness'
import WalletDetail from './pages/WalletDetail'

export default function App() {
  return (
    <BrowserRouter>
      <div className="flex min-h-screen">
        <Sidebar />
        <main className="flex-1 overflow-auto">
          <Routes>
            <Route path="/" element={<Navigate to="/dashboard" replace />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/wallets" element={<SmartMoney />} />
            <Route path="/smart-money" element={<Navigate to="/wallets" replace />} />
            <Route path="/smart-money/:address" element={<WalletDetail />} />
            <Route path="/scanner" element={<MarketScanner />} />
            <Route path="/monitor" element={<MarketMonitor />} />
            <Route path="/markets" element={<Navigate to="/scanner" replace />} />
            <Route path="/pipeline" element={<PipelineMonitor />} />
            <Route path="/backtest" element={<Backtest />} />
            <Route path="/market/:conditionId" element={<MarketDetail />} />
            <Route path="/paper" element={<Paper />} />
            <Route path="/paper-trading" element={<Navigate to="/paper" replace />} />
            <Route path="/live" element={<LiveReadiness />} />
            <Route path="/engine" element={<Navigate to="/live" replace />} />
            <Route path="/signals" element={<SignalLab />} />
            <Route path="/control" element={<Navigate to="/live" replace />} />
            <Route path="/loop" element={<Navigate to="/live" replace />} />
            <Route path="/research" element={<Navigate to="/dashboard" replace />} />
            <Route path="/polymarket" element={<Navigate to="/scanner" replace />} />
            <Route path="/kalshi" element={<Navigate to="/scanner" replace />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
