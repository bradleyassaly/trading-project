import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import Dashboard from './pages/Dashboard'
import SmartMoney from './pages/SmartMoney'
import SignalMonitor from './pages/SignalMonitor'
import MarketScanner from './pages/MarketScanner'
import Paper from './pages/Paper'
import ExecutionEngine from './pages/ExecutionEngine'
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
            <Route path="/signals" element={<SignalMonitor />} />
            <Route path="/scanner" element={<MarketScanner />} />
            <Route path="/markets" element={<Navigate to="/scanner" replace />} />
            <Route path="/paper" element={<Paper />} />
            <Route path="/paper-trading" element={<Navigate to="/paper" replace />} />
            <Route path="/engine" element={<ExecutionEngine />} />
            <Route path="/control" element={<Navigate to="/engine" replace />} />
            <Route path="/loop" element={<Navigate to="/engine" replace />} />
            <Route path="/research" element={<Navigate to="/signals" replace />} />
            <Route path="/polymarket" element={<Navigate to="/scanner" replace />} />
            <Route path="/kalshi" element={<Navigate to="/scanner" replace />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
