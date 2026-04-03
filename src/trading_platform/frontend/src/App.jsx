import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import Dashboard from './pages/Dashboard'
import Signals from './pages/Signals'
import Markets from './pages/Markets'
import PolymarketLive from './pages/PolymarketLive'
import Reasoning from './pages/Reasoning'
import Control from './pages/Control'

export default function App() {
  return (
    <BrowserRouter>
      <div className="flex min-h-screen">
        <Sidebar />
        <main className="flex-1 overflow-auto">
          <Routes>
            <Route path="/" element={<Navigate to="/dashboard" replace />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/signals" element={<Signals />} />
            <Route path="/markets" element={<Markets />} />
            <Route path="/polymarket" element={<PolymarketLive />} />
            <Route path="/reasoning" element={<Reasoning />} />
            <Route path="/control" element={<Control />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
