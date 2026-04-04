import { useCallback, useEffect, useMemo, useState } from 'react'
import { api } from '../api/client'
import { useApi } from '../hooks/useApi'
import LoadingSkeleton from '../components/LoadingSkeleton'
import EmptyState from '../components/EmptyState'

function StatusBadge({ status }) {
  const normalized = String(status || 'unknown').toLowerCase()
  const className =
    normalized === 'healthy' ? 'bg-accent-green/15 text-accent-green' :
    normalized === 'warning' ? 'bg-accent-yellow/15 text-accent-yellow' :
    normalized === 'critical' ? 'bg-accent-red/15 text-accent-red' :
    'bg-surface-hover text-gray-400'
  return (
    <span className={`inline-flex items-center rounded px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${className}`}>
      {normalized}
    </span>
  )
}

export default function ResearchData() {
  const [providerFilter, setProviderFilter] = useState('')
  const [selectedProvider, setSelectedProvider] = useState('')
  const [selectedKey, setSelectedKey] = useState(null)

  const datasetsFetcher = useCallback(
    () => api.researchDatasets(providerFilter ? { provider: providerFilter } : {}),
    [providerFilter],
  )
  const monitoringFetcher = useCallback(() => api.providerMonitoring(), [])
  const healthFetcher = useCallback(() => api.providerHealth(), [])
  const providerDetailFetcher = useCallback(
    () => (selectedProvider ? api.providerDetail(selectedProvider) : Promise.resolve(null)),
    [selectedProvider],
  )
  const datasetDetailFetcher = useCallback(
    () => (selectedKey ? api.monitoredDatasetDetail(selectedKey) : Promise.resolve(null)),
    [selectedKey],
  )
  const rowsFetcher = useCallback(
    () => (selectedKey ? api.researchDatasetRows(selectedKey, { limit: 5 }) : Promise.resolve(null)),
    [selectedKey],
  )
  const replayFetcher = useCallback(
    () => (
      selectedProvider
        ? api.researchReplayPreview({ provider: [selectedProvider], limit: 5, alignment_mode: 'outer_union' })
        : Promise.resolve(null)
    ),
    [selectedProvider],
  )

  const { data: datasets, loading: datasetsLoading } = useApi(datasetsFetcher, 30_000)
  const { data: monitoring } = useApi(monitoringFetcher, 30_000)
  const { data: health } = useApi(healthFetcher, 30_000)
  const { data: providerDetail, loading: providerDetailLoading } = useApi(providerDetailFetcher, 30_000)
  const { data: datasetDetail, loading: datasetDetailLoading } = useApi(datasetDetailFetcher, 30_000)
  const { data: rows, loading: rowsLoading } = useApi(rowsFetcher, 30_000)
  const { data: replayPreview, loading: replayLoading } = useApi(replayFetcher, 30_000)

  const providerOptions = useMemo(() => {
    const values = (datasets?.data || []).map((entry) => entry.provider)
    return Array.from(new Set(values)).sort()
  }, [datasets])

  useEffect(() => {
    if (!selectedProvider && (health?.provider_summaries || []).length) {
      setSelectedProvider(health.provider_summaries[0].provider)
    }
  }, [health, selectedProvider])

  const previewColumns = rows?.data?.length ? Object.keys(rows.data[0]) : []
  const replayColumns = replayPreview?.data?.length ? Object.keys(replayPreview.data[0]) : []
  const selectedMonitoring = useMemo(() => {
    if (!selectedKey || !monitoring?.records) return null
    return monitoring.records.find((record) => record.dataset_key === selectedKey) || null
  }, [monitoring, selectedKey])

  return (
    <div className="p-6 space-y-6">
      <div>
        <p className="text-xs text-gray-600 mb-1">
          Trading Platform <span className="mx-1">></span>
          <span className="text-gray-400">Research Data</span>
        </p>
        <h1 className="text-lg font-semibold text-gray-200">Shared Research Registry</h1>
        <p className="text-sm text-gray-500 mt-1">
          Inspect shared registry datasets, drill into provider health, and preview replay-ready assemblies from the shared reader layer.
        </p>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="card xl:col-span-2">
          <div className="flex items-center gap-3 mb-4">
            <h2 className="text-sm font-medium text-gray-400">Registered Datasets</h2>
            <select
              value={providerFilter}
              onChange={(event) => setProviderFilter(event.target.value)}
              className="ml-auto bg-surface-card border border-surface-border rounded-md px-2 py-1 text-xs text-gray-200"
            >
              <option value="">All providers</option>
              {providerOptions.map((provider) => (
                <option key={provider} value={provider}>{provider}</option>
              ))}
            </select>
          </div>
          {datasetsLoading ? (
            <LoadingSkeleton rows={5} />
          ) : !datasets?.available || !(datasets.data || []).length ? (
            <EmptyState title="No shared datasets published" icon="[]" />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-surface-border text-left text-gray-500">
                    <th className="pb-2 pr-4">Provider</th>
                    <th className="pb-2 pr-4">Dataset</th>
                    <th className="pb-2 pr-4">Asset Class</th>
                    <th className="pb-2 pr-4">Latest Event</th>
                    <th className="pb-2 pr-4">Materialized</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-surface-border">
                  {(datasets.data || []).map((entry) => (
                    <tr
                      key={entry.dataset_key}
                      className={`cursor-pointer hover:bg-surface-hover ${selectedKey === entry.dataset_key ? 'bg-accent-blue/10' : ''}`}
                      onClick={() => {
                        setSelectedKey(entry.dataset_key)
                        setSelectedProvider(entry.provider)
                      }}
                    >
                      <td className="py-2 pr-4 text-gray-300">{entry.provider}</td>
                      <td className="py-2 pr-4">
                        <div className="text-accent-blue font-mono">{entry.dataset_name}</div>
                        <div className="text-gray-600">{entry.dataset_key}</div>
                      </td>
                      <td className="py-2 pr-4 text-gray-400">{entry.asset_class}</td>
                      <td className="py-2 pr-4 text-gray-400">{entry.latest_event_time || '-'}</td>
                      <td className="py-2 pr-4 text-gray-400">{entry.latest_materialized_at || '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-4">Provider Health</h2>
          {!health?.available || !(health.provider_summaries || []).length ? (
            <EmptyState title="No provider health summary" icon="+" />
          ) : (
            <div className="space-y-3">
              {(health.provider_summaries || []).map((provider) => (
                <button
                  type="button"
                  key={provider.provider}
                  className={`w-full text-left rounded border p-3 ${selectedProvider === provider.provider ? 'border-accent-blue bg-accent-blue/10' : 'border-surface-border'}`}
                  onClick={() => setSelectedProvider(provider.provider)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-sm text-gray-200">{provider.provider}</div>
                      <div className="text-xs text-gray-500">
                        {provider.dataset_count} datasets, {provider.stale_dataset_count} stale
                      </div>
                    </div>
                    <StatusBadge status={provider.status} />
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-4">Provider Drill-Down</h2>
          {!selectedProvider ? (
            <EmptyState title="Select a provider" icon="+" />
          ) : providerDetailLoading ? (
            <LoadingSkeleton rows={5} />
          ) : !providerDetail?.available ? (
            <EmptyState title={providerDetail?.reason || 'No provider detail available'} icon="+" />
          ) : (
            <div className="space-y-3 text-xs">
              <div className="flex items-center justify-between">
                <div className="text-gray-200">{providerDetail.provider}</div>
                <StatusBadge status={providerDetail.health_summary?.status} />
              </div>
              <div className="text-gray-500">
                Registry entries: {(providerDetail.datasets || []).length} | monitored scopes: {(providerDetail.monitoring_records || []).length}
              </div>
              <div className="rounded border border-surface-border p-3">
                <div className="text-gray-500 mb-1">Provider datasets</div>
                <div className="space-y-1">
                  {(providerDetail.datasets || []).map((dataset) => (
                    <button
                      type="button"
                      key={dataset.dataset_key}
                      className="block w-full text-left text-accent-blue"
                      onClick={() => setSelectedKey(dataset.dataset_key)}
                    >
                      {dataset.dataset_key}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>

        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-4">Dataset Drill-Down</h2>
          {!selectedKey ? (
            <EmptyState title="Select a dataset" icon="[]" />
          ) : datasetDetailLoading ? (
            <LoadingSkeleton rows={4} />
          ) : !datasetDetail?.available ? (
            <EmptyState title={datasetDetail?.reason || 'Dataset detail unavailable'} icon="[]" />
          ) : (
            <div className="space-y-3 text-xs">
              <div>
                <div className="text-gray-500">Key</div>
                <div className="font-mono text-gray-300 break-all">{datasetDetail.dataset.dataset_key}</div>
              </div>
              <div>
                <div className="text-gray-500">Path</div>
                <div className="font-mono text-gray-300 break-all">{datasetDetail.dataset.dataset_path}</div>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <div className="text-gray-500">Time Column</div>
                  <div className="text-gray-300">{datasetDetail.dataset.time_column || '-'}</div>
                </div>
                <div>
                  <div className="text-gray-500">Storage</div>
                  <div className="text-gray-300">{datasetDetail.dataset.storage_type}</div>
                </div>
              </div>
              <div>
                <div className="text-gray-500">Primary Keys</div>
                <div className="text-gray-300">{(datasetDetail.dataset.primary_keys || []).join(', ') || '-'}</div>
              </div>
              <div>
                <div className="text-gray-500">Provider health</div>
                <div className="mt-1"><StatusBadge status={datasetDetail.provider_health_summary?.status} /></div>
              </div>
              {(selectedMonitoring || datasetDetail.monitoring_record) && (
                <div className="rounded border border-surface-border p-3">
                  <div className="flex items-center justify-between">
                    <div className="text-gray-200">Monitoring</div>
                    <StatusBadge status={(datasetDetail.monitoring_record || selectedMonitoring)?.status} />
                  </div>
                  <div className="mt-2 text-gray-500">
                    Sync: {(datasetDetail.monitoring_record || selectedMonitoring)?.latest_sync_outcome || '-'} | stale: {String((datasetDetail.monitoring_record || selectedMonitoring)?.stale)}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        <div className="card">
          <h2 className="text-sm font-medium text-gray-400 mb-4">Dataset Preview</h2>
          {!selectedKey ? (
            <EmptyState title="Select a dataset" icon="[]" />
          ) : rowsLoading ? (
            <LoadingSkeleton rows={4} />
          ) : !rows?.available || !(rows.data || []).length ? (
            <EmptyState title={rows?.reason || 'No preview rows available'} icon="[]" />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-surface-border text-left text-gray-500">
                    {previewColumns.map((column) => (
                      <th key={column} className="pb-2 pr-4">{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-surface-border">
                  {(rows.data || []).map((row, index) => (
                    <tr key={index}>
                      {previewColumns.map((column) => (
                        <td key={column} className="py-2 pr-4 text-gray-300">
                          {row[column] == null ? '-' : String(row[column])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>

      <div className="card">
        <h2 className="text-sm font-medium text-gray-400 mb-4">Replay Assembly Preview</h2>
        {!selectedProvider ? (
          <EmptyState title="Select a provider for assembly preview" icon="[]" />
        ) : replayLoading ? (
          <LoadingSkeleton rows={4} />
        ) : !replayPreview?.available ? (
          <EmptyState title={replayPreview?.reason || 'Replay preview unavailable'} icon="[]" />
        ) : (
          <div className="space-y-3">
            <div className="flex items-center gap-4 text-xs text-gray-500">
              <span>rows: {replayPreview.row_count}</span>
              <span>mode: {replayPreview.summary?.metadata?.alignment_mode || replayPreview.summary?.request?.alignment_mode}</span>
              <span>datasets: {(replayPreview.summary?.components || []).length}</span>
            </div>
            {!replayPreview.data?.length ? (
              <EmptyState title="No assembled rows for current scope" icon="[]" />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-surface-border text-left text-gray-500">
                      {replayColumns.map((column) => (
                        <th key={column} className="pb-2 pr-4">{column}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-surface-border">
                    {(replayPreview.data || []).map((row, index) => (
                      <tr key={index}>
                        {replayColumns.map((column) => (
                          <td key={column} className="py-2 pr-4 text-gray-300">
                            {row[column] == null ? '-' : String(row[column])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
