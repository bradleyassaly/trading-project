export default function EmptyState({ title = 'No data', reason, hint, icon = '📭' }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center space-y-3 text-gray-500">
      <span className="text-4xl select-none">{icon}</span>
      <p className="text-sm font-medium text-gray-400">{title}</p>
      {reason && <p className="text-xs max-w-xs">{reason}</p>}
      {hint && (
        <p className="text-xs text-gray-600 max-w-xs border border-surface-border rounded-md px-3 py-2 bg-surface-card font-mono">
          {hint}
        </p>
      )}
    </div>
  )
}
