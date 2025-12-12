// import React from "react";

export default function NavbarControls({ columns, reportView, setReportView, lastSyncAt }) {
  return (
    <div className="flex items-center justify-between mt-8 mb-6">
      <div>
        <h1 className="text-2xl font-bold">{import.meta.env.VITE_APP_NAME || 'DHIS2 Sync Dashboard'} </h1>
        <p className="text-sm text-gray-500">Visualise et synchronise les Data Elements</p>
      </div>

      <div className="mb-4 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <div className="flex items-center gap-3">
          <div className="text-sm text-gray-600">Source :</div>

          <select value={reportView} onChange={(e) => setReportView(e.target.value)} className="px-2 py-1 border rounded">
            {columns.map((col) => (
              <option value={col}>{col.toUpperCase()}</option>
            ))}
          </select>
        </div>
      </div>

      <div className="text-right">
        <div className="text-sm text-gray-600">Status: <span className="font-medium text-green-600">Connected</span></div>
        <div className="text-xs text-gray-500 mt-1">
          Dernière sync: {lastSyncAt ? new Date(lastSyncAt).toLocaleString() : "Jamais"}
        </div>
      </div>
    </div>
  );
}