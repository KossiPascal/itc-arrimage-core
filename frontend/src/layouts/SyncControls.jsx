// import React from "react";

export default function SyncControls({ onSync, syncing, onRefresh }) {
  return (
    <div className="flex gap-3 mb-6">
      <button
        className={`px-4 py-2 rounded-md text-white ${syncing ? "bg-gray-400" : "bg-blue-600 hover:bg-blue-700"} shadow`}
        onClick={onSync}
        disabled={syncing}
      >
        {syncing ? "Synchronisation..." : "Synchroniser maintenant"}
      </button>

      <button className="px-4 py-2 border rounded-md" onClick={onRefresh} disabled={syncing}>
        Rafraîchir
      </button>
    </div>
  );
}