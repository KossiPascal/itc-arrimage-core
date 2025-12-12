import { useState } from "react";

// ----- ResultsTable -----
export default function ResultsTable({ rows, error, loading }) {
  const [hoveredRow, setHoveredRow] = useState(null);

  if (loading) {
    return <div className="p-4 bg-blue-50 border border-blue-200 text-blue-800 rounded mt-4 text-center">Chargement...</div>;
  }
  if (error) {
    return <div className="p-4 bg-red-50 border border-red-200 text-red-800 rounded mt-4 text-center">Erreur : {error}</div>;
  }
  if (!rows || !Array.isArray(rows) || rows.length === 0) {
    return <div className="p-4 bg-yellow-50 border border-yellow-200 text-yellow-800 rounded mt-4 text-center">Aucun résultat à afficher</div>;
  }

  const columns = Object.keys(rows[0] || {});

  const renderCell = (value) => {
    if (value === null || value === undefined) return <span className="text-gray-400">NULL</span>;
    if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
    if (value instanceof Date) return value.toLocaleString();
    return value.toString();
  };

  return (
    <div style={{ maxHeight: "500px" }} className="overflow-x-auto overflow-y-auto mt-4 border rounded shadow-sm">
      <table className="min-w-full border-collapse">
        <thead className="bg-gray-100">
          <tr>
            {columns.map((key) => (
              <th key={key} className="px-4 py-2 border text-left font-semibold text-sm">{key}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              className={`hover:bg-gray-50 ${hoveredRow === i ? "bg-gray-50" : ""}`}
              onMouseEnter={() => setHoveredRow(i)}
              onMouseLeave={() => setHoveredRow(null)}
            >
              {columns.map((col, j) => (
                <td key={j} className="px-4 py-2 border text-sm">{renderCell(row[col])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

