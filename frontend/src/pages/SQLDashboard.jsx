import { useState } from "react";

import SQLEditor from "../components/sql_components/SQLEditor";
import ResultsTable from "../components/sql_components/ResultsTable";
import SchemaViewer from "../components/sql_components/SchemaViewer";


export default function SQLDashboard() {
  const [selectedQuery, setSelectedQuery] = useState(null);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleExecute = (rows, err) => {
    if (err) {
      setError(err);
      setResults([]);
    } else {
      setError(null);
      setResults(rows || []);
    }
  };

  const setSqlToRun = (sql) => {
    const query = { id: null, name: '', sql };
    setSelectedQuery(query);
  };


  return (
    <div className="max-w-6xl mx-auto p-6 space-y-4">
      <h1 className="text-2xl font-bold mb-4">PostgreSQL SQL Editor</h1>

      <SQLEditor
        onExecute={handleExecute}
        selectedQuery={selectedQuery}
        setSelectedQuery={setSelectedQuery}
      />

      <ResultsTable
        rows={results}
        error={error}
        loading={loading}
      />

      <SchemaViewer
        onRunSql={(sql) => setSqlToRun(sql)}
      />

    </div>
  );
}
