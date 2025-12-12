import { useMemo, useState } from "react";
import api from "./api";

/**
 * useTable
 * A universal, fully dynamic hook for tables in React.
 * Handles: search, sorting, pagination, loading, API sync, error handling.
 */

export default function useTable(initialData = [], defaultSortBy = "id", defaultSortDir = "desc") {
  // ---------------------------------------------------------------------------
  //                             STATE
  // ---------------------------------------------------------------------------

  const [rows, setRows] = useState(initialData);
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState(null);
  const [lastSyncAt, setLastSyncAt] = useState(null);

  // Controls
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(12);

  const [sortBy, setSortBy] = useState(defaultSortBy);
  const [sortDir, setSortDir] = useState(defaultSortDir);

  // ---------------------------------------------------------------------------
  //                         DYNAMIC COLUMN EXTRACTION
  // ---------------------------------------------------------------------------
  const columns = useMemo(() => {
    if (rows.length === 0) return [];
    return Object.keys(rows[0]);
  }, [rows]);

  // ---------------------------------------------------------------------------
  //                            SEARCH (FILTERING)
  // ---------------------------------------------------------------------------
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return rows;

    const filteredRows = rows.filter((row) =>
      Object.values(row).some((val) =>
        (val ?? "")
          .toString()
          .toLowerCase()
          .includes(q)
      )
    );

    // Reset pagination when searching
    setPage(1);

    return filteredRows;
  }, [query, rows]);

  // ---------------------------------------------------------------------------
  //                           SORTING UTILS
  // ---------------------------------------------------------------------------

  const parseValue = (value) => {
    if (value == null) return "";

    // If value is a date
    if (!isNaN(Date.parse(value))) return new Date(value).getTime();

    // If value is a number
    if (!isNaN(Number(value))) return Number(value);

    // Otherwise compare as lowercase string
    return value.toString().toLowerCase();
  };

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      const A = parseValue(a[sortBy]);
      const B = parseValue(b[sortBy]);

      if (A === B) return 0;

      if (sortDir === "asc") return A > B ? 1 : -1;
      return A > B ? -1 : 1;
    });
  }, [filtered, sortBy, sortDir]);

  // ---------------------------------------------------------------------------
  //                                 PAGINATION
  // ---------------------------------------------------------------------------
  const total = sorted.length;

  const pageData = useMemo(() => {
    const start = (page - 1) * pageSize;
    return sorted.slice(start, start + pageSize);
  }, [sorted, page, pageSize]);

  // ---------------------------------------------------------------------------
  //                               API METHODS
  // ---------------------------------------------------------------------------

  const fetchData = async (route) => {
    setLoading(true);
    setError(null);

    try {
      const res = await api.get(route);
      const data = Array.isArray(res.data) ? res.data : [];

      setRows(data);

      // Auto detect last sync
      if (data.length > 0 && data[0].synced_at) {
        setLastSyncAt(data[0].synced_at);
      }
    } catch (err) {
      console.error(err);
      setError("Erreur lors du chargement des données.");
    } finally {
      setLoading(false);
    }
  };

  const syncNow = async (postRoute, getRoute) => {
    setSyncing(true);
    setError(null);

    try {
      await api.post(postRoute);
      await fetchData(getRoute);
    } catch (err) {
      console.error(err);
      setError("Échec de la synchronisation.");
    } finally {
      setSyncing(false);
    }
  };

  // ---------------------------------------------------------------------------
  //                                 SORTING CONTROL
  // ---------------------------------------------------------------------------

  const handleSort = (column) => {
    if (sortBy === column) {
      setSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(column);
      setSortDir("asc");
    }
  };

  // ---------------------------------------------------------------------------
  //                                  RETURN API
  // ---------------------------------------------------------------------------

  return {
    // raw values
    rows,
    columns,

    // UI state
    loading,
    syncing,
    error,
    lastSyncAt,

    // controls
    query,
    setQuery,
    sortBy,
    setSortBy,
    sortDir,
    setSortDir,
    handleSort,
    page,
    setPage,
    pageSize,
    setPageSize,

    // computed
    total,
    pageData,

    // methods
    fetchData,
    syncNow,
  };
}
