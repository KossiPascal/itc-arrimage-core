import React, { useEffect, useState } from "react";
import { RefreshCw, Table, Eye, Layers3 } from "lucide-react";
import api from "../../utils/api";

export default function SchemaViewer({ onRunSql }) {
  const [tables, setTables] = useState([]);
  const [views, setViews] = useState([]);
  const [matviews, setMatviews] = useState([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [openTables, setOpenTables] = useState(true);
  const [openViews, setOpenViews] = useState(true);
  const [activeTab, setActiveTab] = useState("tables");

  const loadSchema = async () => {
    setLoading(true);
    setError(null);

    try {
      const res = await api.get("/schema/schema_info");
      if (!res.data) throw new Error("Format de réponse inattendu or Réponse invalide");

      setTables(res.data.tables || []);
      setViews(res.data.views || []);
      setMatviews(res.data.matviews || []);

    } catch (err) {
      setError(err.response?.data?.error || err.message || "Erreur inconnue");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadSchema();
  }, []);

  const renderList = (items, key_to_show, icon) => (
    <ul className="space-y-1 mt-2">
      {items.map((item) => (
        <li
          key={item[key_to_show]}
          onClick={() => onRunSql(`SELECT * FROM "${item[key_to_show]}" LIMIT 5;`)}
          className="flex items-center space-x-2 px-3 py-2 rounded-lg cursor-pointer 
                     hover:bg-gray-100 dark:hover:bg-gray-700 transition">
          {icon}
          <span>{item[key_to_show]}</span>
        </li>
      ))}
    </ul>
  );

  // ---------------- RENDER ----------------

  return (

    // <div style={{ maxHeight: "500px" }} className="mt-6 p-4 rounded-xl border bg-white dark:bg-gray-900 dark:border-gray-700 shadow-sm">

    <div className="max-h-100 overflow-auto mt-6 p-4 rounded-xl border bg-white dark:bg-gray-900 dark:border-gray-700 shadow-sm">

      {/* HEADER */}
      <div className="flex justify-between items-center mb-4">
        <h3 className="font-bold text-lg text-gray-800 dark:text-gray-200">
          📊 Schéma PostgreSQL
        </h3>

        <button
          onClick={loadSchema}
          className="flex items-center space-x-1 px-3 py-1.5 rounded-lg bg-blue-600 
                     hover:bg-blue-700 text-white transition"
        >
          <RefreshCw size={16} />
          <span>Rafraîchir</span>
        </button>
      </div>

      {/* TABS */}
      <div className="flex border-b dark:border-gray-700 mb-3">

        <button
          onClick={() => setActiveTab("tables")}
          className={`px-4 py-2 font-semibold transition 
            ${activeTab === "tables"
              ? "border-b-2 border-blue-600 text-blue-600 dark:text-blue-400"
              : "text-gray-500 dark:text-gray-300 hover:text-gray-700 dark:hover:text-gray-100"
            }`
          }
        >
          Tables ({tables.length})
        </button>

        <button
          onClick={() => setActiveTab("views")}
          className={`px-4 py-2 font-semibold transition 
            ${activeTab === "views"
              ? "border-b-2 border-blue-600 text-blue-600 dark:text-blue-400"
              : "text-gray-500 dark:text-gray-300 hover:text-gray-700 dark:hover:text-gray-100"
            }`
          }
        >
          Views ({views.length})
        </button>

        <button
          onClick={() => setActiveTab("matviews")}
          className={`px-4 py-2 font-semibold transition 
            ${activeTab === "matviews"
              ? "border-b-2 border-blue-600 text-blue-600 dark:text-blue-400"
              : "text-gray-500 dark:text-gray-300 hover:text-gray-700 dark:hover:text-gray-100"
            }`
          }
        >
          Mat. Views ({matviews.length})
        </button>

      </div>

      {/* CONTENT */}
      {loading && (
        <div className="text-center py-4 text-blue-600 dark:text-blue-300 animate-pulse">
          Chargement...
        </div>
      )}

      {error && (
        <div className="p-4 bg-red-50 dark:bg-red-900 border border-red-300 
                        dark:border-red-700 text-red-700 dark:text-red-200 rounded-lg text-center">
          Erreur : {error}
        </div>
      )}

      {!loading && !error && (
        <>
          {activeTab === "tables" && renderList(
            tables,'table_name',
            <Table size={16} className="text-blue-600 dark:text-blue-300" />
          )}

          {activeTab === "views" && renderList(
            views, 'view_name',
            <Eye size={16} className="text-green-600 dark:text-green-300" />
          )}

          {activeTab === "matviews" && renderList(
            matviews, 'matview_name',
            <Layers3 size={16} className="text-purple-600 dark:text-purple-300" />
          )}
        </>
      )}
    </div>
  );

  // return (
  //   <div className="mt-6 p-4 border rounded-xl bg-white dark:bg-gray-900 shadow-sm">
  //     <div className="flex items-center justify-between border-b pb-2 mb-3">
  //       <h3 className="font-bold text-lg text-gray-800 dark:text-gray-200">
  //         📊 Schéma PostgreSQL
  //       </h3>

  //       <button
  //         onClick={loadSchema}
  //         className="flex items-center space-x-1 px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-700 text-white transition"
  //       >
  //         <RefreshCw size={16} />
  //         <span>Rafraîchir</span>
  //       </button>
  //     </div>

  //     {/* Loading */}
  //     {loading && (
  //       <div className="text-center py-4 text-blue-600 dark:text-blue-300 animate-pulse">
  //         Chargement du schéma...
  //       </div>
  //     )}

  //     {/* Error */}
  //     {error && !loading && (
  //       <div className="p-4 bg-red-50 dark:bg-red-900 text-red-700 dark:text-red-200 border border-red-300 rounded-lg text-center">
  //         ⚠️ Erreur : {error}
  //         <div>
  //           <button
  //             onClick={loadSchema}
  //             className="mt-2 px-3 py-1 bg-red-600 text-white rounded hover:bg-red-700"
  //           >
  //             Réessayer
  //           </button>
  //         </div>
  //       </div>
  //     )}

  //     {/* Empty */}
  //     {!loading && !error && tables.length === 0 && views.length === 0 && (
  //       <div className="p-4 bg-yellow-50 dark:bg-yellow-900 border border-yellow-300 text-yellow-700 dark:text-yellow-200 rounded-lg text-center">
  //         Aucune table ou vue trouvée
  //       </div>
  //     )}

  //     {/* MAIN CONTENT */}
  //     {!loading && !error && (
  //       <div className="space-y-4 text-sm">

  //         {/* TABLES */}
  //         {tables.length > 0 && (
  //           <div className="border border-gray-200 dark:border-gray-700 rounded-lg">
  //             <button
  //               className="w-full flex justify-between items-center px-4 py-2 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 transition"
  //               onClick={() => setOpenTables(!openTables)}
  //             >
  //               <div className="flex items-center space-x-2">
  //                 <Table size={16} />
  //                 <span className="font-semibold">Tables ({tables.length})</span>
  //               </div>
  //               <span>{openTables ? "▲" : "▼"}</span>
  //             </button>

  //             {openTables && (
  //               <ul className="px-6 py-2 space-y-1">
  //                 {tables.map((t) => (
  //                   <li
  //                     key={t}
  //                     className="px-3 py-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center space-x-2 transition"
  //                   >
  //                     <Table size={14} className="text-blue-600 dark:text-blue-300" />
  //                     <span>{t}</span>
  //                   </li>
  //                 ))}
  //               </ul>
  //             )}
  //           </div>
  //         )}

  //         {/* VIEWS */}
  //         {views.length > 0 && (
  //           <div className="border border-gray-200 dark:border-gray-700 rounded-lg">
  //             <button
  //               className="w-full flex justify-between items-center px-4 py-2 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 transition"
  //               onClick={() => setOpenViews(!openViews)}
  //             >
  //               <div className="flex items-center space-x-2">
  //                 <Eye size={16} />
  //                 <span className="font-semibold">Vues ({views.length})</span>
  //               </div>
  //               <span>{openViews ? "▲" : "▼"}</span>
  //             </button>

  //             {openViews && (
  //               <ul className="px-6 py-2 space-y-1">
  //                 {views.map((v) => (
  //                   <li
  //                     key={v}
  //                     className="px-3 py-1 rounded hover:bg-gray-100 dark:hover:bg-gray-700 cursor-pointer flex items-center space-x-2 transition"
  //                   >
  //                     <Eye size={14} className="text-green-600 dark:text-green-300" />
  //                     <span>{v}</span>
  //                   </li>
  //                 ))}
  //               </ul>
  //             )}
  //           </div>
  //         )}

  //       </div>
  //     )}
  //   </div>
  // );
}
