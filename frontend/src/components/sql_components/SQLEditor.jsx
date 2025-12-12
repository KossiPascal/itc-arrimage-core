import { useEffect, useState, useRef } from "react";
import Editor from "@monaco-editor/react";
import api from "../../utils/api";
import { useTheme } from "../../utils/ThemeContext";
import { useAuth } from "../../contexts/AuthContext";


export default function SQLEditor({ onExecute, selectedQuery, setSelectedQuery }) {

    const emptyQuery = { id: null, name: "", sql: "" };

    const [query, setQuery] = useState(emptyQuery);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [executeSql, setExecuteSql] = useState(true);
    const [editorHeight, setEditorHeight] = useState(300); // hauteur initiale en px
    const { theme, toggleTheme } = useTheme();
    const { user } = useAuth();

    const editorRef = useRef(null);

    // ---------------- LOAD SELECTED QUERY ----------------
    // Load query into editor
    useEffect(() => {
        if (selectedQuery) {
            setQuery({
                id: selectedQuery.id || null,
                name: selectedQuery.name || "",
                sql: selectedQuery.sql || ""
            });
        }
    }, [selectedQuery]);

    // Auto run only after sql value updated
    useEffect(() => {
        if (query.sql && query.sql.trim() !== "" && executeSql) {
            executeSQL();
        }
        setExecuteSql(true);
    }, [query.sql]);

    // ---------------- EXECUTE SQL ----------------
    const executeSQL = async () => {
        const sqlToRun = (query.sql || "").trim();

        // const safeSQL = sqlToRun.replace(/'/g, "\\'").replace(/\n/g, " ");
        // const safeSQL = encodeURIComponent(sqlToRun);
        // {decodeURIComponent(q)}

        if (!sqlToRun) {
            const msg = "La requête SQL ne peut pas être vide.";
            setError(msg);
            onExecute([], msg);
            return;
        }

        if (!user.isAdmin) {
            const blocked = ["DROP ", "TRUNCATE ", "ALTER ", "GRANT ", "REVOKE "];
            if (blocked.some(cmd => sqlToRun.toUpperCase().includes(cmd))) {
                const msg = "❌ Commande SQL dangereuse bloquée.";
                setError(msg);
                onExecute([], msg);
                return;
            }
        }

        setLoading(true);
        setError(null);

        try {
            const res = await api.post("/sql/execute", {
                user_id: user.id,
                sql: sqlToRun,
                max_rows: null   // si undefined → null 
            });

            onExecute(res?.data?.rows ?? [], null);

        } catch (err) {
            const msg = err.response?.data?.error || err.message;
            setError(msg);
            onExecute([], msg);
        } finally {
            setLoading(false);
        }
    };

    // ---------------- RESET ----------------
    const resetSQL = () => {
        setQuery(emptyQuery);
        setSelectedQuery(null);
        setError(null);
    };

    // ---------------- SAVE / UPDATE ----------------
    const saveQuery = async () => {
        if (!query.name.trim()) {
            setError("Le nom de la requête est obligatoire.");
            return;
        }

        setLoading(true);
        setError(null);

        const payload = { name: query.name, sql: query.sql };

        try {
            if (query.id) {
                await api.put(`/query/${query.id}`, payload);
            } else {
                const res = await api.post("/query/", payload);
                setQuery(prev => ({ ...prev, id: res.data.id }));
            }
        } catch (err) {
            setError(err.response?.data?.error || err.message);
        } finally {
            setLoading(false);
        }
    };

    // ---------------- DELETE ----------------
    const deleteQuery = async () => {
        if (!query.id) return;
        if (!window.confirm("Supprimer cette requête ?")) return;

        setLoading(true);

        try {
            await api.delete(`/query/${query.id}`);
            resetSQL();
        } catch (err) {
            setError(err.response?.data?.error || err.message);
        } finally {
            setLoading(false);
        }
    };

    const onEditorChange = (sql) => {
        setExecuteSql(false);
        setQuery({ ...query, sql: sql || "" });
    }

    const handleEditorDidMount = (editor) => {
        editorRef.current = editor;

        const updateHeight = () => {
            if (!editor) return;

            // Hauteur réelle du contenu
            const rawHeight = editor.getContentHeight();

            // Applique des bornes strictes
            const newHeight = Math.min(800, Math.max(200, rawHeight));

            // Empêche les micro-variations qui "poussent" la hauteur inutilement
            if (Math.abs(newHeight - editorHeight) > 10) {
                setEditorHeight(newHeight);
            }
        };

        // Débounce léger pour éviter spamming à chaque caractére
        let resizeTimer;
        editor.onDidContentSizeChange(() => {
            clearTimeout(resizeTimer);
            resizeTimer = setTimeout(updateHeight, 80);
        });

        // Initial
        updateHeight();
    };

    return (
        <div className="space-y-4">
            <div className="flex space-x-2 items-center">
                <label className="text-sm font-medium text-gray-500">Hauteur:</label>

                <input
                    type="range"
                    min="200"
                    max="1000"
                    value={editorHeight}
                    onChange={(e) => setEditorHeight(Number(e.target.value))}
                    className="w-40"
                />

                <span className="text-sm text-gray-600">{editorHeight}px</span>
            </div>

            {/* SQL EDITOR */}
            <Editor
                height={`${editorHeight}px`} // ← ici on contrôle la hauteur
                language="sql"
                value={query.sql}
                onChange={(sql) => onEditorChange(sql)}
                theme={theme === "dark" ? "vs-dark" : "light"}
                options={{
                    minimap: { enabled: false },
                    fontSize: 14,
                    automaticLayout: true,
                    scrollBeyondLastLine: false,
                    wordWrap: "on",
                }}
                onMount={handleEditorDidMount}

            />

            {/* ERROR */}
            {error && (
                <div className="text-red-700 bg-red-100 p-2 rounded">
                    {error}
                </div>
            )}

            {/* BUTTONS */}
            <div className="flex space-x-2 flex-wrap">

                <button
                    onClick={toggleTheme}
                    className="px-4 py-2 bg-blue-600 text-white rounded"
                >
                    Thème
                </button>

                <button
                    onClick={executeSQL}
                    disabled={loading}
                    className={`px-4 py-2 rounded text-white ${loading ? "bg-gray-400" : "bg-blue-600 hover:bg-blue-700"
                        }`}
                >
                    {loading ? "Exécution..." : "Exécuter"}
                </button>

                <button
                    onClick={resetSQL}
                    disabled={loading}
                    className="px-4 py-2 bg-gray-500 hover:bg-gray-600 text-white rounded"
                >
                    Réinitialiser
                </button>
            </div>
        </div>
    );

}
