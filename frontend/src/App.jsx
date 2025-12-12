import React, { useEffect } from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import AppLayout from "./components/AppLayout";
import { ThemeProvider } from "./utils/ThemeContext";

import Login from "./pages/Login";
import SyncDhis2 from "./pages/SyncDhis2";
import ArrimateData from "./pages/ArrimateData";
import Register from "./pages/Register";
import SQLDashboard from "./pages/SQLDashboard";


/**
 * ProtectedRoute: protège les routes nécessitant une authentification.
 */
function ProtectedRoute({ children }) {
  const { isAuthenticated, loading } = useAuth();
  if (loading) return null; // éviter scintillement
  return isAuthenticated ? children : <Navigate to="/login" replace />;
}

export default function App() {

  useEffect(() => {
    document.title = import.meta.env.VITE_APP_NAME || "ARRIMAGE DHIS2";
  }, []);
  return (
    <ThemeProvider>
        <AuthProvider>
          <Routes>
            {/* ---- PUBLIC ROUTES ---- */}
            <Route path="/login" element={<Login />} />


            {/* ---- PROTECTED ROUTES ---- */}
            <Route path="/register" element={
                <ProtectedRoute>
                  <AppLayout>
                    <Register />
                  </AppLayout>
                </ProtectedRoute>
            }/>

            <Route path="/sync" element={
                <ProtectedRoute>
                  <AppLayout>
                    <SyncDhis2 />
                  </AppLayout>
                </ProtectedRoute>
            }/>

            <Route path="/arrimate" element={
                <ProtectedRoute>
                  <AppLayout>
                    <ArrimateData />
                  </AppLayout>
                </ProtectedRoute>
            }/>
            
            
            <Route path="/postgresql" element={
                <ProtectedRoute>
                  <AppLayout>
                    <SQLDashboard />
                  </AppLayout>
                </ProtectedRoute>
            }/>


            {/* ---- ROOT → redirection intelligente ---- */}
            <Route path="/" element={
                <ProtectedRoute>
                  <Navigate to="/sync" replace />
                </ProtectedRoute>
            }/>

            {/* ---- CATCH-ALL ---- */}
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </AuthProvider>
    </ThemeProvider>
  );
}


// function ThemeWrapper({ children }) {
//   const { theme, toggleTheme } = useTheme();
//   return (
//     <div className={theme === "dark" ? "bg-gray-900 text-white min-h-screen" : "bg-white text-black min-h-screen"}>
//       <button
//         onClick={toggleTheme}
//         className="m-4 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
//       >
//         Toggle Theme
//       </button>
//       {children}
//     </div>
//   );
// }