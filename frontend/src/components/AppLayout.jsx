// frontend/src/components/AppLayout.jsx
import React from "react";
import Navbar from "./Navbar";

/**
 * AppLayout: wrapper pour les pages protégées avec Navbar et espace contenu
 * Utilise flex pour que le contenu occupe l'espace restant sous la Navbar
 */
export default function AppLayout({ children }) {
  return (
    <div className="min-h-screen flex flex-col bg-gray-50">
      {/* Navbar fixe en haut */}
      <Navbar />

      {/* Contenu principal */}
      <main className="flex-1 p-6 mt-4">
        {children}
      </main>
    </div>
  );
}
