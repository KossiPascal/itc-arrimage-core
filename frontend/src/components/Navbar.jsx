// frontend/src/components/Navbar.jsx
import React, { useState, useEffect } from "react";
import { Link, NavLink, useNavigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";

export default function Navbar() {
  const { user, isAdmin, isSuperAdmin, logout } = useAuth();

  const navigate = useNavigate();
  const [open, setOpen] = useState(false);

  const handleLogout = async () => {
    if (confirm("Souhaitez-vous vraiment vous déconnecter?")) {
      try {
        await logout();
        navigate("/login", { replace: true });
      } catch (err) {
        console.error("Logout failed:", err);
      }
    }

  };

  const navLinks = [
    { name: "SYNC", path: "/sync", show: true },
    { name: "ARRIMAGE", path: "/arrimate", show: true },
    { name: "SQL", path: "/postgresql", show: (isSuperAdmin || isAdmin) == true },
    { name: "USERS", path: "/register", show: (isSuperAdmin || isAdmin) == true },

    // ajoute ici d'autres modules si nécessaire
  ];


  return (
    <nav className="bg-white shadow-md fixed w-full z-50 top-0">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          {/* Logo / Brand */}
          <div className="flex items-center">
            <Link to="/sync" className="text-xl font-bold text-blue-600">
              {import.meta.env.VITE_APP_SUBNAME || 'ITC App'}
            </Link>
          </div>

          {/* Desktop Links */}
          <div className="hidden md:flex md:items-center md:space-x-6">
            {navLinks.map((link) => 
              link.show ? (
                <NavLink
                key={link.path}
                to={link.path}
                className={({ isActive }) =>
                  `text-gray-700 hover:text-blue-600 ${isActive ? "font-semibold underline" : ""
                  }`
                }
              >
                {link.name}
              </NavLink>
              ):null
            )}
          </div>

          {/* User Menu */}
          <div className="flex items-center space-x-4">
            {user && (
              <div className="text-gray-700 text-sm font-medium">
                {user.username} ({user.role})
              </div>
            )}
            <button
              onClick={handleLogout}
              className="px-3 py-1 bg-red-600 text-white rounded hover:bg-red-700"
            >
              Logout
            </button>

            {/* Mobile menu button */}
            <div className="md:hidden">
              <button
                onClick={() => setOpen(!open)}
                className="text-gray-700 hover:text-blue-600 focus:outline-none"
              >
                <svg
                  className="h-6 w-6"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                  xmlns="http://www.w3.org/2000/svg"
                >
                  {open ? (
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M6 18L18 6M6 6l12 12"
                    />
                  ) : (
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M4 6h16M4 12h16M4 18h16"
                    />
                  )}
                </svg>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Mobile Menu */}
      {open && (
        <div className="md:hidden bg-white shadow-md">
          <div className="px-2 pt-2 pb-3 space-y-1">
            {navLinks.map((link) => (
              <NavLink
                key={link.path}
                to={link.path}
                onClick={() => setOpen(false)}
                className={({ isActive }) =>
                  `block px-3 py-2 rounded text-gray-700 hover:text-blue-600 ${isActive ? "font-semibold underline" : ""
                  }`
                }
              >
                {link.name}
              </NavLink>
            ))}
          </div>
        </div>
      )}
    </nav>
  );
}
