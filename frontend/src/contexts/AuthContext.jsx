import React, { createContext, useContext, useEffect, useState } from "react";
import api from "../utils/api";

const TIMEOUT = Number(import.meta.env.VITE_API_TIMEOUT || 120) * 1000;

const AuthContext = createContext();

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  /**
   * Clear auth storage
   */
  const clearStorage = () => {
    localStorage.removeItem("access_token");
    localStorage.removeItem("refresh_token");
    localStorage.removeItem("user");
  };

  /**
   * Restore session
   */
  useEffect(() => {
    const restore = async () => {
      const token = localStorage.getItem("access_token");
      const storedUser = localStorage.getItem("user");

      if (!token || !storedUser) {
        setLoading(false);
        return;
      }

      try {
        const parsedUser = JSON.parse(storedUser);
        setUser(parsedUser);
        setIsAuthenticated(true);

        // Optional: validate token with backend
        await api.get("/auth/me", {
          headers: { Authorization: `Bearer ${token}` },
        });
      } catch (err) {
        console.warn("Session restore failed", err);
        clearStorage();
        setUser(null);
        setIsAuthenticated(false);
      }

      setLoading(false);
    };

    restore();
  }, []);

  /**
   * Login
   */
  const login = async (username, password) => {
    setError(null);
    try {
      const res = await api.post("/auth/login", { username, password });
      const { access_token, refresh_token, user } = res.data;

      localStorage.setItem("access_token", access_token);
      localStorage.setItem("refresh_token", refresh_token);
      localStorage.setItem("user", JSON.stringify(user));

      setUser(user);
      setIsAuthenticated(true);

      return user;
    } catch (err) {
      const msg = err.response?.data?.error || err.message;
      setError(msg);
      throw new Error(msg);
    }
  };

  /**
   * Register
   */
  const register = async ({ username, fullname, password, role }) => {
    setError(null);
    try {
      const res = await api.post("/user/register", { username, fullname, password, role });
      return res.data;
    } catch (err) {
      const msg = err.response?.data?.error || err.message;
      setError(msg);
      throw new Error(msg);
    }
  };

  /**
   * Get all users
   */
  const getUsers = async () => {
    setError(null);
    try {
      const res = await api.get("/user/all");
      return res.data;
    } catch (err) {
      const msg = err.response?.data?.error || err.message;
      setError(msg);
      throw new Error(msg);
    }
  };

  /**
   * Update user
   */
  const updateUser = async (id, { fullname, password, role }) => {
    setError(null);
    try {
      const res = await api.put(`/user/update/${id}`, { fullname, password, role });
      return res.data;
    } catch (err) {
      const msg = err.response?.data?.error || err.message;
      setError(msg);
      throw new Error(msg);
    }
  };

  /**
   * Update user
   */
  const updateUserPassword = async (id, old_password, new_password) => {
    setError(null);
    try {
      const res = await api.put(`/user/update-password/${id}`, { old_password, new_password });
      return res.data;
    } catch (err) {
      const msg = err.response?.data?.error || err.message;
      setError(msg);
      throw new Error(msg);
    }
  };
  /**
   * Update user
   */
  const adminUpdateUserPassword = async (id, new_password) => {
    setError(null);
    try {
      const res = await api.put(`/user/admin-update-password/${id}`, { new_password });
      return res.data;
    } catch (err) {
      const msg = err.response?.data?.error || err.message;
      setError(msg);
      throw new Error(msg);
    }
  };



  /**
   * Delete user
   */
  const deleteUser = async (id) => {
    setError(null);
    try {
      const res = await api.delete(`/user/delete/${id}`);
      return res.data;
    } catch (err) {
      const msg = err.response?.data?.error || err.message;
      setError(msg);
      throw new Error(msg);
    }
  };

  /**
   * Logout
   */
  const logout = async () => {
    const refresh_token = localStorage.getItem("refresh_token");

    try {
      if (refresh_token) {
        await api.post("/auth/logout", { refresh_token });
      }
    } catch (err) {
      console.warn("Logout error", err);
    }

    clearStorage();
    setUser(null);
    setIsAuthenticated(false);

    window.location.href = "/login";
  };

  /**
   * Refresh access token
   */
  const refreshToken = async () => {
    const refresh_token = localStorage.getItem("refresh_token");
    if (!refresh_token) return null;

    try {
      const res = await api.post("/auth/refresh", { refresh_token }, { timeout: TIMEOUT, withCredentials: true });
      const { access_token, refresh_token: newRefreshToken } = res.data;

      // Mise à jour tokens
      localStorage.setItem("access_token", access_token);
      if (newRefreshToken) {
        localStorage.setItem("refresh_token", newRefreshToken);
      }

      return access_token;
    } catch (err) {
      console.error("Refresh token failed", err);
      logout();
      return null;
    }
  };

  return (
    <AuthContext.Provider
      value={{
        user,
        isAdmin: user && user.role && ["admin", "superadmin"].includes(user.role),
        isSuperAdmin: user?.role === "superadmin",
        loading,
        error,
        isAuthenticated,
        login,
        register,
        logout,
        refreshToken,
        getUsers,
        updateUser,
        updateUserPassword,
        adminUpdateUserPassword,
        deleteUser,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
