import axios from "axios";

// ------------------------------------------------------
// 🔧 CONFIGURATION
// ------------------------------------------------------
const API_URL = import.meta.env.VITE_API_URL || "/api";
const TIMEOUT = Number(import.meta.env.VITE_API_TIMEOUT || 120) * 1000;

// axios instance principale
const api = axios.create({
  baseURL: API_URL,
  timeout: TIMEOUT,
  headers: { "Content-Type": "application/json" },
});

// Pour éviter plusieurs refresh simultanés
let isRefreshing = false;
let pendingRequests = [];

// Fonction pour rejouer les requêtes en attente
const processQueue = (error, token = null) => {
  pendingRequests.forEach((promise) => {
    if (error) promise.reject(error);
    else promise.resolve(token);
  });
  pendingRequests = [];
};

// ------------------------------------------------------
// 🔐 REQUEST INTERCEPTOR
// Injecte automatiquement le access token
// ------------------------------------------------------
api.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem("access_token");
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// ------------------------------------------------------
// 🔐 RESPONSE INTERCEPTOR
// Gère : token expiré, refresh automatique, logout forcé,
// boucles infinies, multi-onglets
// ------------------------------------------------------
api.interceptors.response.use(
  (response) => response,

  async (error) => {
    const originalRequest = error.config;

    // ⚠ Aucun accès → peut être offline
    if (!error.response) {
      console.error("Network/server error:", error.message);
      return Promise.reject(error);
    }

    const status = error.response.status;
    const errorMsg = error.response.data?.error?.toLowerCase() || "";


    // ------------------------------------------------------
    // 📌 CAS 1 : 🟡 403 — PAS de déconnexion !
    // ------------------------------------------------------
    if (status === 403) {
      // On laisse l'appelant gérer (UI peut afficher "Accès refusé")
      return Promise.reject(error);
    }

    // ------------------------------------------------------
    // 📌 CAS 1 : Access token expiré → 401 + token_expired
    // ------------------------------------------------------
    const isExpiredToken = status === 401 && errorMsg.includes("expired") && !originalRequest._retry;

    if (isExpiredToken) {
      originalRequest._retry = true;

      const refreshToken = localStorage.getItem("refresh_token");
      if (!refreshToken) {
        return forceLogout(); // vrai cas logout
      }

      // --------------------------------------------------
      // 🛡 Empêcher plusieurs refresh simultanés
      // --------------------------------------------------
      if (isRefreshing) {
        return new Promise((resolve, reject) => {
          pendingRequests.push({ resolve, reject });
        })
          .then((token) => {
            originalRequest.headers.Authorization = `Bearer ${token}`;
            return api(originalRequest);
          })
          .catch((err) => Promise.reject(err));
      }

      isRefreshing = true;

      try {
        const res = await axios.post(
          `${API_URL}/auth/refresh`, 
          { refresh_token: refreshToken }, 
          { timeout: TIMEOUT, withCredentials: true }
        );

        const { access_token, refresh_token: newRefreshToken } = res.data;

        // Mise à jour tokens
        localStorage.setItem("access_token", access_token);
        if (newRefreshToken) {
          localStorage.setItem("refresh_token", newRefreshToken);
        }

        isRefreshing = false;
        processQueue(null, access_token);

        // rejoue la requête d’origine
        originalRequest.headers.Authorization = `Bearer ${access_token}`;
        return api(originalRequest);

      } catch (err) {
        isRefreshing = false;
        processQueue(err, null);
        return forceLogout();
      }
    }

    // ------------------------------------------------------
    // 📌 CAS 2 : Refresh token invalide / expiré
    // ------------------------------------------------------
    if (status === 401 && errorMsg.includes("refresh")) {
      return forceLogout();
    }

    if (status === 498) {  // Token invalid cases
      return forceLogout();
    }

    // ------------------------------------------------------
    // 📌 CAS 3 : Autres erreurs
    // ------------------------------------------------------
    console.error("API error:", error.response.data || error.message);
    return Promise.reject(error);
  }
);

// ------------------------------------------------------
// 🔒 LOGOUT COMPLET ET PROPRE
// ------------------------------------------------------
function forceLogout() {
  localStorage.removeItem("access_token");
  localStorage.removeItem("refresh_token");
  localStorage.removeItem("user");

  if (window.location.pathname !== "/login") {
    window.location.href = "/login";
  }

  return Promise.reject(new Error("Session expired"));
}

export default api;
