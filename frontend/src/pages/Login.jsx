// frontend/src/pages/Login.jsx
import React, { useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";

export default function Login() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [err, setErr] = useState(null);
  const { login } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();
  const from = location.state?.from?.pathname || "/sync";

  const handleSubmit = async (e) => {
    e.preventDefault();
    setErr(null);
    try {
      await login(username, password);
      navigate(from, { replace: true });
    } catch (error) {
      console.error(error);
      // setErr("Échec de la connexion — vérifie tes identifiants.");
      setErr(err.response?.data?.error || err.message);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 p-4">
      <form onSubmit={handleSubmit} className="w-full max-w-md bg-white p-6 rounded shadow">
        <h2 className="text-xl font-bold mb-4">Connexion</h2>

        {err && <div className="mb-3 text-red-600">{err}</div>}

        <label className="block mb-2">
          <span className="text-sm">Nom d'utilisateur</span>
          <input value={username} onChange={(e)=>setUsername(e.target.value)} className="mt-1 block w-full px-3 py-2 border rounded" />
        </label>

        <label className="block mb-4">
          <span className="text-sm">Mot de passe</span>
          <input type="password" value={password} onChange={(e)=>setPassword(e.target.value)} className="mt-1 block w-full px-3 py-2 border rounded" />
        </label>

        <button className="w-full bg-blue-600 text-white py-2 rounded">Se connecter</button>
      </form>
    </div>
  );
}
