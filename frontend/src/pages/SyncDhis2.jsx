// frontend/src/pages/SyncDhis2.jsx
import React, { useState } from "react";
import api from "../utils/api";
import { useAuth } from "../contexts/AuthContext";

export default function SyncDhis2() {
  const { user } = useAuth();

  const STEPS = [
    { key: "orgunits", label: "Syncroniser Orgunits", endpoint: "orgunits" },
    { key: "dataElements", label: "Syncroniser DataElements", endpoint: "dataElements" },
    { key: "teis", label: "Syncroniser TEI", endpoint: "teis_enrollments_events_attributes" },
    { key: "enrollments", label: "Syncroniser Enrollments", endpoint: "teis_enrollments_events_attributes" },
    { key: "events", label: "Syncroniser Events", endpoint: "teis_enrollments_events_attributes" },
    { key: "attributes", label: "Syncroniser Attributes", endpoint: "teis_enrollments_events_attributes" },
    { key: "matview", label: "Build/Rebuild MatView", endpoint: "build-matview" },
  ];


  const [enabledSteps, setEnabledSteps] = useState({
    orgunits: false,
    dataElements: false,
    teis: false,
    enrollments: false,
    events: false,
    attributes: false,
  });

  const [status, setStatus] = useState({});
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(null);
  const [success, setSuccess] = useState(null);

  const apiCall = async (endpoint, params = {}) => {
    let url = `/sync/${endpoint}`
    if (["build-matview"].includes(endpoint)){
      url = endpoint
    }
    const res = await api.post(url, params).catch((error) => {
      throw error.response?.data?.error || error.message;
    });
    return res.data;
  };

  const updateStatus = (key, value) => {
    setStatus((prev) => ({ ...prev, [key]: value }));
  };

  const toggleStep = (key) => {
    setEnabledSteps((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  const handleSync = async () => {
    setErr(null);
    setSuccess(null);
    setLoading(true);
    setStatus({});

    try {
      // Préparer les actions
      const actions = [];
      let dataStepsEnabled = false;
      const dataStepParams = {};

      for (const step of STEPS) {
        if (["teis", "enrollments", "events", "attributes"].includes(step.key)) {
          dataStepsEnabled = dataStepsEnabled || enabledSteps[step.key];
          dataStepParams[step.key] = enabledSteps[step.key];
        } else {
          actions.push({ key: step.key, ok: enabledSteps[step.key], endpoint: step.endpoint, params: {} });
        }
      }

      if (dataStepsEnabled) {
        actions.push({
          key: "data",
          ok: true,
          endpoint: "teis_enrollments_events_attributes",
          params: dataStepParams,
        });
      }

      // Exécuter les actions
      for (const action of actions) {
        if (!action.ok) continue;

        if (action.key === "data") {
          // Sous-étapes multi-data
          for (const subKey of ["teis", "enrollments", "events", "attributes"]) {
            if (enabledSteps[subKey]) updateStatus(subKey, "En cours...");
          }
        } else {
          updateStatus(action.key, "En cours...");
        }

        const result = await apiCall(action.endpoint, action.params);

        // Mettre à jour le statut selon l'étape
        if (action.key === "orgunits") {
          updateStatus(action.key, `Succès : ${result.synced} orgunits`);
        } else if (action.key === "data") {
          if (dataStepParams.teis) updateStatus("teis", `Succès : ${result.teis} TEIs`);
          if (dataStepParams.enrollments) updateStatus("enrollments", `Succès : ${result.enrollments} enrollments`);
          if (dataStepParams.events) updateStatus("events", `Succès : ${result.events} events`);
          if (dataStepParams.attributes) updateStatus("attributes", `Succès : ${result.attributes} attributes`);
        } else if (action.key === "dataElements") {
          updateStatus(action.key, `Succès : ${result.synced} dataElements`);
        } else if (action.key === "matview") {
          updateStatus(action.key, `Succès : ${result.matview} Matview build`);
        } else if (action.key === "refresh_matview") {
          updateStatus(action.key, `Succès : ${result.matview} Matview Rafraichie`);
        }
        
      }

      setSuccess("Mis à jour terminée avec succès !");
    } catch (error) {
      setErr(`Erreur : ${error}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col items-center justify-center bg-gray-100 p-10">
      <div className="w-full max-w-lg bg-white p-6 rounded-2xl shadow">
        <h2 className="text-2xl font-bold mb-4 text-center">Mis à jour des utilitaires</h2>

        {err && <div className="mb-3 p-2 bg-red-100 text-red-700 rounded">{err}</div>}
        {success && <div className="mb-3 p-2 bg-green-100 text-green-700 rounded">{success}</div>}

        <div className="mb-4 space-y-2">
          <h3 className="font-semibold">Sélectionner les modules à mettre à jour :</h3>

          {STEPS.map((step) => (
            <label key={step.key} className="flex items-center space-x-2">
              <input type="checkbox" checked={enabledSteps[step.key]} onChange={() => toggleStep(step.key)} />
              <span>{step.label}</span>
            </label>
          ))}
        </div>

        <button
          onClick={handleSync}
          disabled={loading}
          className={`w-full py-2 rounded text-white ${loading ? "bg-gray-500" : "bg-blue-600 hover:bg-blue-700"}`}
        >
          {loading ? "Mis à jour en cours..." : "Mettre à jour maintenant"}
        </button>

        <div className="mt-5 space-y-2 text-sm">
          {Object.entries(status).map(([key, value]) => (
            <div key={key}>
              <strong>{key.toUpperCase()} :</strong> {value}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
