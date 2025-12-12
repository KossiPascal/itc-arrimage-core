import React, { useState, useEffect } from "react";
import api from "../utils/api";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import "./ArrimateData.css"; // Styles personnalisés

export default function ArrimateData() {
    const [startDate, setStartDate] = useState(new Date());
    const [endDate, setEndDate] = useState(new Date());
    const [orgUnits, setOrgUnits] = useState([]);
    const [selectedOrgUnits, setSelectedOrgUnits] = useState([]);
    const [selectAll, setSelectAll] = useState(false);
    const [status, setStatus] = useState("");
    const [statusType, setStatusType] = useState("");

    /** Load orgUnits */
    useEffect(() => {
        const loadOrgUnits = async () => {
            try {
                const res = await api.get("/fetch/orgunits");
                setOrgUnits(res.data || []);
            } catch (error) {
                setStatusType("error");
                setStatus("❌ Impossible de charger les OrgUnits.");
            }
        };
        loadOrgUnits();
    }, []);

    /** Handle select all checkbox */
    const handleSelectAll = () => {
        if (!selectAll) {
            const allIds = orgUnits.map((ou) => ou.id);
            setSelectedOrgUnits(allIds);
            setSelectAll(true);
        } else {
            setSelectedOrgUnits([]);
            setSelectAll(false);
        }
    };

    /** Handle individual selection */
    const handleOrgUnitChange = (e) => {
        const values = Array.from(e.target.selectedOptions, (opt) => opt.value);
        setSelectedOrgUnits(values);
        setSelectAll(values.length === orgUnits.length);
    };

    /** API POST wrapper */
    const apiCall = async (endpoint, params = {}) => {
        const res = await api.post(endpoint, params).catch((error) => {
            throw error.response?.data?.error || error.message;
        });
        return res.data;
    };

    /** Handle synchronization */
    const handleSync = async () => {
        if (!orgUnits || orgUnits.length === 0) {
            setStatusType("warning");
            setStatus("⚠️ Veuillez synchroniser les OrgUnits ou contacter votre administrateur.");
            return;
        }

        if (!startDate || !endDate) {
            setStatusType("warning");
            setStatus("⚠️ Veuillez renseigner les dates.");
            return;
        }

        if (!selectedOrgUnits || selectedOrgUnits.length === 0) {
            const orgunit_ids = orgUnits.map((ou) => ou.id);
            setSelectedOrgUnits(orgunit_ids);
        }

        setStatusType("loading");
        setStatus("⏳ Synchronisation en cours...");

        try {
            const params = {
                start_date: startDate.toISOString().split("T")[0],
                end_date: endDate.toISOString().split("T")[0],
                orgunits: selectedOrgUnits,
            };

            const res = await apiCall(`/arrimate-indicators`, params);

            if(res.status == 200){
                setStatusType("success");
                setStatus(`🎉 ${res.success}, ${res.error}`);
            } else if(res.status == 201){
                setStatusType("warning");
                setStatus(`⚠️ ${res.success}, ${res.error}`);
            } else {
                setStatusType("success");
                setStatus(`❌ ${res.error}`);
            }
        } catch (err) {
            setStatusType("error");
            setStatus(`❌ Erreur: ${err}`);
        }

        setTimeout(() => {
            setStatus("");
            setStatusType("");
        }, 60*1000*5);
    };

    /** Alert styles */
    const alertStyles = {
        success: "bg-green-50 border border-green-300 text-green-700 animate-fadeIn",
        error: "bg-red-50 border border-red-300 text-red-700 animate-fadeIn",
        warning: "bg-yellow-100 border border-yellow-400 text-yellow-800 animate-fadeIn",
        loading: "bg-blue-50 border border-blue-300 text-blue-700 animate-pulse",
    };

    return (
        <div className="flex flex-col items-center justify-center bg-gradient-to-br from-gray-100 to-gray-200 p-20">
            <div className="w-full max-w-2xl bg-white p-8 rounded-3xl shadow-xl border border-gray-200">

                <h1 className="text-2xl font-bold mb-6 text-gray-800 text-center">
                    📊 Synchronisation des Données Agrégées vers DHIS2
                </h1>

                {/* Dates */}
                <div className="flex flex-col md:flex-row md:space-x-4 mb-4">
                    <div className="mb-4 w-full">
                        <label className="block text-gray-700 font-medium mb-1">Start Date</label>
                        <DatePicker
                            required
                            selected={startDate}
                            onChange={setStartDate}
                            className="w-full border rounded-lg px-4 py-2 shadow-sm focus:ring-2 focus:ring-blue-500 focus:outline-none"
                        />
                    </div>

                    <div className="mb-4 w-full">
                        <label className="block text-gray-700 font-medium mb-1">End Date</label>
                        <DatePicker
                            required
                            selected={endDate}
                            onChange={setEndDate}
                            className="w-full border rounded-lg px-4 py-2 shadow-sm focus:ring-2 focus:ring-blue-500 focus:outline-none"
                        />
                    </div>
                </div>

                {/* OrgUnit */}
                <div className="mb-4">
                    <div className="flex items-center justify-between mb-1">
                        <label className="block text-gray-700 font-medium">Sélectionner les OrgUnits</label>
                        <label className="flex items-center space-x-2 cursor-pointer select-none">
                            <input
                                type="checkbox"
                                checked={selectAll}
                                onChange={handleSelectAll}
                                className="w-4 h-4 accent-blue-600"
                            />
                            <span className="text-sm text-gray-600 font-medium">Tout sélectionner</span>
                        </label>
                    </div>

                    <select
                        multiple
                        value={selectedOrgUnits}
                        onChange={handleOrgUnitChange}
                        className="w-full h-40 border rounded-lg px-4 py-2 shadow-sm focus:ring-2 focus:ring-blue-500 focus:outline-none bg-white"
                    >
                        {orgUnits.length === 0 && <option disabled>Chargement...</option>}
                        {orgUnits.map((ou) => (
                            <option key={ou.id} value={ou.id}>{ou.name}</option>
                        ))}
                    </select>

                    <p className="text-sm text-gray-500 mt-1">
                        Maintenez CTRL (Windows) ou CMD (Mac) pour sélectionner plusieurs éléments.
                    </p>
                </div>

                {/* Sync button */}
                <button
                    onClick={handleSync}
                    className="w-full bg-blue-600 hover:bg-blue-700 transition text-white font-semibold py-3 rounded-xl shadow-md active:scale-95 mt-2">
                    🚀 Lancer la Synchronisation
                </button>

                {/* Status Box */}
                {status && (
                    <div className={`mt-6 text-center font-semibold p-4 rounded-xl border transition-all duration-300 ${alertStyles[statusType]}`}>
                        {status}
                    </div>
                )}
            </div>
        </div>
    );
}
