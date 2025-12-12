// frontend/src/pages/Register.jsx

import React, { useState, useEffect } from "react";
import { PencilSquareIcon, TrashIcon, KeyIcon, PlusIcon } from "@heroicons/react/24/solid";
import { useAuth } from "../contexts/AuthContext";
import "./Register.css";

export default function Register() {
    const [users, setUsers] = useState([]);
    const [loading, setLoading] = useState(true);
    const [err, setErr] = useState("");
    const [success, setSuccess] = useState("");

    // MODALS
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [showEditModal, setShowEditModal] = useState(false);

    // SELECTED USER
    const [selectedUser, setSelectedUser] = useState(null);
    
    // EDIT DATA
    const [editFullname, setEditFullname] = useState("");
    const [editRole, setEditRole] = useState("user");

    // CREATE DATA
    const [newFullname, setNewFullname] = useState("");
    const [newUsername, setNewUsername] = useState("");
    const [newRole, setNewRole] = useState("user");

    // FOR PASSWORD
    const [newPassword, setNewPassword] = useState("");
    const [confirmPassword, setConfirmPassword] = useState("");

    const [showPwdModal, setShowPwdModal] = useState(false);


    const { register, getUsers, updateUser, adminUpdateUserPassword, deleteUser, user, isAdmin, isSuperAdmin } = useAuth();

    // ---------------- FETCH USERS ----------------
    const fetchUsers = async () => {
        try {
            setLoading(true);
            const data = await getUsers();
            setUsers(data);
        } catch (error) {
            setErr("Erreur lors du chargement des utilisateurs");
        } finally {
            setLoading(false);
        }
    };

    // assume fetchUsers is defined (voir ton code précédent)
    useEffect(() => {
        if (!user) return;      // attendre chargement

        if (isSuperAdmin || isAdmin) {
            fetchUsers();
        } else {
            setErr("Only admin can view users");
        }
    }, [user, isAdmin]);


    // ---------------- CREATE USER ----------------
    const handleCreateUser = async (e) => {
        e.preventDefault();
        try {
            if(newPassword !== confirmPassword){
                setErr("Les mots de passes ne concordent pas!");
                return;
            }
            const payload = {
                fullname: newFullname,
                username: newUsername,
                password: newPassword,
                role: newRole,
            };
            await register(payload);

            setSuccess("✔ Utilisateur créé avec succès !");
            setShowCreateModal(false);
            fetchUsers();

            // reset
            setNewFullname("");
            setNewUsername("");
            setNewPassword("");
            setConfirmPassword("");
            setNewRole("user");

        } catch (err) {
            setErr(err.message || "Erreur lors de la création");
        }
    };

    // ---------------- UPDATE USER ----------------
    const openEditModal = (user) => {
        setSelectedUser(user);
        setEditFullname(user.fullname);
        setEditRole(user.role);
        setNewPassword("");
        setConfirmPassword("");
        setShowEditModal(true);
    };

    const openPasswordModal = (user) => {
        setSelectedUser(user);
        setNewPassword("");
        setConfirmPassword("");
        setShowPwdModal(true);
    };

    const saveNewPassword = async () => {
        try {
            if(newPassword !== confirmPassword){
                setErr("Les mots de passes ne concordent pas!");
                return;
            }
            await adminUpdateUserPassword(selectedUser.id, newPassword);
            setShowPwdModal(false);
            fetchUsers();
        } catch (error) {
            setErr(error.response?.data?.error || error.message);
        }
    };


    const handleUpdateUser = async (e) => {
        e.preventDefault();

        if (!selectedUser) return;
        if (!window.confirm("Confirmer la modification de cet utilisateur ?")) return;

        try {
            if(newPassword.length > 0 && newPassword !== confirmPassword){
                setErr("Les mots de passes ne concordent pas!");
                return;
            }
            const payload = {
                fullname: editFullname,
                role: editRole,
                ...(newPassword.length > 0 ? { password: newPassword } : {}), // change only if provided
            };

            await updateUser(selectedUser.id, payload);

            setSuccess("✔ Utilisateur modifié !");
            setShowEditModal(false);
            fetchUsers();
        } catch (err) {
            setErr(err.message || "Erreur lors de la modification");
        }
    };

    // ---------------- DELETE USER ----------------
    const handleDelete = async (user) => {
        if (user.username === "admin") {
            return setErr("⚠ Impossible de supprimer l’utilisateur admin.");
        }

        if (!window.confirm("Confirmer la suppression ?")) return;

        try {
            await deleteUser(user.id);
            fetchUsers();
        } catch (err) {
            setErr(err.message || "Erreur lors de la suppression");
        }
    };

    return (
        <div className="reg-container">
            <div className="reg-header">
                <h1>👤 Gestion des utilisateurs</h1>
                {isAdmin && (
                    <button className="btn-primary" onClick={() => setShowCreateModal(true)}>
                        <PlusIcon className="icon" /> Ajouter un utilisateur
                    </button>
                )}
            </div>

            {/* STATUS MESSAGES */}
            {err && <div className="alert error">{err}</div>}
            {success && <div className="alert success">{success}</div>}

            {/* TABLE */}
            <div className="reg-table-card">
                {loading ? (
                    <p>Chargement...</p>
                ) : (
                    <table className="reg-table">
                        <thead>
                            <tr>
                                <th>Nom complet</th>
                                <th>Nom d’utilisateur</th>
                                <th>Rôle</th>
                                <th className="text-center">Actions</th>
                            </tr>
                        </thead>

                        <tbody>
                            {users.map((u) => (
                                <tr key={u.id}>
                                    <td>{u.fullname}</td>
                                    <td>{u.username}</td>
                                    <td>{u.role}</td>

                                    <td className="action-cell">
                                        {/* EDIT */}
                                        {isAdmin && u.role !== "superadmin" ? (
                                            <button
                                                className="icon-btn"
                                                onClick={() => openEditModal(u)}
                                            >
                                                <PencilSquareIcon className="icon text-blue-500" />
                                            </button>
                                        ) : (<span></span>)}

                                        {/* DELETE */}
                                        {isAdmin && u.role !== "superadmin" ? (
                                            <button
                                                className="icon-btn"
                                                onClick={() => handleDelete(u)}
                                            >
                                                <TrashIcon className="icon text-red-500" />
                                            </button>
                                        ) : (<span></span>)}

                                        {/* EDIT PASSWORD*/}
                                        {isSuperAdmin && u.role === "superadmin" ? (
                                            <button
                                                className={`icon-btn ${u.username === "admin" ? "disabled" : ""}`}
                                                onClick={() => openPasswordModal(u)}
                                            >
                                                <PencilSquareIcon className="icon text-orange-500" />
                                            </button>
                                        ) : (<span></span>)}
                                    </td>
                                </tr>
                            )
                            )}
                        </tbody>
                    </table>
                )}
            </div>

            {/* ---------------- CREATE MODAL ---------------- */}
            {showCreateModal && (
                <div className="modal-overlay">
                    <div className="modal">
                        <h2>Créer un utilisateur</h2>

                        <form onSubmit={handleCreateUser}>
                            <label>Nom complet</label>
                            <input value={newFullname} onChange={(e) => setNewFullname(e.target.value)} required />

                            <label>Nom d’utilisateur</label>
                            <input value={newUsername} onChange={(e) => setNewUsername(e.target.value)} required />

                            <label>Mot de passe utilisateur</label>
                            <input type="password" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} required />

                            <label>Confirmer Mot de passe</label>
                            <input type="password" value={confirmPassword} onChange={(e) => setConfirmPassword(e.target.value)} required />

                            <label>Rôle</label>
                            <select value={newRole} onChange={(e) => setNewRole(e.target.value)}>
                                <option value="user">Utilisateur</option>
                                <option value="admin">Administrateur</option>
                            </select>

                            <div className="modal-actions">
                                <button type="button" className="btn-secondary" onClick={() => setShowCreateModal(false)}>
                                    Annuler
                                </button>
                                <button type="submit" className="btn-primary">Créer</button>
                            </div>
                        </form>
                    </div>
                </div>
            )}

            {/* ---------------- EDIT MODAL ---------------- */}
            {showEditModal && (
                <div className="modal-overlay">
                    <div className="modal">
                        <h2>Modifier l’utilisateur {selectedUser.username}</h2>

                        <form onSubmit={handleUpdateUser}>
                            <label>Nom complet</label>
                            <input value={editFullname} onChange={(e) => setEditFullname(e.target.value)} required />

                            <label>Rôle</label>
                            <select value={editRole} onChange={(e) => setEditRole(e.target.value)}>
                                <option value="user">Utilisateur</option>
                                <option value="admin">Administrateur</option>
                            </select>

                            <label>Nouveau mot de passe (optionnel)</label>
                            <input type="password" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} />

                            <label>COnfirmer mot de passe (optionnel)</label>
                            <input type="password" value={confirmPassword} onChange={(e) => setConfirmPassword(e.target.value)} />

                            <div className="modal-actions">
                                <button type="button" className="btn-secondary" onClick={() => setShowEditModal(false)}>
                                    Annuler
                                </button>
                                <button type="submit" className="btn-primary">Enregistrer</button>
                            </div>
                        </form>
                    </div>
                </div>
            )}

            {showPwdModal && (
                <div className="fixed inset-0 bg-black bg-opacity-40 flex justify-center items-center">
                    <div className="bg-white p-6 rounded-xl shadow-xl w-96">
                        <h3 className="text-lg font-bold mb-4">
                            Modifier le mot de passe de {selectedUser.username}
                        </h3>

                        <label className="block mb-3">
                            <span className="text-sm">Nouveau mot de passe</span>
                            <input
                                type="password"
                                className="w-full border rounded px-3 py-2 mt-1"
                                value={newPassword}
                                onChange={(e) => setNewPassword(e.target.value)}
                            />
                        </label>

                        <label className="block mb-3">
                            <span className="text-sm">Confirmer Nouveau mot de passe</span>
                            <input
                                type="password"
                                className="w-full border rounded px-3 py-2 mt-1"
                                value={confirmPassword}
                                onChange={(e) => setConfirmPassword(e.target.value)}
                            />
                        </label>


                        <div className="flex justify-end mt-4">
                            <button
                                onClick={() => setShowPwdModal(false)}
                                className="px-4 py-2 bg-gray-300 rounded mr-2 hover:bg-gray-400"
                            >
                                Annuler
                            </button>

                            <button
                                onClick={saveNewPassword}
                                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
                            >
                                Enregistrer
                            </button>
                        </div>
                    </div>
                </div>
            )}

        </div>
    );
}
