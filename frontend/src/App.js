import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import PublicForm from './pages/PublicForm';
import AdminDashboard from './pages/AdminDashboard';

const API = process.env.REACT_APP_BACKEND_URL;

export { API };

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Navigate to="/evaluer" replace />} />
        <Route path="/evaluer" element={<PublicForm />} />
        <Route path="/admin" element={<AdminDashboard />} />
        <Route path="/admin/*" element={<AdminDashboard />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
