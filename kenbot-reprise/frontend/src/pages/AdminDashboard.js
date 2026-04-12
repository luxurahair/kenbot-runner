import React, { useState, useEffect, useCallback } from 'react';
import { API } from '../App';

const STATUTS = ['NOUVEAU', 'EN ÉVALUATION', 'OFFRE ENVOYÉE', 'ACCEPTÉ', 'REFUSÉ'];
const STATUT_COLORS = {
  'NOUVEAU': { bg: '#1e3a5f', color: '#93c5fd' },
  'EN ÉVALUATION': { bg: '#713f12', color: '#fef08a' },
  'OFFRE ENVOYÉE': { bg: '#3b0764', color: '#d8b4fe' },
  'ACCEPTÉ': { bg: '#065f46', color: '#6ee7b7' },
  'REFUSÉ': { bg: '#7f1d1d', color: '#fca5a5' },
};

export default function AdminDashboard() {
  const [authenticated, setAuthenticated] = useState(false);
  const [phone, setPhone] = useState('');
  const [password, setPassword] = useState('');
  const [loginError, setLoginError] = useState('');
  const [evaluations, setEvaluations] = useState([]);
  const [selectedEval, setSelectedEval] = useState(null);
  const [filter, setFilter] = useState('all');
  const [loading, setLoading] = useState(false);

  const fetchEvaluations = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API}/api/evaluations`);
      const data = await res.json();
      setEvaluations(data.evaluations || []);
    } catch (e) { console.error(e); }
    setLoading(false);
  }, []);

  useEffect(() => { if (authenticated) fetchEvaluations(); }, [authenticated, fetchEvaluations]);

  const handleLogin = async () => {
    setLoginError('');
    try {
      const res = await fetch(`${API}/api/auth/login`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phone, password }),
      });
      if (res.ok) { setAuthenticated(true); }
      else { setLoginError('Identifiants incorrects'); }
    } catch (e) { setLoginError('Erreur de connexion'); }
  };

  const updateStatus = async (evalId, newStatus) => {
    try {
      await fetch(`${API}/api/evaluations/${evalId}`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: newStatus }),
      });
      fetchEvaluations();
      if (selectedEval && selectedEval.id === evalId) {
        setSelectedEval(prev => ({ ...prev, status: newStatus }));
      }
    } catch (e) { console.error(e); }
  };

  const s = {
    page: { minHeight: '100vh', background: '#0a0a1a', color: '#e5e7eb' },
    login: { display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '100vh', padding: '1rem' },
    card: { background: '#1a1a2e', borderRadius: '12px', padding: '1.5rem', width: '100%', maxWidth: '400px' },
    input: { width: '100%', padding: '12px 14px', borderRadius: '8px', border: '1px solid #374151', background: '#111827', color: '#e5e7eb', fontSize: '1rem', marginBottom: '0.75rem' },
    btn: { width: '100%', padding: '14px', borderRadius: '8px', border: 'none', fontSize: '1rem', fontWeight: 700, cursor: 'pointer', background: '#22c55e', color: '#000' },
    header: { background: '#111827', borderBottom: '1px solid #1f2937', padding: '1rem 1.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
    badge: (status) => { const c = STATUT_COLORS[status] || { bg: '#374151', color: '#9ca3af' }; return { padding: '3px 10px', borderRadius: '12px', fontSize: '0.75rem', fontWeight: 700, background: c.bg, color: c.color }; },
  };

  // ── LOGIN ──
  if (!authenticated) {
    return (
      <div style={s.page}>
        <div style={s.login}>
          <div style={s.card}>
            <div style={{ textAlign: 'center', marginBottom: '1.5rem' }}>
              <div style={{ fontSize: '1.5rem', fontWeight: 800, color: '#22c55e' }}>KENBOT REPRISE</div>
              <div style={{ color: '#6b7280', fontSize: '0.85rem' }}>Administration</div>
            </div>
            <input
              data-testid="admin-phone"
              style={s.input} placeholder="Téléphone" type="tel" value={phone}
              onChange={e => setPhone(e.target.value)} onKeyDown={e => e.key === 'Enter' && handleLogin()}
            />
            <input
              data-testid="admin-password"
              style={s.input} placeholder="Mot de passe" type="password" value={password}
              onChange={e => setPassword(e.target.value)} onKeyDown={e => e.key === 'Enter' && handleLogin()}
            />
            {loginError && <div style={{ color: '#ef4444', fontSize: '0.85rem', marginBottom: '0.5rem' }}>{loginError}</div>}
            <button data-testid="admin-login-btn" style={s.btn} onClick={handleLogin}>Connexion</button>
          </div>
        </div>
      </div>
    );
  }

  const filtered = evaluations.filter(e => filter === 'all' || e.status === filter);

  // ── DETAIL VIEW ──
  if (selectedEval) {
    const ev = selectedEval;
    return (
      <div style={s.page}>
        <div style={s.header}>
          <button
            data-testid="back-btn"
            onClick={() => setSelectedEval(null)}
            style={{ background: 'none', border: '1px solid #374151', borderRadius: '8px', color: '#9ca3af', padding: '6px 14px', cursor: 'pointer' }}
          >← Retour</button>
          <span style={s.badge(ev.status)}>{ev.status}</span>
        </div>
        <div style={{ padding: '1.5rem', maxWidth: '700px', margin: '0 auto' }}>
          {/* Véhicule */}
          <div style={{ ...s.card, marginBottom: '1rem' }}>
            <h3 style={{ color: '#22c55e', marginBottom: '1rem' }}>
              {ev.year} {ev.make} {ev.model} {ev.trim}
            </h3>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' }}>
              {[
                ['VIN', ev.vin],
                ['Moteur', ev.engine],
                ['Carburant', ev.fuel_type],
                ['Motricité', ev.drive_type],
                ['Km', ev.km ? `${Number(ev.km).toLocaleString('fr-CA')} km` : '—'],
                ['Paiement', ev.paiement_restant ? `${ev.paiement_restant} $/mois` : '—'],
                ['État', ev.etat_general],
              ].map(([l, v]) => v ? (
                <div key={l}>
                  <div style={{ color: '#6b7280', fontSize: '0.75rem' }}>{l}</div>
                  <div style={{ color: '#e5e7eb', fontSize: '0.9rem', fontWeight: 600 }}>{v}</div>
                </div>
              ) : null)}
            </div>
          </div>

          {/* Photos */}
          {ev.photos && ev.photos.length > 0 && (
            <div style={{ ...s.card, marginBottom: '1rem' }}>
              <h4 style={{ color: '#e5e7eb', marginBottom: '0.75rem' }}>📸 Photos ({ev.photos.length})</h4>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem' }}>
                {ev.photos.map((url, i) => (
                  <img key={i} src={url} alt={`Photo ${i + 1}`} style={{ width: '100%', aspectRatio: '1', objectFit: 'cover', borderRadius: '8px', border: '1px solid #374151', cursor: 'pointer' }}
                    onClick={() => window.open(url, '_blank')} />
                ))}
              </div>
            </div>
          )}

          {/* Client */}
          <div style={{ ...s.card, marginBottom: '1rem' }}>
            <h4 style={{ color: '#e5e7eb', marginBottom: '0.75rem' }}>👤 Client</h4>
            <div style={{ color: '#e5e7eb', fontWeight: 600 }}>{ev.client_name}</div>
            <div style={{ color: '#93c5fd' }}>📞 <a href={`tel:${ev.client_phone}`} style={{ color: '#93c5fd' }}>{ev.client_phone}</a></div>
            {ev.client_email && <div style={{ color: '#9ca3af' }}>✉️ {ev.client_email}</div>}
            {ev.client_notes && <div style={{ color: '#9ca3af', marginTop: '0.5rem', fontStyle: 'italic' }}>"{ev.client_notes}"</div>}
          </div>

          {/* Status update */}
          <div style={{ ...s.card }}>
            <h4 style={{ color: '#e5e7eb', marginBottom: '0.75rem' }}>Changer le statut</h4>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
              {STATUTS.map(st => (
                <button
                  key={st}
                  data-testid={`status-${st}`}
                  onClick={() => updateStatus(ev.id, st)}
                  style={{
                    ...s.badge(st), cursor: 'pointer', border: ev.status === st ? '2px solid #22c55e' : '2px solid transparent',
                    padding: '6px 14px', fontSize: '0.8rem',
                  }}
                >{st}</button>
              ))}
            </div>
          </div>

          <div style={{ color: '#6b7280', fontSize: '0.75rem', marginTop: '0.75rem', textAlign: 'center' }}>
            Reçu le {new Date(ev.created_at).toLocaleString('fr-CA')}
          </div>
        </div>
      </div>
    );
  }

  // ── LIST VIEW ──
  return (
    <div style={s.page}>
      <div style={s.header}>
        <div>
          <span style={{ fontSize: '1.2rem', fontWeight: 800, color: '#22c55e' }}>KENBOT REPRISE</span>
          <span style={{ color: '#6b7280', marginLeft: '0.75rem', fontSize: '0.85rem' }}>Administration</span>
        </div>
        <button onClick={fetchEvaluations} style={{ background: 'none', border: '1px solid #374151', borderRadius: '8px', color: '#9ca3af', padding: '6px 14px', cursor: 'pointer' }}>
          {loading ? '...' : '🔄 Rafraîchir'}
        </button>
      </div>

      {/* Stats */}
      <div style={{ display: 'flex', gap: '0.5rem', padding: '1rem 1.5rem', flexWrap: 'wrap' }}>
        {[
          { label: 'Tous', count: evaluations.length, id: 'all' },
          ...STATUTS.map(st => ({ label: st, count: evaluations.filter(e => e.status === st).length, id: st })),
        ].map(f => (
          <button
            key={f.id}
            data-testid={`filter-${f.id}`}
            onClick={() => setFilter(f.id)}
            style={{
              padding: '4px 12px', borderRadius: '16px', border: 'none', cursor: 'pointer', fontSize: '0.8rem', fontWeight: 600,
              background: filter === f.id ? '#3b82f6' : '#1f2937', color: filter === f.id ? '#fff' : '#9ca3af',
            }}
          >
            {f.label} ({f.count})
          </button>
        ))}
      </div>

      {/* List */}
      <div style={{ padding: '0 1.5rem' }}>
        {filtered.length === 0 && (
          <div style={{ textAlign: 'center', color: '#6b7280', padding: '3rem' }}>
            {evaluations.length === 0 ? 'Aucune évaluation reçue' : 'Aucune évaluation avec ce filtre'}
          </div>
        )}
        {filtered.map(ev => (
          <div
            key={ev.id}
            data-testid={`eval-${ev.id}`}
            onClick={() => setSelectedEval(ev)}
            style={{
              background: '#1a1a2e', borderRadius: '10px', padding: '1rem 1.25rem', marginBottom: '0.75rem', cursor: 'pointer',
              border: '1px solid #1f2937', transition: 'border-color 0.2s',
            }}
            onMouseEnter={e => e.currentTarget.style.borderColor = '#374151'}
            onMouseLeave={e => e.currentTarget.style.borderColor = '#1f2937'}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
              <div style={{ fontWeight: 700, color: '#e5e7eb' }}>
                {ev.year} {ev.make} {ev.model} {ev.trim}
              </div>
              <span style={s.badge(ev.status)}>{ev.status}</span>
            </div>
            <div style={{ display: 'flex', gap: '1.5rem', color: '#9ca3af', fontSize: '0.85rem' }}>
              <span>👤 {ev.client_name}</span>
              <span>📞 {ev.client_phone}</span>
              {ev.km && <span>📊 {Number(ev.km).toLocaleString('fr-CA')} km</span>}
              {ev.photos && ev.photos.length > 0 && <span>📸 {ev.photos.length} photos</span>}
            </div>
            <div style={{ color: '#6b7280', fontSize: '0.75rem', marginTop: '0.25rem' }}>
              {new Date(ev.created_at).toLocaleString('fr-CA')}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
