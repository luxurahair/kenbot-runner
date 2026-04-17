import React, { useState, useEffect, useCallback } from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import './App.css';

const API = process.env.REACT_APP_BACKEND_URL;

// ═══ PHOTO LIGHTBOX / SLIDER ═══
function PhotoSlider({ photos, startIndex, onClose }) {
  const [idx, setIdx] = useState(startIndex || 0);
  useEffect(() => {
    const handler = (e) => {
      if (e.key === 'Escape') onClose();
      if (e.key === 'ArrowRight') setIdx(i => Math.min(i + 1, photos.length - 1));
      if (e.key === 'ArrowLeft') setIdx(i => Math.max(i - 1, 0));
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [photos.length, onClose]);

  if (!photos?.length) return null;
  return (
    <div className="lightbox-overlay" onClick={onClose} data-testid="photo-slider">
      <div className="lightbox-inner" onClick={e => e.stopPropagation()}>
        <button className="lightbox-close" onClick={onClose}>&times;</button>
        <div className="lightbox-main">
          {idx > 0 && <button className="lightbox-nav lightbox-prev" onClick={() => setIdx(i => i - 1)}>&#8249;</button>}
          <img src={photos[idx]} alt={`Photo ${idx + 1}`} className="lightbox-img" />
          {idx < photos.length - 1 && <button className="lightbox-nav lightbox-next" onClick={() => setIdx(i => i + 1)}>&#8250;</button>}
        </div>
        <div className="lightbox-counter">{idx + 1} / {photos.length}</div>
        <div className="lightbox-thumbs">
          {photos.map((p, i) => <img key={i} src={p} alt="" className={`lightbox-thumb ${i === idx ? 'active' : ''}`} onClick={() => setIdx(i)} />)}
        </div>
      </div>
    </div>
  );
}

// ═══ WHOLESALE PANEL ═══
function WholesalePanel({ evaluation, onClose }) {
  const [contacts, setContacts] = useState([]);
  const [newEmail, setNewEmail] = useState('');
  const [newName, setNewName] = useState('');
  const [newPhone, setNewPhone] = useState('');
  const [sending, setSending] = useState(false);
  const [sent, setSent] = useState([]);

  useEffect(() => {
    fetch(`${API}/api/wholesale-contacts`).then(r => r.json()).then(d => setContacts(d.contacts || [])).catch(() => {});
  }, []);

  const addContact = async () => {
    if (!newEmail && !newPhone) return;
    try {
      const r = await fetch(`${API}/api/wholesale-contacts`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: newName, email: newEmail, phone: newPhone }) });
      if (r.ok) { setNewEmail(''); setNewName(''); setNewPhone(''); const d = await fetch(`${API}/api/wholesale-contacts`).then(r2 => r2.json()); setContacts(d.contacts || []); }
    } catch (e) { console.error(e); }
  };

  const deleteContact = async (id) => {
    await fetch(`${API}/api/wholesale-contacts/${id}`, { method: 'DELETE' });
    setContacts(c => c.filter(x => x.id !== id));
  };

  const sendToContact = async (contact) => {
    setSending(true);
    try {
      await fetch(`${API}/api/wholesale/send`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ evaluation_id: evaluation.id, contact_email: contact.email, contact_name: contact.name }) });
      setSent(s => [...s, contact.id || contact.email]);
    } catch (e) { console.error(e); }
    setSending(false);
  };

  const sendToAll = async () => {
    setSending(true);
    for (const c of contacts.filter(c => c.email)) {
      await sendToContact(c);
    }
    setSending(false);
  };

  const ev = evaluation;
  const ws = {
    overlay: { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', zIndex: 1000, display: 'flex', justifyContent: 'center', alignItems: 'flex-start', padding: '2rem', overflowY: 'auto', backdropFilter: 'blur(4px)' },
    panel: { background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '10px', width: '100%', maxWidth: '600px', padding: '1.5rem' },
    title: { fontFamily: 'Chivo', fontWeight: 700, fontSize: '1.1rem', marginBottom: '1rem' },
    row: { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr auto', gap: '0.4rem', marginBottom: '0.5rem' },
    input: { padding: '8px 10px', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.8rem', width: '100%' },
    contact: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.6rem 0', borderBottom: '1px solid var(--border)' },
  };

  return (
    <div style={ws.overlay} onClick={onClose} data-testid="wholesale-panel">
      <div style={ws.panel} onClick={e => e.stopPropagation()}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
          <div style={ws.title}>Envoyer aux grossistes</div>
          <button className="eval-btn-ghost" onClick={onClose}>&times;</button>
        </div>
        <div style={{ background: 'var(--surface-secondary)', padding: '0.75rem', borderRadius: '6px', marginBottom: '1rem', fontSize: '0.8rem' }}>
          <strong>{ev.year} {ev.make} {ev.model} {ev.trim}</strong> — {ev.km ? `${Number(ev.km).toLocaleString('fr-CA')} km` : ''} — {ev.etat_general}
        </div>

        <div style={{ marginBottom: '1rem' }}>
          <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: '0.5rem', textTransform: 'uppercase' }}>Ajouter un contact</div>
          <div style={ws.row}>
            <input style={ws.input} placeholder="Nom" value={newName} onChange={e => setNewName(e.target.value)} />
            <input style={ws.input} placeholder="Email" type="email" value={newEmail} onChange={e => setNewEmail(e.target.value)} />
            <input style={ws.input} placeholder="Telephone" value={newPhone} onChange={e => setNewPhone(e.target.value)} />
            <button className="eval-btn-primary" onClick={addContact} style={{ whiteSpace: 'nowrap' }}>+</button>
          </div>
        </div>

        <div style={{ marginBottom: '1rem' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
            <span style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase' }}>Contacts ({contacts.length})</span>
            {contacts.length > 0 && <button className="eval-btn-primary" onClick={sendToAll} disabled={sending}>{sending ? '...' : 'Envoyer a tous'}</button>}
          </div>
          {contacts.length === 0 ? (
            <div style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '1rem', fontSize: '0.8rem' }}>Aucun contact. Ajoutez des grossistes ci-dessus.</div>
          ) : contacts.map((c, i) => (
            <div key={c.id || i} style={ws.contact}>
              <div>
                <div style={{ fontWeight: 600, fontSize: '0.85rem' }}>{c.name || 'Sans nom'}</div>
                <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>{c.email} {c.phone ? `| ${c.phone}` : ''}</div>
              </div>
              <div style={{ display: 'flex', gap: '0.3rem' }}>
                {sent.includes(c.id || c.email) ? (
                  <span style={{ color: 'var(--accent-green)', fontSize: '0.75rem', fontWeight: 700 }}>Envoye</span>
                ) : (
                  <button className="eval-btn-primary" onClick={() => sendToContact(c)} disabled={sending || !c.email} style={{ fontSize: '0.7rem', padding: '4px 10px' }}>Envoyer</button>
                )}
                <button className="eval-action-btn" onClick={() => deleteContact(c.id)} style={{ fontSize: '0.65rem', padding: '4px 6px' }}>&#128465;</button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// Role-based tab access
const ROLE_TABS = {
  admin: ['cockpit', 'compare', 'reprise', 'evaluations', 'utilisateurs', 'dashboard', 'inventory', 'posts', 'textpreview', 'events', 'architecture', 'changelog'],
  directeur: ['evaluations', 'reprise', 'inventory'],
  conseiller: ['reprise', 'evaluations'],
};

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/reprise" element={<StandaloneReprise />} />
        <Route path="/*" element={<DashboardApp />} />
      </Routes>
    </BrowserRouter>
  );
}

// ═══════════════════════════════════════════════════
// STANDALONE REPRISE PAGE — for clients
// ═══════════════════════════════════════════════════
function StandaloneReprise() {
  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg)' }}>
      <header style={{ background: '#09090b', borderBottom: '1px solid #27272a', padding: '1rem 1.5rem', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '0.75rem' }}>
        <img src="/kennebec-logo.png" alt="Kennebec" style={{ height: '32px', objectFit: 'contain' }} />
      </header>
      <div style={{ textAlign: 'center', padding: '1.5rem 1rem 0' }}>
        <h1 style={{ fontFamily: 'Chivo', fontWeight: 900, fontSize: '1.4rem', color: '#0ea5e9', margin: '0 0 0.25rem' }}>ON REPREND TOUT VOS ECHANGES!</h1>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem', margin: 0 }}>Obtenez votre evaluation gratuite en quelques minutes</p>
      </div>
      <div style={{ maxWidth: '800px', margin: '0 auto', padding: '1.5rem' }}>
        <RepriseTab standalone />
      </div>
      <div style={{ textAlign: 'center', padding: '2rem 1rem', color: '#52525b', fontSize: '0.75rem' }}>
        Kennebec Dodge Chrysler — 10240 boul. Lacroix, Saint-Georges — 418-222-3939
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════
// DASHBOARD LOGIN
// ═══════════════════════════════════════════════════
function LoginPage({ onLogin }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [showPwd, setShowPwd] = useState(false);
  const [forgotMode, setForgotMode] = useState(false);
  const [forgotUser, setForgotUser] = useState('');
  const [forgotMsg, setForgotMsg] = useState('');
  const [forgotLoading, setForgotLoading] = useState(false);

  const handleLogin = async () => {
    setError(''); setLoading(true);
    try {
      const res = await fetch(`${API}/api/reprise/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      });
      if (res.ok) {
        const data = await res.json();
        onLogin({ name: data.name, role: data.role, username: data.username });
      } else {
        setError('Identifiants incorrects');
      }
    } catch { setError('Erreur de connexion'); }
    setLoading(false);
  };

  const handleForgot = async () => {
    if (!forgotUser.trim()) { setForgotMsg('Entrez votre nom d\'utilisateur'); return; }
    setForgotLoading(true); setForgotMsg('');
    try {
      const r = await fetch(`${API}/api/users/forgot-password`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: forgotUser.trim() }) });
      const d = await r.json();
      setForgotMsg(d.message || 'Courriel envoye.');
    } catch { setForgotMsg('Erreur reseau'); }
    setForgotLoading(false);
  };

  const inputStyle = { width: '100%', padding: '12px 14px', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.9rem', fontFamily: 'IBM Plex Sans' };

  return (
    <div style={{ minHeight: '100vh', background: 'var(--bg)', display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '1rem' }}>
      <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '8px', padding: '2rem', width: '100%', maxWidth: '360px' }} data-testid="login-form">
        <div style={{ textAlign: 'center', marginBottom: '2rem' }}>
          <div style={{ fontFamily: 'Chivo', fontWeight: 900, fontSize: '1.25rem', color: '#0ea5e9', letterSpacing: '0.15em' }}>KENBOT</div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>{forgotMode ? 'Reinitialiser le mot de passe' : 'Connexion au tableau de bord'}</div>
        </div>

        {forgotMode ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
            <input data-testid="forgot-username" style={inputStyle} placeholder="Nom d'utilisateur" value={forgotUser} onChange={e => setForgotUser(e.target.value)} onKeyDown={e => e.key === 'Enter' && handleForgot()} />
            {forgotMsg && <div style={{ fontSize: '0.8rem', color: forgotMsg.includes('envoye') ? '#22c55e' : forgotMsg.includes('Aucun') ? '#eab308' : 'var(--text-secondary)', fontWeight: 600 }}>{forgotMsg}</div>}
            <button data-testid="forgot-btn" onClick={handleForgot} disabled={forgotLoading} style={{ width: '100%', padding: '12px', borderRadius: '6px', border: 'none', background: '#0ea5e9', color: '#fff', fontWeight: 700, fontSize: '0.9rem', cursor: 'pointer' }}>
              {forgotLoading ? '...' : 'Envoyer le mot de passe'}
            </button>
            <button onClick={() => { setForgotMode(false); setForgotMsg(''); }} style={{ background: 'none', border: 'none', color: '#0ea5e9', cursor: 'pointer', fontSize: '0.8rem', fontWeight: 600 }}>
              Retour a la connexion
            </button>
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
            <input data-testid="login-username" style={inputStyle} placeholder="Nom d'utilisateur" value={username} onChange={e => setUsername(e.target.value)} onKeyDown={e => e.key === 'Enter' && handleLogin()} />
            <div style={{ position: 'relative' }}>
              <input data-testid="login-password" type={showPwd ? 'text' : 'password'} style={{ ...inputStyle, paddingRight: '42px' }} placeholder="Mot de passe" value={password} onChange={e => setPassword(e.target.value)} onKeyDown={e => e.key === 'Enter' && handleLogin()} />
              <button onClick={() => setShowPwd(!showPwd)} type="button" style={{ position: 'absolute', right: '8px', top: '50%', transform: 'translateY(-50%)', background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-secondary)', fontSize: '0.8rem', padding: '4px' }} data-testid="toggle-pwd">{showPwd ? 'Cacher' : 'Voir'}</button>
            </div>
            {error && <div style={{ color: 'var(--accent-red)', fontSize: '0.8rem' }} data-testid="login-error">{error}</div>}
            <button data-testid="login-btn" onClick={handleLogin} disabled={loading} style={{ width: '100%', padding: '12px', borderRadius: '6px', border: 'none', background: '#0ea5e9', color: '#fff', fontWeight: 700, fontSize: '0.9rem', cursor: 'pointer', fontFamily: 'IBM Plex Sans' }}>
              {loading ? '...' : 'Connexion'}
            </button>
            <button onClick={() => setForgotMode(true)} style={{ background: 'none', border: 'none', color: '#0ea5e9', cursor: 'pointer', fontSize: '0.8rem', fontWeight: 600 }} data-testid="forgot-link">
              Mot de passe oublie?
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════
// DASHBOARD APP (with auth)
// ═══════════════════════════════════════════════════
function DashboardApp() {
  const [user, setUser] = useState(() => {
    try { return JSON.parse(sessionStorage.getItem('kenbot_user')); } catch { return null; }
  });

  const handleLogin = (u) => {
    setUser(u);
    sessionStorage.setItem('kenbot_user', JSON.stringify(u));
  };
  const handleLogout = () => {
    setUser(null);
    sessionStorage.removeItem('kenbot_user');
  };

  if (!user) return <LoginPage onLogin={handleLogin} />;

  const allowedTabs = ROLE_TABS[user.role] || ROLE_TABS.conseiller;
  return <Dashboard user={user} allowedTabs={allowedTabs} onLogout={handleLogout} />;
}

function Dashboard({ user, allowedTabs, onLogout }) {
  const defaultTab = allowedTabs[0] || 'evaluations';
  const [tab, setTab] = useState(defaultTab);
  const [status, setStatus] = useState(null);
  const [inventory, setInventory] = useState([]);
  const [posts, setPosts] = useState([]);
  const [events, setEvents] = useState([]);
  const [changelog, setChangelog] = useState([]);
  const [architecture, setArchitecture] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedStock, setSelectedStock] = useState(null);

  const fetchAll = useCallback(async () => {
    setLoading(true);
    try {
      const [statusRes, invRes, postsRes, eventsRes, changelogRes, archRes] = await Promise.all([
        fetch(`${API}/api/system/status`),
        fetch(`${API}/api/inventory`),
        fetch(`${API}/api/posts`),
        fetch(`${API}/api/events?limit=30`),
        fetch(`${API}/api/changelog`),
        fetch(`${API}/api/architecture`),
      ]);
      setStatus(await statusRes.json());
      setInventory(await invRes.json());
      setPosts(await postsRes.json());
      setEvents(await eventsRes.json());
      setChangelog(await changelogRes.json());
      setArchitecture(await archRes.json());
    } catch (e) {
      console.error('Fetch error:', e);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  return (
    <div>
      <Header tab={tab} setTab={setTab} status={status} allowedTabs={allowedTabs} user={user} onLogout={onLogout} />
      <div className="main-content">
        {loading && tab !== 'reprise' && tab !== 'evaluations' ? <LoadingState /> : (
          <>
            {tab === 'cockpit' && <CockpitTab inventory={inventory} status={status} />}
            {tab === 'compare' && <CompareTab />}
            {tab === 'reprise' && <RepriseTab user={user} />}
            {tab === 'evaluations' && <EvaluationsTab user={user} />}
            {tab === 'utilisateurs' && <UtilisateursTab />}
            {tab === 'dashboard' && <DashboardTab status={status} events={events} posts={posts} />}
            {tab === 'inventory' && <InventoryTab inventory={inventory} />}
            {tab === 'posts' && <PostsTab posts={posts} />}
            {tab === 'textpreview' && <TextPreviewTab inventory={inventory} onSelectStock={setSelectedStock} selectedStock={selectedStock} />}
            {tab === 'events' && <EventsTab events={events} />}
            {tab === 'architecture' && <ArchitectureTab architecture={architecture} />}
            {tab === 'changelog' && <ChangelogTab changelog={changelog} />}
          </>
        )}
      </div>
    </div>
  );
}

function Header({ tab, setTab, status, allowedTabs, user, onLogout }) {
  const [showRunPanel, setShowRunPanel] = useState(false);
  const [showSettings, setShowSettings] = useState(false); // avatar dropdown
  const [showSettingsModal, setShowSettingsModal] = useState(false); // full settings panel
  const [settingsTab, setSettingsTab] = useState('compte');
  // Password
  const [pwdOld, setPwdOld] = useState('');
  const [pwdNew, setPwdNew] = useState('');
  const [pwdConfirm, setPwdConfirm] = useState('');
  const [pwdMsg, setPwdMsg] = useState('');
  const [pwdLoading, setPwdLoading] = useState(false);
  const [showOld, setShowOld] = useState(false);
  const [showNew, setShowNew] = useState(false);
  // Email
  const [newEmail, setNewEmail] = useState('');
  const [emailMsg, setEmailMsg] = useState('');
  const [emailLoading, setEmailLoading] = useState(false);
  // Wholesale contacts (directeur+admin)
  const [wsContacts, setWsContacts] = useState([]);
  const [wsNew, setWsNew] = useState({ prenom: '', nom: '', entreprise: '', telephone: '', email: '' });
  const [wsEdit, setWsEdit] = useState(null);
  const [wsMsg, setWsMsg] = useState('');
  const [wsLoading, setWsLoading] = useState(false);
  // Users (admin)
  const [settingsUsers, setSettingsUsers] = useState([]);
  const [userEdit, setUserEdit] = useState(null);
  const [userMsg, setUserMsg] = useState('');

  const isAdmin = user?.role === 'admin';
  const isDirecteur = user?.role === 'directeur';
  const canManageWholesale = isAdmin || isDirecteur;
  const ROLE_LABELS = { admin: 'Administrateur', directeur: 'Directeur des ventes', conseiller: 'Conseiller' };

  const fetchWsContacts = async () => { try { const r = await fetch(`${API}/api/wholesale-contacts`); const d = await r.json(); setWsContacts(d.contacts || []); } catch {} };
  const fetchSettingsUsers = async () => { try { const r = await fetch(`${API}/api/users`); const d = await r.json(); setSettingsUsers(d.users || []); } catch {} };

  useEffect(() => { if (showSettingsModal) { if (canManageWholesale) fetchWsContacts(); if (isAdmin || isDirecteur) fetchSettingsUsers(); } }, [showSettingsModal]);

  const handleChangePassword = async () => {
    setPwdMsg('');
    if (!pwdOld || !pwdNew) { setPwdMsg('Remplir tous les champs'); return; }
    if (pwdNew !== pwdConfirm) { setPwdMsg('Les mots de passe ne correspondent pas'); return; }
    if (pwdNew.length < 6) { setPwdMsg('Minimum 6 caracteres'); return; }
    setPwdLoading(true);
    try {
      const r = await fetch(`${API}/api/users/change-password`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: user?.username, old_password: pwdOld, new_password: pwdNew }) });
      if (r.ok) { setPwdMsg('Mot de passe change!'); setPwdOld(''); setPwdNew(''); setPwdConfirm(''); }
      else { const d = await r.json(); setPwdMsg(d.detail || 'Erreur'); }
    } catch (e) { setPwdMsg('Erreur reseau'); }
    setPwdLoading(false);
  };

  const handleChangeEmail = async () => {
    setEmailMsg('');
    if (!newEmail.includes('@')) { setEmailMsg('Courriel invalide'); return; }
    setEmailLoading(true);
    try {
      const r = await fetch(`${API}/api/users/${user?.username}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ email: newEmail }) });
      if (r.ok) { setEmailMsg('Courriel mis a jour!'); }
      else { setEmailMsg('Erreur'); }
    } catch { setEmailMsg('Erreur reseau'); }
    setEmailLoading(false);
  };

  const handleAddWholesale = async () => {
    if (!wsNew.email || !wsNew.nom) { setWsMsg('Nom et email requis'); return; }
    setWsLoading(true);
    try {
      const r = await fetch(`${API}/api/wholesale-contacts`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: `${wsNew.prenom} ${wsNew.nom}`.trim(), company: wsNew.entreprise, phone: wsNew.telephone, email: wsNew.email }) });
      if (r.ok) { setWsNew({ prenom: '', nom: '', entreprise: '', telephone: '', email: '' }); setWsMsg('Grossiste ajoute!'); fetchWsContacts(); }
      else { setWsMsg('Erreur'); }
    } catch { setWsMsg('Erreur reseau'); }
    setWsLoading(false);
  };

  const handleEditWholesale = async () => {
    if (!wsEdit) return;
    setWsLoading(true);
    try {
      const r = await fetch(`${API}/api/wholesale-contacts/${wsEdit.id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: wsEdit.name, company: wsEdit.company, phone: wsEdit.phone, email: wsEdit.email, active: wsEdit.active }) });
      if (r.ok) { setWsEdit(null); setWsMsg('Modifie!'); fetchWsContacts(); }
      else { setWsMsg('Erreur'); }
    } catch { setWsMsg('Erreur reseau'); }
    setWsLoading(false);
  };

  const handleEditUser = async (u, changes) => {
    try {
      const body = {};
      if (changes.name) body.name = changes.name;
      if (changes.email) body.email = changes.email;
      if (changes.phone) body.phone = changes.phone;
      if (changes.role) body.role = changes.role;
      if (changes.password) body.password = changes.password;
      const r = await fetch(`${API}/api/users/${u.username}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      if (r.ok) { setUserMsg('Modifie!'); fetchSettingsUsers(); setUserEdit(null); }
      else { setUserMsg('Erreur'); }
    } catch { setUserMsg('Erreur reseau'); }
  };

  const ms = { input: { width: '100%', padding: '8px 10px', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.8rem', background: 'var(--bg)', fontFamily: 'IBM Plex Sans' }, label: { fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: '2px', display: 'block' }, row: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' } };

  const allTabs = [
    { id: 'cockpit', label: 'Cockpit' },
    { id: 'compare', label: 'Kennebec vs FB' },
    { id: 'reprise', label: 'Reprise' },
    { id: 'evaluations', label: 'Evaluations' },
    { id: 'utilisateurs', label: 'Utilisateurs' },
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'inventory', label: 'Inventaire' },
    { id: 'posts', label: 'Posts FB' },
    { id: 'textpreview', label: 'Preview Texte' },
    { id: 'events', label: 'Events' },
    { id: 'architecture', label: 'Architecture' },
    { id: 'changelog', label: 'Changelog' },
  ];
  const tabs = allTabs.filter(t => allowedTabs.includes(t.id));
  const connected = status?.supabase_connected;

  const settingsTabs = [{ id: 'compte', label: 'Mon compte' }];
  if (canManageWholesale) settingsTabs.push({ id: 'grossistes', label: 'Grossistes' });
  if (isAdmin || isDirecteur) settingsTabs.push({ id: 'equipe', label: 'Equipe' });

  return (
    <>
      <header className="header" data-testid="header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span className="header-logo" data-testid="header-logo">KENBOT</span>
          <span className={`status-dot ${connected ? '' : 'offline'}`} data-testid="status-dot" title={connected ? 'Supabase connecte' : 'Supabase deconnecte'} />
        </div>
        <nav className="header-nav" data-testid="header-nav">
          {tabs.map(t => (
            <button key={t.id} className={tab === t.id ? 'active' : ''} onClick={() => setTab(t.id)} data-testid={`nav-${t.id}`}>
              {t.label}
            </button>
          ))}
        </nav>
        <div className="header-right">
          {user?.role === 'admin' && (
            <button className="run-btn" onClick={() => setShowRunPanel(!showRunPanel)} data-testid="run-cron-btn">RUN CRON</button>
          )}
          <span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', fontFamily: 'IBM Plex Mono' }} data-testid="user-name">{user?.name}</span>
          <div style={{ position: 'relative' }}>
            <button onClick={() => setShowSettings(s => !s)} style={{ width: 32, height: 32, borderRadius: '50%', border: '2px solid #0ea5e9', background: '#0ea5e920', color: '#0ea5e9', fontWeight: 700, fontSize: '0.75rem', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: 'Chivo' }} data-testid="avatar-btn">{(user?.name || 'U').charAt(0).toUpperCase()}</button>
            {showSettings && <>
              <div style={{ position: 'fixed', inset: 0, zIndex: 1199 }} onClick={() => setShowSettings(false)} />
              <div style={{ position: 'absolute', right: 0, top: 40, background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '8px', padding: '0.5rem 0', minWidth: '220px', zIndex: 1200, boxShadow: '0 8px 24px rgba(0,0,0,0.5)' }} data-testid="avatar-dropdown">
                <div style={{ padding: '0.75rem 1rem', borderBottom: '1px solid var(--border)' }}>
                  <div style={{ fontWeight: 700, fontSize: '0.85rem' }}>{user?.name}</div>
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{user?.username} | {ROLE_LABELS[user?.role] || user?.role}</div>
                </div>
                <button onClick={() => { setShowSettings(false); setShowSettingsModal(true); setSettingsTab('compte'); }} style={{ width: '100%', padding: '0.6rem 1rem', background: 'none', border: 'none', textAlign: 'left', cursor: 'pointer', fontSize: '0.8rem', color: 'var(--text-primary, #eee)', display: 'flex', alignItems: 'center', gap: '0.5rem' }} onMouseEnter={e => e.target.style.background='var(--bg)'} onMouseLeave={e => e.target.style.background='none'}>Mon compte</button>
                {canManageWholesale && <button onClick={() => { setShowSettings(false); setShowSettingsModal(true); setSettingsTab('grossistes'); }} style={{ width: '100%', padding: '0.6rem 1rem', background: 'none', border: 'none', textAlign: 'left', cursor: 'pointer', fontSize: '0.8rem', color: 'var(--text-primary, #eee)', display: 'flex', alignItems: 'center', gap: '0.5rem' }} onMouseEnter={e => e.target.style.background='var(--bg)'} onMouseLeave={e => e.target.style.background='none'}>Grossistes</button>}
                {(isAdmin || isDirecteur) && <button onClick={() => { setShowSettings(false); setShowSettingsModal(true); setSettingsTab('equipe'); }} style={{ width: '100%', padding: '0.6rem 1rem', background: 'none', border: 'none', textAlign: 'left', cursor: 'pointer', fontSize: '0.8rem', color: 'var(--text-primary, #eee)', display: 'flex', alignItems: 'center', gap: '0.5rem' }} onMouseEnter={e => e.target.style.background='var(--bg)'} onMouseLeave={e => e.target.style.background='none'}>Equipe</button>}
                <div style={{ borderTop: '1px solid var(--border)', marginTop: '0.25rem' }} />
                <button onClick={onLogout} style={{ width: '100%', padding: '0.6rem 1rem', background: 'none', border: 'none', textAlign: 'left', cursor: 'pointer', fontSize: '0.8rem', color: '#ef4444', display: 'flex', alignItems: 'center', gap: '0.5rem' }} onMouseEnter={e => e.target.style.background='var(--bg)'} onMouseLeave={e => e.target.style.background='none'}>Deconnexion</button>
              </div>
            </>}
          </div>
          <span className="version-tag" data-testid="version-tag">v{status?.version || '2.2.0'}</span>
        </div>
      </header>
      {showRunPanel && <RunPanel onClose={() => setShowRunPanel(false)} />}

      {/* SETTINGS MODAL */}
      {showSettingsModal && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.75)', zIndex: 1100, display: 'flex', justifyContent: 'center', alignItems: 'flex-start', paddingTop: '3rem', backdropFilter: 'blur(4px)', overflowY: 'auto' }} onClick={() => setShowSettingsModal(false)}>
          <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '10px', padding: '1.5rem', width: '100%', maxWidth: '520px', marginBottom: '2rem' }} onClick={e => e.stopPropagation()}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
              <span style={{ fontFamily: 'Chivo', fontWeight: 700, fontSize: '1rem' }}>Parametres</span>
              <button onClick={() => setShowSettingsModal(false)} style={{ background: 'none', border: '1px solid var(--border)', padding: '4px 12px', cursor: 'pointer', fontSize: '0.7rem', borderRadius: '4px' }}>Fermer</button>
            </div>

            {/* Settings tabs */}
            <div style={{ display: 'flex', gap: '4px', marginBottom: '1rem', borderBottom: '1px solid var(--border)', paddingBottom: '0.5rem' }}>
              {settingsTabs.map(t => (
                <button key={t.id} onClick={() => setSettingsTab(t.id)} style={{ padding: '6px 12px', borderRadius: '4px 4px 0 0', border: 'none', background: settingsTab === t.id ? '#0ea5e920' : 'transparent', color: settingsTab === t.id ? '#0ea5e9' : 'var(--text-secondary)', fontWeight: settingsTab === t.id ? 700 : 400, cursor: 'pointer', fontSize: '0.75rem' }}>{t.label}</button>
              ))}
            </div>

            {/* TAB: Mon compte */}
            {settingsTab === 'compte' && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                <div style={{ padding: '0.75rem', background: 'var(--bg)', borderRadius: '6px', border: '1px solid var(--border)' }}>
                  <div style={{ fontSize: '0.75rem', fontWeight: 700, marginBottom: '0.5rem', color: '#0ea5e9' }}>Changer le mot de passe</div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                    <div style={{ position: 'relative' }}>
                      <input type={showOld ? 'text' : 'password'} placeholder="Mot de passe actuel" value={pwdOld} onChange={e => setPwdOld(e.target.value)} style={{ ...ms.input, paddingRight: '42px' }} />
                      <button type="button" onClick={() => setShowOld(!showOld)} style={{ position: 'absolute', right: '8px', top: '50%', transform: 'translateY(-50%)', background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-secondary)', fontSize: '0.7rem' }}>{showOld ? 'Cacher' : 'Voir'}</button>
                    </div>
                    <div style={{ position: 'relative' }}>
                      <input type={showNew ? 'text' : 'password'} placeholder="Nouveau mot de passe" value={pwdNew} onChange={e => setPwdNew(e.target.value)} style={{ ...ms.input, paddingRight: '42px' }} />
                      <button type="button" onClick={() => setShowNew(!showNew)} style={{ position: 'absolute', right: '8px', top: '50%', transform: 'translateY(-50%)', background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-secondary)', fontSize: '0.7rem' }}>{showNew ? 'Cacher' : 'Voir'}</button>
                    </div>
                    <input type={showNew ? 'text' : 'password'} placeholder="Confirmer" value={pwdConfirm} onChange={e => setPwdConfirm(e.target.value)} onKeyDown={e => e.key === 'Enter' && handleChangePassword()} style={ms.input} />
                    {pwdMsg && <div style={{ fontSize: '0.75rem', color: pwdMsg.includes('change') ? '#22c55e' : '#ef4444', fontWeight: 600 }}>{pwdMsg}</div>}
                    <button onClick={handleChangePassword} disabled={pwdLoading} className="eval-btn-primary" style={{ fontSize: '0.75rem' }}>{pwdLoading ? '...' : 'Changer le mot de passe'}</button>
                  </div>
                </div>
                <div style={{ padding: '0.75rem', background: 'var(--bg)', borderRadius: '6px', border: '1px solid var(--border)' }}>
                  <div style={{ fontSize: '0.75rem', fontWeight: 700, marginBottom: '0.5rem', color: '#0ea5e9' }}>Modifier mon courriel</div>
                  <input type="email" placeholder="Nouveau courriel" value={newEmail} onChange={e => setNewEmail(e.target.value)} style={ms.input} />
                  {emailMsg && <div style={{ fontSize: '0.75rem', color: emailMsg.includes('jour') ? '#22c55e' : '#ef4444', fontWeight: 600, marginTop: '4px' }}>{emailMsg}</div>}
                  <button onClick={handleChangeEmail} disabled={emailLoading} className="eval-btn-primary" style={{ fontSize: '0.75rem', marginTop: '0.5rem', width: '100%' }}>{emailLoading ? '...' : 'Mettre a jour'}</button>
                </div>
              </div>
            )}

            {/* TAB: Grossistes (directeur + admin) */}
            {settingsTab === 'grossistes' && canManageWholesale && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                <div style={{ padding: '0.75rem', background: 'var(--bg)', borderRadius: '6px', border: '1px solid var(--border)' }}>
                  <div style={{ fontSize: '0.75rem', fontWeight: 700, marginBottom: '0.5rem', color: '#a855f7' }}>Ajouter un grossiste</div>
                  <div style={ms.row}>
                    <div><label style={ms.label}>Prenom</label><input style={ms.input} value={wsNew.prenom} onChange={e => setWsNew(w => ({ ...w, prenom: e.target.value }))} /></div>
                    <div><label style={ms.label}>Nom *</label><input style={ms.input} value={wsNew.nom} onChange={e => setWsNew(w => ({ ...w, nom: e.target.value }))} /></div>
                  </div>
                  <div style={{ marginTop: '0.5rem' }}><label style={ms.label}>Entreprise</label><input style={ms.input} value={wsNew.entreprise} onChange={e => setWsNew(w => ({ ...w, entreprise: e.target.value }))} placeholder="Nom de l'entreprise" /></div>
                  <div style={{ ...ms.row, marginTop: '0.5rem' }}>
                    <div><label style={ms.label}>Telephone</label><input style={ms.input} value={wsNew.telephone} onChange={e => setWsNew(w => ({ ...w, telephone: e.target.value }))} placeholder="418-..." /></div>
                    <div><label style={ms.label}>Email *</label><input style={ms.input} type="email" value={wsNew.email} onChange={e => setWsNew(w => ({ ...w, email: e.target.value }))} /></div>
                  </div>
                  {wsMsg && <div style={{ fontSize: '0.75rem', color: wsMsg.includes('ajoute') || wsMsg.includes('Modifie') ? '#22c55e' : '#ef4444', fontWeight: 600, marginTop: '4px' }}>{wsMsg}</div>}
                  <button onClick={handleAddWholesale} disabled={wsLoading} className="eval-btn-primary" style={{ fontSize: '0.75rem', marginTop: '0.5rem', width: '100%', background: '#a855f7' }}>{wsLoading ? '...' : 'Ajouter'}</button>
                </div>
                <div style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-secondary)' }}>Contacts ({wsContacts.length})</div>
                {wsContacts.map(c => (
                  <div key={c.id || c.email} style={{ padding: '0.5rem 0.75rem', background: 'var(--bg)', borderRadius: '6px', border: `1px solid ${c.active !== false ? 'var(--border)' : '#ef444440'}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem' }}>
                    {wsEdit?.id === c.id ? (
                      <div style={{ flex: 1 }}>
                        <div style={ms.row}>
                          <input style={ms.input} value={wsEdit.name} onChange={e => setWsEdit(w => ({ ...w, name: e.target.value }))} placeholder="Nom" />
                          <input style={ms.input} value={wsEdit.company || ''} onChange={e => setWsEdit(w => ({ ...w, company: e.target.value }))} placeholder="Entreprise" />
                        </div>
                        <div style={{ ...ms.row, marginTop: '4px' }}>
                          <input style={ms.input} value={wsEdit.phone || ''} onChange={e => setWsEdit(w => ({ ...w, phone: e.target.value }))} placeholder="Tel" />
                          <input style={ms.input} value={wsEdit.email} onChange={e => setWsEdit(w => ({ ...w, email: e.target.value }))} placeholder="Email" />
                        </div>
                        <div style={{ display: 'flex', gap: '4px', marginTop: '4px' }}>
                          <button onClick={handleEditWholesale} className="eval-btn-primary" style={{ fontSize: '0.65rem', flex: 1 }}>Sauvegarder</button>
                          <button onClick={() => setWsEdit(null)} className="eval-btn-ghost" style={{ fontSize: '0.65rem' }}>Annuler</button>
                        </div>
                      </div>
                    ) : (
                      <>
                        <div style={{ flex: 1 }}>
                          <div style={{ fontWeight: 600, fontSize: '0.8rem' }}>{c.name} {c.company ? `(${c.company})` : ''}</div>
                          <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{c.email} {c.phone ? `| ${c.phone}` : ''}</div>
                        </div>
                        <button onClick={() => setWsEdit({ ...c })} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: '4px', padding: '2px 8px', fontSize: '0.65rem', cursor: 'pointer', color: 'var(--text-secondary)' }}>Modifier</button>
                      </>
                    )}
                  </div>
                ))}
              </div>
            )}

            {/* TAB: Equipe (admin + directeur) */}
            {settingsTab === 'equipe' && (isAdmin || isDirecteur) && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                {userMsg && <div style={{ fontSize: '0.75rem', color: '#22c55e', fontWeight: 600 }}>{userMsg}</div>}
                {settingsUsers.map(u => (
                  <div key={u.username} style={{ padding: '0.5rem 0.75rem', background: 'var(--bg)', borderRadius: '6px', border: '1px solid var(--border)' }}>
                    {userEdit?.username === u.username ? (
                      <div>
                        <div style={ms.row}>
                          <div><label style={ms.label}>Nom</label><input style={ms.input} value={userEdit.name} onChange={e => setUserEdit(p => ({ ...p, name: e.target.value }))} /></div>
                          <div><label style={ms.label}>Role</label><select style={ms.input} value={userEdit.role} onChange={e => setUserEdit(p => ({ ...p, role: e.target.value }))}><option value="conseiller">Conseiller</option><option value="directeur">Directeur</option><option value="admin">Admin</option></select></div>
                        </div>
                        <div style={{ ...ms.row, marginTop: '4px' }}>
                          <div><label style={ms.label}>Courriel</label><input style={ms.input} type="email" value={userEdit.email || ''} onChange={e => setUserEdit(p => ({ ...p, email: e.target.value }))} /></div>
                          <div><label style={ms.label}>Telephone</label><input style={ms.input} value={userEdit.phone || ''} onChange={e => setUserEdit(p => ({ ...p, phone: e.target.value }))} /></div>
                        </div>
                        <div style={{ marginTop: '4px' }}><label style={ms.label}>Nouveau mot de passe</label><input style={ms.input} value={userEdit.newPwd || ''} onChange={e => setUserEdit(p => ({ ...p, newPwd: e.target.value }))} placeholder="Laisser vide si inchange" /></div>
                        <div style={{ display: 'flex', gap: '4px', marginTop: '6px' }}>
                          <button onClick={() => handleEditUser(u, { name: userEdit.name, email: userEdit.email, phone: userEdit.phone, role: userEdit.role, password: userEdit.newPwd || undefined })} className="eval-btn-primary" style={{ fontSize: '0.65rem', flex: 1 }}>Sauvegarder</button>
                          <button onClick={() => setUserEdit(null)} className="eval-btn-ghost" style={{ fontSize: '0.65rem' }}>Annuler</button>
                        </div>
                      </div>
                    ) : (
                      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div>
                          <div style={{ fontWeight: 600, fontSize: '0.8rem' }}>{u.name} <span style={{ fontSize: '0.65rem', color: u.role === 'admin' ? '#ef4444' : u.role === 'directeur' ? '#a855f7' : '#0ea5e9', fontWeight: 700 }}>{u.role}</span></div>
                          <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{u.username} {u.email ? `| ${u.email}` : ''}</div>
                        </div>
                        <button onClick={() => setUserEdit({ ...u, newPwd: '' })} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: '4px', padding: '2px 8px', fontSize: '0.65rem', cursor: 'pointer', color: 'var(--text-secondary)' }}>Modifier</button>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </>
  );
}

function LoadingState() {
  return (
    <div style={{ textAlign: 'center', padding: '4rem', fontFamily: 'IBM Plex Mono, monospace', color: 'var(--text-secondary)' }}>
      <div style={{ fontSize: '1.5rem', marginBottom: '1rem' }}>[ LOADING ]</div>
      <div>Chargement depuis Supabase...</div>
    </div>
  );
}

function DashboardTab({ status, events, posts }) {
  const stats = status?.stats || {};
  const inv = stats.inventory || {};
  const postStats = stats.posts || {};
  const evStats = stats.events || {};
  const lastEvent = status?.last_event;

  return (
    <div>
      <div className="stats-grid animate-in" data-testid="stats-grid">
        <div className="card animate-in delay-1">
          <div className="card-label">Vehicules actifs</div>
          <div className="card-value" data-testid="stat-active-vehicles">{inv.active || 0}</div>
          <div className="card-sub">{inv.sold || 0} vendus / {inv.total || 0} total</div>
        </div>
        <div className="card animate-in delay-2">
          <div className="card-label">Posts Facebook</div>
          <div className="card-value" data-testid="stat-active-posts">{postStats.active || 0}</div>
          <div className="card-sub">{postStats.with_photos || 0} avec photos</div>
        </div>
        <div className="card animate-in delay-3">
          <div className="card-label">Sans photos</div>
          <div className="card-value" data-testid="stat-no-photo" style={{ color: (postStats.no_photo || 0) > 0 ? 'var(--accent-red)' : 'inherit' }}>
            {postStats.no_photo || 0}
          </div>
          <div className="card-sub">a mettre a jour</div>
        </div>
        <div className="card animate-in delay-4">
          <div className="card-label">Events totaux</div>
          <div className="card-value small" data-testid="stat-events">{(evStats.total || 0).toLocaleString()}</div>
          <div className="card-sub">{lastEvent ? `Dernier: ${lastEvent.type}` : ''}</div>
        </div>
      </div>

      <div className="bento-grid">
        <div className="card animate-in">
          <div className="section-subtitle">Events recents (Supabase live)</div>
          <EventsMiniTable events={events} />
        </div>
        <div className="card animate-in">
          <div className="section-subtitle">Posts Status</div>
          <PostsMiniList posts={posts} />
        </div>
      </div>
    </div>
  );
}

function EventsMiniTable({ events }) {
  if (!events || events.length === 0) return <div style={{ color: 'var(--text-secondary)', fontFamily: 'IBM Plex Mono', fontSize: '0.8rem', padding: '1rem 0' }}>Aucun event</div>;
  return (
    <div className="table-wrap" data-testid="events-table">
      <table>
        <thead>
          <tr><th>Date</th><th>Type</th><th>Slug</th></tr>
        </thead>
        <tbody>
          {events.slice(0, 15).map((e, i) => (
            <tr key={e.id || i} data-testid={`event-row-${i}`}>
              <td>{formatDateTime(e.created_at)}</td>
              <td><EventBadge type={e.type} /></td>
              <td style={{ fontSize: '0.75rem', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{e.slug}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function EventBadge({ type }) {
  const t = (type || '').toUpperCase();
  let cls = 'badge-info';
  if (t.includes('ERROR') || t.includes('FAIL')) cls = 'badge-error';
  else if (t.includes('NEW') || t.includes('POST') || t.includes('SUCCESS')) cls = 'badge-ok';
  else if (t.includes('SOLD')) cls = 'badge-sold';
  else if (t.includes('PRICE')) cls = 'badge-warning';
  else if (t.includes('PHOTO')) cls = 'badge-info';
  else if (t.includes('SKIP')) cls = 'badge-low';
  return <span className={`badge ${cls}`}>{t}</span>;
}

function PostsMiniList({ posts }) {
  const sorted = [...(posts || [])].sort((a, b) => (a.no_photo === b.no_photo ? 0 : a.no_photo ? -1 : 1));
  return (
    <div data-testid="posts-mini-list" style={{ maxHeight: 400, overflowY: 'auto' }}>
      {sorted.slice(0, 20).map((p, i) => (
        <div className="post-item" key={p.slug || i} data-testid={`post-item-${i}`}>
          <span className="post-stock">{p.stock}</span>
          <span className="post-title">{p.slug?.replace(/-/g, ' ').slice(0, 35)}</span>
          {p.no_photo ? (
            <span className="badge badge-no-photo">NO PHOTO</span>
          ) : (
            <span className="post-photos">{p.photo_count > 0 ? `${p.photo_count} photos` : 'OK'}</span>
          )}
          <span className={`badge ${p.status === 'ACTIVE' ? 'badge-active' : 'badge-sold'}`}>{p.status}</span>
        </div>
      ))}
    </div>
  );
}

function InventoryTab({ inventory }) {
  const [filter, setFilter] = useState('ALL');
  const filtered = filter === 'ALL' ? inventory : inventory.filter(v => v.status === filter);
  const active = inventory.filter(v => v.status === 'ACTIVE');
  const sold = inventory.filter(v => v.status === 'SOLD');

  return (
    <div>
      <h2 className="section-title" data-testid="inventory-title">Inventaire Kennebec (Supabase live)</h2>
      <div className="stats-grid" style={{ marginBottom: '1rem' }}>
        <div className="card" onClick={() => setFilter('ALL')} style={{ cursor: 'pointer', borderColor: filter === 'ALL' ? 'var(--border-heavy)' : undefined }}>
          <div className="card-label">Total</div><div className="card-value">{inventory.length}</div>
        </div>
        <div className="card" onClick={() => setFilter('ACTIVE')} style={{ cursor: 'pointer', borderColor: filter === 'ACTIVE' ? 'var(--accent-green)' : undefined }}>
          <div className="card-label">Actifs</div><div className="card-value" style={{ color: 'var(--accent-green)' }}>{active.length}</div>
        </div>
        <div className="card" onClick={() => setFilter('SOLD')} style={{ cursor: 'pointer', borderColor: filter === 'SOLD' ? 'var(--primary)' : undefined }}>
          <div className="card-label">Vendus</div><div className="card-value">{sold.length}</div>
        </div>
        <div className="card">
          <div className="card-label">Stickers PDF</div><div className="card-value small">60</div>
        </div>
      </div>
      <div className="card">
        <div className="table-wrap" data-testid="inventory-table">
          <table>
            <thead>
              <tr><th>Stock</th><th>Titre</th><th>Prix</th><th>KM</th><th>VIN</th><th>Status</th></tr>
            </thead>
            <tbody>
              {filtered.map((v, i) => (
                <tr key={v.slug || i} data-testid={`inv-row-${i}`}>
                  <td style={{ fontWeight: 600 }}>{v.stock || '--'}</td>
                  <td style={{ fontFamily: 'IBM Plex Sans, sans-serif' }}>{v.title || v.slug?.replace(/-/g, ' ') || '--'}</td>
                  <td>{v.price_int ? `${v.price_int.toLocaleString()} $` : '--'}</td>
                  <td>{v.km_int ? `${v.km_int.toLocaleString()} km` : '--'}</td>
                  <td style={{ fontSize: '0.65rem' }}>{v.vin || '--'}</td>
                  <td><span className={`badge ${v.status === 'ACTIVE' ? 'badge-active' : 'badge-sold'}`}>{v.status}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function PostsTab({ posts }) {
  const noPhoto = (posts || []).filter(p => p.no_photo);
  const active = (posts || []).filter(p => p.status === 'ACTIVE');
  const sold = (posts || []).filter(p => p.status === 'SOLD');

  return (
    <div>
      <h2 className="section-title" data-testid="posts-title">Posts Facebook (Supabase live)</h2>
      <div className="stats-grid" style={{ marginBottom: '1rem' }}>
        <div className="card"><div className="card-label">Total</div><div className="card-value">{(posts || []).length}</div></div>
        <div className="card"><div className="card-label">Actifs</div><div className="card-value" style={{ color: 'var(--accent-green)' }}>{active.length}</div></div>
        <div className="card"><div className="card-label">Vendus</div><div className="card-value">{sold.length}</div></div>
        <div className="card"><div className="card-label">Sans photos</div><div className="card-value" style={{ color: noPhoto.length > 0 ? 'var(--accent-red)' : 'inherit' }}>{noPhoto.length}</div></div>
      </div>

      {noPhoto.length > 0 && (
        <div className="card" style={{ borderColor: 'var(--accent-red)', borderWidth: 2, marginBottom: '1.5rem' }}>
          <div className="section-subtitle" style={{ color: 'var(--accent-red)' }}>
            Posts sans photos ({noPhoto.length}) — En attente de PHOTOS_ADDED
          </div>
          {noPhoto.map((p, i) => (
            <div className="post-item" key={p.slug || i} data-testid={`no-photo-post-${i}`}>
              <span className="post-stock">{p.stock}</span>
              <span className="post-title">{p.slug?.replace(/-/g, ' ')}</span>
              <span className="badge badge-no-photo">NO PHOTO</span>
              <span style={{ fontFamily: 'IBM Plex Mono, monospace', fontSize: '0.65rem', color: 'var(--text-secondary)' }}>
                {p.post_id?.slice(0, 15)}
              </span>
            </div>
          ))}
        </div>
      )}

      <div className="card">
        <div className="section-subtitle">Tous les posts ({(posts || []).length})</div>
        <div className="table-wrap" data-testid="posts-table">
          <table>
            <thead><tr><th>Stock</th><th>Slug</th><th>Post ID</th><th>Publie le</th><th>Modifie le</th><th>Status</th></tr></thead>
            <tbody>
              {(posts || []).map((p, i) => (
                <tr key={p.slug || i} data-testid={`post-row-${i}`}>
                  <td style={{ fontWeight: 600 }}>{p.stock}</td>
                  <td style={{ fontFamily: 'IBM Plex Sans, sans-serif', fontSize: '0.78rem' }}>{p.slug?.replace(/-/g, ' ').slice(0, 38)}</td>
                  <td style={{ fontSize: '0.65rem' }}>{p.post_id?.slice(0, 18) || '--'}</td>
                  <td>{formatDate(p.published_at)}</td>
                  <td>{formatDate(p.last_updated_at)}</td>
                  <td>
                    {p.no_photo && <span className="badge badge-no-photo" style={{ marginRight: 4 }}>NO PHOTO</span>}
                    <span className={`badge ${p.status === 'ACTIVE' ? 'badge-active' : 'badge-sold'}`}>{p.status}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function EventsTab({ events }) {
  return (
    <div>
      <h2 className="section-title" data-testid="events-title">Events (Supabase live — {(events || []).length} derniers)</h2>
      <div className="card">
        <div className="table-wrap" data-testid="events-full-table">
          <table>
            <thead><tr><th>Date</th><th>Type</th><th>Slug</th><th>Payload</th></tr></thead>
            <tbody>
              {(events || []).map((e, i) => (
                <tr key={e.id || i} data-testid={`event-full-row-${i}`}>
                  <td>{formatDateTime(e.created_at)}</td>
                  <td><EventBadge type={e.type} /></td>
                  <td style={{ fontSize: '0.75rem', maxWidth: 250, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{e.slug}</td>
                  <td style={{ fontSize: '0.7rem', maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: 'var(--text-secondary)' }}>
                    {e.payload ? JSON.stringify(e.payload).slice(0, 80) : '--'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function ArchitectureTab({ architecture }) {
  if (!architecture) return null;
  const typeClass = { core: 'core', external: 'external', storage: 'storage' };
  return (
    <div>
      <h2 className="section-title" data-testid="arch-title">Architecture Kenbot</h2>
      <div className="section-subtitle">Machine d'etat vehicule</div>
      <div className="states-row" data-testid="states-row">
        <div className="state-box new">NEW</div>
        <div className="state-box sold">SOLD</div>
        <div className="state-box restore">RESTORE</div>
        <div className="state-box price">PRICE_CHANGED</div>
        <div className="state-box photos">PHOTOS_ADDED</div>
      </div>
      <div className="section-subtitle" style={{ marginTop: '2rem' }}>Composants du systeme</div>
      <div className="arch-grid" data-testid="arch-grid">
        {architecture.components.map(c => (
          <div key={c.id} className={`arch-node ${typeClass[c.type] || ''}`} data-testid={`arch-node-${c.id}`}>
            <div className="arch-node-title">{c.name}</div>
            <div className="arch-node-desc">{c.description}</div>
          </div>
        ))}
      </div>
      <div className="card" style={{ marginTop: '1.5rem' }}>
        <div className="section-subtitle">Flux de donnees</div>
        {architecture.flows.map((f, i) => (
          <div className="arch-flow" key={i} data-testid={`flow-${i}`}>
            <span style={{ fontWeight: 600 }}>{f.from}</span>
            <span className="arch-arrow">&rarr;</span>
            <span style={{ fontWeight: 600 }}>{f.to}</span>
            <span style={{ marginLeft: 'auto' }}>{f.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function ChangelogTab({ changelog }) {
  return (
    <div>
      <h2 className="section-title" data-testid="changelog-title">Changelog & Fixes</h2>
      {(changelog || []).map((entry, i) => (
        <div className="changelog-item animate-in" key={i} style={{ animationDelay: `${i * 0.05}s` }} data-testid={`changelog-entry-${i}`}>
          <div className="changelog-header">
            <span className="changelog-version">{entry.version}</span>
            <span className="changelog-date">{entry.date}</span>
            <span className={`badge ${entry.type === 'bugfix' ? 'badge-error' : 'badge-info'}`}>{entry.type}</span>
            <span className="changelog-title">{entry.title}</span>
          </div>
          <div className="changelog-changes">
            {(entry.changes || []).map((c, j) => (
              <div className="changelog-change" key={j} data-testid={`change-${i}-${j}`}>
                <span className={`badge badge-${c.severity}`}>{c.severity}</span>
                <span className="changelog-change-desc">
                  {c.description}
                  {c.fix && <><br /><strong style={{ color: 'var(--accent-green)' }}>Fix:</strong> {c.fix}</>}
                </span>
                <span className="changelog-change-file">{c.file}{c.line ? ` L${c.line}` : ''}</span>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}


function CockpitTab({ inventory, status }) {
  const [simulating, setSimulating] = useState(false);
  const [simResults, setSimResults] = useState(null);
  const [maxTargets, setMaxTargets] = useState(3);
  const [forceStock, setForceStock] = useState('');
  const [expandedIdx, setExpandedIdx] = useState(null);
  const [copiedIdx, setCopiedIdx] = useState(null);
  const [logs, setLogs] = useState(null);
  const [logsLoading, setLogsLoading] = useState(false);

  const stats = status?.stats || {};
  const inv = stats.inventory || {};
  const postStats = stats.posts || {};

  const runSimulation = async () => {
    setSimulating(true);
    setSimResults(null);
    setExpandedIdx(null);
    try {
      const params = new URLSearchParams({ max_targets: maxTargets });
      if (forceStock.trim()) params.set('force_stock', forceStock.trim());
      const res = await fetch(`${API}/api/cockpit/simulate?${params}`, { method: 'POST' });
      const data = await res.json();
      setSimResults(data);
      if (data.ok && data.results?.length > 0) setExpandedIdx(0);
    } catch (e) {
      setSimResults({ ok: false, error: e.message });
    }
    setSimulating(false);
  };

  const loadLogs = async () => {
    setLogsLoading(true);
    try {
      const res = await fetch(`${API}/api/cockpit/recent-logs?limit=30`);
      const data = await res.json();
      setLogs(data);
    } catch (e) { setLogs({ ok: false, error: e.message }); }
    setLogsLoading(false);
  };

  const handleCopy = (text, idx) => {
    navigator.clipboard.writeText(text);
    setCopiedIdx(idx);
    setTimeout(() => setCopiedIdx(null), 2000);
  };

  return (
    <div data-testid="cockpit-tab">
      <h2 className="section-title" data-testid="cockpit-title">Cockpit Kenbot</h2>

      {/* Quick stats row */}
      <div className="ck-stats-row">
        <div className="ck-stat"><span className="ck-stat-val">{inv.active || 0}</span><span className="ck-stat-label">Inventaire actif</span></div>
        <div className="ck-stat"><span className="ck-stat-val">{postStats.active || 0}</span><span className="ck-stat-label">Posts FB actifs</span></div>
        <div className="ck-stat"><span className="ck-stat-val" style={{color: (postStats.no_photo||0) > 0 ? 'var(--accent-red)' : undefined}}>{postStats.no_photo || 0}</span><span className="ck-stat-label">Sans photos</span></div>
        <div className="ck-stat"><span className="ck-stat-val">{(inv.active || 0) - (postStats.active || 0)}</span><span className="ck-stat-label">Sans post FB</span></div>
      </div>

      {/* Simulation panel */}
      <div className="ck-sim-panel" data-testid="ck-sim-panel">
        <div className="ck-sim-header">
          <div>
            <div className="ck-sim-title">Simulation Dry Run</div>
            <div className="ck-sim-sub">Genere les textes IA sans publier — voir le resultat avant de lancer le vrai cron</div>
          </div>
          <div className="ck-sim-controls">
            <div className="ck-sim-field">
              <span className="ck-sim-field-label">Cibles</span>
              <input type="number" value={maxTargets} onChange={e => setMaxTargets(parseInt(e.target.value) || 3)} min={1} max={10} className="ck-sim-input" data-testid="ck-max-targets" />
            </div>
            <div className="ck-sim-field">
              <span className="ck-sim-field-label">Force stock</span>
              <input type="text" value={forceStock} onChange={e => setForceStock(e.target.value)} placeholder="06193" className="ck-sim-input ck-sim-stock" data-testid="ck-force-stock" />
            </div>
            <button className="ck-sim-btn" onClick={runSimulation} disabled={simulating} data-testid="ck-simulate-btn">
              {simulating ? 'SIMULATION EN COURS...' : 'SIMULER LE CRON'}
            </button>
          </div>
        </div>

        {/* Loading */}
        {simulating && (
          <div className="ck-loading">
            <div className="tp-loading-bar"></div>
            <span>Generation IA en cours pour {maxTargets} vehicule{maxTargets > 1 ? 's' : ''}...</span>
          </div>
        )}

        {/* Results */}
        {simResults && !simulating && (
          simResults.ok ? (
            <div className="ck-results" data-testid="ck-results">
              <div className="ck-results-header">
                <span>{simResults.count} vehicule{simResults.count > 1 ? 's' : ''} traite{simResults.count > 1 ? 's' : ''}</span>
                <span className="ck-results-time">{simResults.elapsed_seconds}s</span>
              </div>
              {simResults.results.map((r, i) => (
                <div key={r.stock || i} className={`ck-result-card ${expandedIdx === i ? 'expanded' : ''}`} data-testid={`ck-result-${i}`}>
                  <div className="ck-result-header" onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}>
                    <div className="ck-result-left">
                      <span className="ck-result-stock">{r.stock}</span>
                      <span className="ck-result-title">{r.title}</span>
                      <span className={`badge ${r.event === 'NEW' ? 'badge-ok' : r.event === 'PREVIEW' ? 'badge-info' : 'badge-warning'}`}>{r.event}</span>
                      {r.generation_method && <span className={`badge ${r.generation_method.includes('STICKER') ? 'ck-badge-sticker' : 'ck-badge-llm'}`}>{r.generation_method}</span>}
                      {r.vin_decoded && <span className="badge ck-badge-vin">VIN</span>}
                    </div>
                    <div className="ck-result-right">
                      {r.error ? (
                        <span className="badge badge-error">ERREUR</span>
                      ) : (
                        <span className="badge badge-ok">{r.chars} chars</span>
                      )}
                      {r.elapsed && <span className="ck-result-time">{r.elapsed}s</span>}
                      <span className="ck-expand-icon">{expandedIdx === i ? '−' : '+'}</span>
                    </div>
                  </div>

                  {expandedIdx === i && (
                    <div className="ck-result-body">
                      {/* Intelligence row */}
                      {r.intelligence && (
                        <div className="ck-intel-row">
                          {r.intelligence.brand && <span className="ck-intel-tag">{r.intelligence.brand}</span>}
                          {r.intelligence.model && <span className="ck-intel-tag">{r.intelligence.model}</span>}
                          {r.intelligence.trim && <span className="ck-intel-tag">{r.intelligence.trim}</span>}
                          {r.intelligence.type && <span className={`ck-intel-tag ck-type-${r.intelligence.type}`}>{r.intelligence.type}</span>}
                          {r.intelligence.hp && <span className="ck-intel-tag ck-hp">{r.intelligence.engine} — {r.intelligence.hp} HP</span>}
                          {r.intelligence.vibe && <span className="ck-intel-tag ck-vibe">{r.intelligence.vibe}</span>}
                        </div>
                      )}
                      {/* VIN specs */}
                      {r.vin_specs && (
                        <div className="ck-vin-row">
                          {r.vin_specs.drive && <span className="tp-vin-tag">{r.vin_specs.drive}</span>}
                          {r.vin_specs.transmission && <span className="tp-vin-tag">{r.vin_specs.transmission}</span>}
                          {r.vin_specs.fuel && <span className="tp-vin-tag">{r.vin_specs.fuel}</span>}
                          {r.vin_specs.electrification && <span className="tp-vin-tag tp-vin-elec">{r.vin_specs.electrification}</span>}
                          {r.vin_specs.seats && <span className="tp-vin-tag">{r.vin_specs.seats} places</span>}
                          {r.vin_specs.country && <span className="tp-vin-tag">{r.vin_specs.country}</span>}
                        </div>
                      )}
                      {/* Text */}
                      {r.text ? (
                        <div className="ck-text-wrap">
                          <div className="ck-text-actions">
                            <button className="tp-copy-btn" onClick={() => handleCopy(r.text, i)} style={{padding:'4px 12px',fontSize:'0.65rem'}}>
                              {copiedIdx === i ? 'COPIE !' : 'COPIER'}
                            </button>
                          </div>
                          <div className="ck-text-body">{r.text}</div>
                        </div>
                      ) : r.error ? (
                        <div className="ck-error">{r.error}</div>
                      ) : null}
                    </div>
                  )}
                </div>
              ))}
            </div>
          ) : (
            <div className="ck-error">{simResults.error}</div>
          )
        )}
      </div>

      {/* Recent logs */}
      <div className="ck-logs-panel" data-testid="ck-logs-panel">
        <div className="ck-logs-header">
          <span className="ck-sim-title">Logs recents (Supabase)</span>
          <button className="ck-logs-btn" onClick={loadLogs} disabled={logsLoading} data-testid="ck-logs-btn">
            {logsLoading ? 'CHARGEMENT...' : 'CHARGER LES LOGS'}
          </button>
        </div>
        {logs?.ok && (
          <div className="ck-logs-body">
            {logs.runs?.length > 0 && (
              <div className="ck-runs">
                <div className="ck-runs-title">Derniers runs</div>
                {logs.runs.map((r, i) => (
                  <div key={r.run_id || i} className="ck-run-item">
                    <span className={`badge ${r.status === 'ok' ? 'badge-ok' : 'badge-error'}`}>{r.status}</span>
                    <span className="ck-run-date">{formatDateTime(r.created_at)}</span>
                    <span className="ck-run-note">{r.note || ''}</span>
                  </div>
                ))}
              </div>
            )}
            <div className="ck-events-list">
              {(logs.events || []).slice(0, 20).map((e, i) => (
                <div key={e.id || i} className="ck-event-item">
                  <span className="ck-ev-date">{formatDateTime(e.created_at)}</span>
                  <EventBadge type={e.type} />
                  <span className="ck-ev-slug">{(e.slug || '').slice(0, 30)}</span>
                  <span className="ck-ev-payload">{e.payload ? JSON.stringify(e.payload).slice(0, 60) : ''}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}


function TextPreviewTab({ inventory, onSelectStock, selectedStock }) {
  const [search, setSearch] = useState('');
  const [generating, setGenerating] = useState(false);
  const [result, setResult] = useState(null);
  const [copied, setCopied] = useState(false);
  const [genCount, setGenCount] = useState(0);
  const [mode, setMode] = useState('generate'); // 'generate' or 'sticker'

  const activeVehicles = (inventory || []).filter(v => v.status === 'ACTIVE');
  const filtered = activeVehicles.filter(v => {
    if (!search) return true;
    const s = search.toLowerCase();
    return (v.title || '').toLowerCase().includes(s)
      || (v.stock || '').toLowerCase().includes(s)
      || (v.vin || '').toLowerCase().includes(s);
  });

  const handleGenerate = async (stock, useSticker = false) => {
    if (!stock) return;
    setGenerating(true);
    setResult(null);
    setCopied(false);
    setMode(useSticker ? 'sticker' : 'generate');
    try {
      const endpoint = useSticker
        ? `${API}/api/humanize-sticker/${stock}`
        : `${API}/api/generate-text/${stock}`;
      const res = await fetch(endpoint, { method: 'POST' });
      const data = await res.json();
      setResult(data);
      setGenCount(c => c + 1);
    } catch (e) {
      setResult({ ok: false, error: e.message });
    }
    setGenerating(false);
  };

  const handleSelect = (stock) => {
    onSelectStock(stock);
    setResult(null);
    setCopied(false);
    setMode('generate');
  };

  const handleCopy = () => {
    if (result?.text) {
      navigator.clipboard.writeText(result.text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const intel = result?.intelligence || {};
  const selectedVehicle = activeVehicles.find(v => v.stock === selectedStock);
  // Pre-fill basic info from inventory when no AI result yet
  const displayKm = intel.km_formatted || (selectedVehicle?.km_int ? `${selectedVehicle.km_int.toLocaleString()} km` : '—');
  const displayPrice = intel.price_formatted || (selectedVehicle?.price_int ? `${selectedVehicle.price_int.toLocaleString()} $` : '—');

  // Detect Stellantis VINs (start with 1C, 2C, 3C, 1J, 3D, 2A)
  const vin = (selectedVehicle?.vin || '').toUpperCase();
  const isStellantis = /^(1C|2C|3C|1J|3D|2A)/.test(vin);
  const titleLower = (selectedVehicle?.title || '').toLowerCase();
  const isStellBrand = ['ram', 'dodge', 'jeep', 'chrysler', 'fiat'].some(b => titleLower.startsWith(b));

  return (
    <div data-testid="text-preview-tab">
      <h2 className="section-title" data-testid="text-preview-title">Preview Texte IA</h2>

      <div className="tp-layout">
        {/* Left: Vehicle selector */}
        <div className="tp-sidebar" data-testid="tp-sidebar">
          <div className="tp-search-wrap">
            <input
              className="tp-search"
              type="text"
              placeholder="Rechercher stock, titre, VIN..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              data-testid="tp-search-input"
            />
            <span className="tp-count">{filtered.length} vehicules</span>
          </div>
          <div className="tp-vehicle-list" data-testid="tp-vehicle-list">
            {filtered.map((v, i) => (
              <div
                key={v.stock || i}
                className={`tp-vehicle-item ${selectedStock === v.stock ? 'selected' : ''}`}
                onClick={() => handleSelect(v.stock)}
                data-testid={`tp-vehicle-${v.stock}`}
              >
                <div className="tp-v-stock">{v.stock}</div>
                <div className="tp-v-title">{v.title || v.slug?.replace(/-/g, ' ')}</div>
                <div className="tp-v-meta">
                  <span>{v.price_int ? `${v.price_int.toLocaleString()} $` : '--'}</span>
                  <span>{v.km_int ? `${v.km_int.toLocaleString()} km` : '--'}</span>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Right: Preview panel */}
        <div className="tp-preview" data-testid="tp-preview-panel">
          {!selectedStock ? (
            <div className="tp-empty" data-testid="tp-empty-state">
              <div className="tp-empty-icon">AI</div>
              <div className="tp-empty-title">Selectionnez un vehicule</div>
              <div className="tp-empty-sub">Cliquez sur un vehicule dans la liste pour generer un apercu du texte Facebook IA</div>
            </div>
          ) : (
            <>
              {/* Vehicle header */}
              <div className="tp-vehicle-header" data-testid="tp-vehicle-header">
                <div>
                  <div className="tp-vh-title">{selectedVehicle?.title || selectedStock}</div>
                  <div className="tp-vh-meta">
                    Stock: {selectedStock}
                    {selectedVehicle?.vin && <> &middot; VIN: {selectedVehicle.vin}</>}
                  </div>
                  <div className="tp-vh-price-row">
                    <span className="tp-vh-price">{selectedVehicle?.price_int ? `${selectedVehicle.price_int.toLocaleString()} $` : ''}</span>
                    <span className="tp-vh-km">{selectedVehicle?.km_int ? `${selectedVehicle.km_int.toLocaleString()} km` : ''}</span>
                  </div>
                </div>
                <div className="tp-actions">
                  <button
                    className="tp-generate-btn"
                    onClick={() => handleGenerate(selectedStock, false)}
                    disabled={generating}
                    data-testid="tp-generate-btn"
                  >
                    {generating && mode === 'generate' ? 'GENERATION...' : 'GENERER TEXTE'}
                  </button>
                  {(isStellantis || isStellBrand) && (
                    <button
                      className="tp-sticker-btn"
                      onClick={() => handleGenerate(selectedStock, true)}
                      disabled={generating}
                      data-testid="tp-sticker-btn"
                    >
                      {generating && mode === 'sticker' ? 'HUMANISATION...' : 'HUMANISER STICKER'}
                    </button>
                  )}
                  {result?.ok && (
                    <button className="tp-copy-btn" onClick={handleCopy} data-testid="tp-copy-btn">
                      {copied ? 'COPIE !' : 'COPIER'}
                    </button>
                  )}
                </div>
              </div>

              {/* Intelligence panel */}
              {(result?.intelligence || selectedVehicle) && (
                <div className="tp-intel" data-testid="tp-intel-panel">
                  <div className="tp-intel-title">Intelligence Vehicule {result?.vin_specs ? '+ VIN NHTSA' : ''}</div>
                  <div className="tp-intel-grid">
                    <div className="tp-intel-item">
                      <span className="tp-il">Marque</span>
                      <span className="tp-iv">{intel.brand || '—'}</span>
                    </div>
                    <div className="tp-intel-item">
                      <span className="tp-il">Modele</span>
                      <span className="tp-iv">{intel.model || '—'}</span>
                    </div>
                    <div className="tp-intel-item">
                      <span className="tp-il">Trim</span>
                      <span className="tp-iv">{intel.trim || '—'}</span>
                    </div>
                    <div className="tp-intel-item">
                      <span className="tp-il">Type</span>
                      <span className="tp-iv">
                        <span className={`badge tp-type-badge tp-type-${intel.vehicle_type || 'general'}`}>
                          {intel.vehicle_type || '—'}
                        </span>
                      </span>
                    </div>
                    {intel.hp && (
                      <div className="tp-intel-item tp-intel-wide">
                        <span className="tp-il">Moteur</span>
                        <span className="tp-iv tp-engine">{intel.engine} — {intel.hp} HP</span>
                      </div>
                    )}
                    {intel.trim_vibe && (
                      <div className="tp-intel-item tp-intel-wide">
                        <span className="tp-il">Vibe</span>
                        <span className="tp-iv tp-vibe">{intel.trim_vibe}</span>
                      </div>
                    )}
                    <div className="tp-intel-item">
                      <span className="tp-il">KM</span>
                      <span className="tp-iv">{displayKm} {intel.km_description && <span className="tp-desc">({intel.km_description})</span>}</span>
                    </div>
                    <div className="tp-intel-item">
                      <span className="tp-il">Prix</span>
                      <span className="tp-iv">{displayPrice} {intel.price_description && <span className="tp-desc">({intel.price_description})</span>}</span>
                    </div>
                  </div>
                  {/* VIN NHTSA Specs */}
                  {result?.vin_specs && (
                    <div className="tp-vin-specs" data-testid="tp-vin-specs">
                      <div className="tp-vin-specs-title">Specs VIN (NHTSA)</div>
                      <div className="tp-vin-specs-grid">
                        {result.vin_specs.drive_type && <div className="tp-vin-tag">{result.vin_specs.drive_type}</div>}
                        {result.vin_specs.transmission && <div className="tp-vin-tag">{result.vin_specs.transmission}{result.vin_specs.transmission_speeds ? ` ${result.vin_specs.transmission_speeds}v` : ''}</div>}
                        {result.vin_specs.fuel_primary && <div className="tp-vin-tag">{result.vin_specs.fuel_primary}</div>}
                        {result.vin_specs.electrification && <div className="tp-vin-tag tp-vin-elec">{result.vin_specs.electrification}</div>}
                        {result.vin_specs.seats && <div className="tp-vin-tag">{result.vin_specs.seats} places{result.vin_specs.seat_rows > 2 ? `, ${result.vin_specs.seat_rows} rangees` : ''}</div>}
                        {result.vin_specs.plant_country && <div className="tp-vin-tag">{result.vin_specs.plant_country}</div>}
                        {result.vin_specs.adaptive_cruise === 'Standard' && <div className="tp-vin-tag tp-vin-safety">Cruise adaptatif</div>}
                        {result.vin_specs.auto_braking === 'Standard' && <div className="tp-vin-tag tp-vin-safety">Freinage auto</div>}
                        {result.vin_specs.blind_spot === 'Standard' && <div className="tp-vin-tag tp-vin-safety">Angle mort</div>}
                        {result.vin_specs.lane_keeping === 'Standard' && <div className="tp-vin-tag tp-vin-safety">Maintien voie</div>}
                        {result.vin_specs.backup_camera === 'Standard' && <div className="tp-vin-tag tp-vin-safety">Camera recul</div>}
                        {result.vin_specs.headlamp_type?.includes('LED') && <div className="tp-vin-tag">Phares LED</div>}
                        {result.vin_specs.keyless === 'Standard' && <div className="tp-vin-tag">Sans cle</div>}
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* Generated text */}
              {generating && (
                <div className="tp-loading" data-testid="tp-loading">
                  <div className="tp-loading-bar"></div>
                  <span>Generation du texte via GPT-4o...</span>
                </div>
              )}

              {result && !generating && (
                result.ok ? (
                  <div className="tp-text-result" data-testid="tp-text-result">
                    <div className="tp-text-header">
                      <span className="tp-text-label">{result.is_sticker ? 'Sticker Stellantis humanise' : 'Texte Facebook genere'}</span>
                      <div className="tp-text-meta">
                        <span className="badge badge-ok">{result.chars} chars</span>
                        {result.style && <span className="badge badge-info">style: {result.style}</span>}
                        {result.is_sticker && <span className="badge" style={{background:'#7C3AED',color:'white'}}>STICKER</span>}
                        <span className="badge badge-active">{result.model}</span>
                      </div>
                    </div>
                    <div className="tp-text-body" data-testid="tp-text-body">
                      {result.text}
                    </div>
                  </div>
                ) : (
                  <div className="tp-error" data-testid="tp-error">
                    <span className="badge badge-error">ERREUR</span>
                    <span>{result.error}</span>
                  </div>
                )
              )}

              {!result && !generating && (
                <div className="tp-hint" data-testid="tp-hint">
                  Cliquez sur "GENERER LE TEXTE" pour voir l'apercu IA
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function RunPanel({ onClose }) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [dryRun, setDryRun] = useState(false);
  const [maxTargets, setMaxTargets] = useState(4);
  const [forceStock, setForceStock] = useState('');

  const triggerRun = async () => {
    setLoading(true);
    setResult(null);
    try {
      const res = await fetch(`${API}/api/trigger/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dry_run: dryRun, max_targets: maxTargets, force_stock: forceStock || null }),
      });
      const data = await res.json();
      setResult(data);
    } catch (e) {
      setResult({ ok: false, message: e.message });
    }
    setLoading(false);
  };

  return (
    <div className="run-panel" data-testid="run-panel">
      <div className="run-panel-inner">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
          <span style={{ fontFamily: 'Chivo, sans-serif', fontWeight: 900, fontSize: '1.1rem' }}>LANCER LE CRON</span>
          <button onClick={onClose} style={{ background: 'none', border: '1px solid var(--border)', padding: '4px 12px', cursor: 'pointer', fontFamily: 'IBM Plex Mono, monospace', fontSize: '0.75rem' }} data-testid="close-run-panel">FERMER</button>
        </div>

        <div className="run-options">
          <label className="run-option" data-testid="dry-run-toggle">
            <input type="checkbox" checked={dryRun} onChange={e => setDryRun(e.target.checked)} />
            <span>Dry Run</span>
            <span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>— Simule sans publier</span>
          </label>

          <div className="run-option">
            <span>Max targets</span>
            <input type="number" value={maxTargets} onChange={e => setMaxTargets(parseInt(e.target.value) || 4)} min={1} max={20} style={{ width: 60, fontFamily: 'IBM Plex Mono', padding: '4px 8px', border: '1px solid var(--border)' }} data-testid="max-targets-input" />
          </div>

          <div className="run-option">
            <span>Force stock</span>
            <input type="text" value={forceStock} onChange={e => setForceStock(e.target.value)} placeholder="ex: 06234" style={{ width: 120, fontFamily: 'IBM Plex Mono', padding: '4px 8px', border: '1px solid var(--border)', textTransform: 'uppercase' }} data-testid="force-stock-input" />
            <span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>— Optionnel</span>
          </div>
        </div>

        <button className="run-execute-btn" onClick={triggerRun} disabled={loading} data-testid="execute-run-btn">
          {loading ? 'ENVOI EN COURS...' : 'EXECUTER'}
        </button>

        {result && (
          <div className={`run-result ${result.ok ? 'run-result-ok' : 'run-result-error'}`} data-testid="run-result">
            <span style={{ fontWeight: 600 }}>{result.ok ? 'OK' : 'ERREUR'}</span>
            <span>{result.message}</span>
          </div>
        )}
      </div>
    </div>
  );
}

function formatTime(ts) {
  if (!ts) return '--';
  try { return new Date(ts).toLocaleTimeString('fr-CA', { hour: '2-digit', minute: '2-digit' }); } catch { return ts; }
}
function formatDate(ts) {
  if (!ts) return '--';
  try { return new Date(ts).toLocaleDateString('fr-CA', { month: 'short', day: 'numeric', year: 'numeric' }); } catch { return ts; }
}
function formatDateTime(ts) {
  if (!ts) return '--';
  try { const d = new Date(ts); return `${d.toLocaleDateString('fr-CA', { month: 'short', day: 'numeric' })} ${d.toLocaleTimeString('fr-CA', { hour: '2-digit', minute: '2-digit' })}`; } catch { return ts; }
}


function CompareTab() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [lastRefresh, setLastRefresh] = useState(null);

  const fetchCompare = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API}/api/vehicles/compare`);
      const json = await res.json();
      setData(json);
      setLastRefresh(new Date().toLocaleTimeString('fr-CA'));
    } catch (e) {
      console.error('Compare fetch error:', e);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetchCompare(); }, [fetchCompare]);

  // Auto-refresh 2 fois par jour (12h = 43200000ms)
  useEffect(() => {
    const interval = setInterval(fetchCompare, 43200000);
    return () => clearInterval(interval);
  }, [fetchCompare]);

  if (loading && !data) return <LoadingState />;
  if (!data) return <div style={{ padding: '2rem', color: '#ef4444' }}>Erreur de chargement</div>;

  const { vehicles = [], stats = {} } = data;

  const filtered = vehicles.filter(v => {
    const matchFilter = filter === 'all'
      || (filter === 'problems' && v.problem)
      || (filter === 'active' && v.kennebec_status === 'ACTIVE' && v.fb_status === 'ACTIVE')
      || (filter === 'no_fb' && v.fb_status === 'AUCUN POST')
      || (filter === 'faux_vendu' && v.problem === 'FAUX VENDU')
      || (filter === 'sans_photo' && v.problem === 'SANS PHOTO')
      || (filter === 'sold' && v.fb_status === 'SOLD');
    const matchSearch = !search || v.title.toLowerCase().includes(search.toLowerCase())
      || v.stock.toLowerCase().includes(search.toLowerCase());
    return matchFilter && matchSearch;
  });

  const statusBadge = (status, problem) => {
    const colors = {
      'ACTIVE': { bg: '#065f46', color: '#6ee7b7' },
      'SOLD': { bg: '#7f1d1d', color: '#fca5a5' },
      'AUCUN POST': { bg: '#78350f', color: '#fde68a' },
      'INCONNU': { bg: '#374151', color: '#9ca3af' },
    };
    const c = colors[status] || colors['INCONNU'];
    return (
      <span style={{ padding: '2px 8px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 600, backgroundColor: c.bg, color: c.color }}>
        {status}
      </span>
    );
  };

  const problemBadge = (problem) => {
    if (!problem) return null;
    const colors = {
      'FAUX VENDU': { bg: '#7f1d1d', color: '#fca5a5', icon: '🚨' },
      'PAS SUR FB': { bg: '#78350f', color: '#fde68a', icon: '⚠️' },
      'SANS PHOTO': { bg: '#713f12', color: '#fef08a', icon: '📷' },
      'FB PAS MAJ': { bg: '#1e3a5f', color: '#93c5fd', icon: '🔄' },
    };
    const c = colors[problem] || { bg: '#374151', color: '#9ca3af', icon: '❓' };
    return (
      <span style={{ padding: '2px 8px', borderRadius: '4px', fontSize: '0.75rem', fontWeight: 700, backgroundColor: c.bg, color: c.color }}>
        {c.icon} {problem}
      </span>
    );
  };

  const fmtPrice = (p) => p ? `${Number(p).toLocaleString('fr-CA')} $` : '—';
  const fmtDate = (d) => {
    if (!d) return '—';
    try {
      return new Date(d).toLocaleDateString('fr-CA', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
    } catch { return d; }
  };

  return (
    <div style={{ padding: '1.5rem' }} data-testid="compare-tab">
      {/* Stats cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: '0.75rem', marginBottom: '1.5rem' }}>
        {[
          { label: 'Kennebec', value: stats.kennebec_active || 0, color: '#3b82f6', icon: '🌐' },
          { label: 'FB Actifs', value: stats.fb_active || 0, color: '#22c55e', icon: '📘' },
          { label: 'FB Vendus', value: stats.fb_sold || 0, color: '#ef4444', icon: '🏷️' },
          { label: 'Pas sur FB', value: stats.no_fb_post || 0, color: '#f59e0b', icon: '⚠️' },
          { label: 'Faux Vendus', value: stats.faux_vendu || 0, color: '#dc2626', icon: '🚨' },
          { label: 'Sans Photo', value: stats.sans_photo || 0, color: '#eab308', icon: '📷' },
          { label: 'Problèmes', value: stats.problems || 0, color: stats.problems > 0 ? '#ef4444' : '#22c55e', icon: stats.problems > 0 ? '❌' : '✅' },
        ].map(s => (
          <div key={s.label} style={{ backgroundColor: '#1a1a2e', borderRadius: '8px', padding: '1rem', textAlign: 'center', border: `1px solid ${s.color}33` }}>
            <div style={{ fontSize: '1.5rem' }}>{s.icon}</div>
            <div style={{ fontSize: '1.75rem', fontWeight: 700, color: s.color }}>{s.value}</div>
            <div style={{ fontSize: '0.75rem', color: '#9ca3af' }}>{s.label}</div>
          </div>
        ))}
      </div>

      {/* Filters + Search */}
      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '1rem', flexWrap: 'wrap', alignItems: 'center' }}>
        {[
          { id: 'all', label: `Tous (${vehicles.length})` },
          { id: 'problems', label: `Problèmes (${stats.problems || 0})` },
          { id: 'active', label: 'Actifs FB' },
          { id: 'no_fb', label: 'Pas sur FB' },
          { id: 'faux_vendu', label: 'Faux Vendus' },
          { id: 'sans_photo', label: 'Sans Photo' },
          { id: 'sold', label: 'Vendus' },
        ].map(f => (
          <button
            key={f.id}
            data-testid={`filter-${f.id}`}
            onClick={() => setFilter(f.id)}
            style={{
              padding: '4px 12px', borderRadius: '16px', border: 'none', cursor: 'pointer', fontSize: '0.8rem', fontWeight: 600,
              backgroundColor: filter === f.id ? '#3b82f6' : '#1f2937',
              color: filter === f.id ? '#fff' : '#9ca3af',
            }}
          >
            {f.label}
          </button>
        ))}
        <input
          data-testid="compare-search"
          type="text"
          placeholder="Chercher stock ou titre..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{ marginLeft: 'auto', padding: '6px 12px', borderRadius: '6px', border: '1px solid #374151', backgroundColor: '#111827', color: '#e5e7eb', fontSize: '0.85rem', width: '220px' }}
        />
        <button
          data-testid="compare-refresh"
          onClick={fetchCompare}
          style={{ padding: '6px 12px', borderRadius: '6px', border: '1px solid #374151', backgroundColor: '#1f2937', color: '#9ca3af', cursor: 'pointer', fontSize: '0.8rem' }}
        >
          {loading ? '...' : '🔄'}
        </button>
        {lastRefresh && <span style={{ fontSize: '0.7rem', color: '#6b7280' }}>MAJ: {lastRefresh}</span>}
      </div>

      {/* Table */}
      <div style={{ overflowX: 'auto', borderRadius: '8px', border: '1px solid #1f2937' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }} data-testid="compare-table">
          <thead>
            <tr style={{ backgroundColor: '#111827' }}>
              {['Stock', 'Véhicule', 'Prix', 'Kennebec', 'Facebook', 'Photos FB', 'Publié le', 'Problème'].map(h => (
                <th key={h} style={{ padding: '10px 12px', textAlign: 'left', color: '#9ca3af', fontWeight: 600, borderBottom: '1px solid #1f2937', whiteSpace: 'nowrap' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map(v => (
              <tr key={v.stock} style={{ borderBottom: '1px solid #1f293766', backgroundColor: v.problem ? '#1a0a0a' : 'transparent' }}>
                <td style={{ padding: '8px 12px', fontFamily: 'monospace', fontWeight: 700, color: '#e5e7eb' }}>{v.stock}</td>
                <td style={{ padding: '8px 12px', color: '#d1d5db', maxWidth: '280px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{v.title}</td>
                <td style={{ padding: '8px 12px', color: '#22c55e', fontWeight: 600, whiteSpace: 'nowrap' }}>{fmtPrice(v.price)}</td>
                <td style={{ padding: '8px 12px' }}>{statusBadge(v.kennebec_status)}</td>
                <td style={{ padding: '8px 12px' }}>{statusBadge(v.fb_status)}</td>
                <td style={{ padding: '8px 12px', textAlign: 'center', color: v.fb_no_photo ? '#fbbf24' : '#6ee7b7' }}>
                  {v.fb_no_photo ? '📷 0' : v.fb_photos > 0 ? `✅ ${v.fb_photos}` : '—'}
                </td>
                <td style={{ padding: '8px 12px', color: '#9ca3af', whiteSpace: 'nowrap' }}>{fmtDate(v.fb_published)}</td>
                <td style={{ padding: '8px 12px' }}>{problemBadge(v.problem)}</td>
              </tr>
            ))}
            {filtered.length === 0 && (
              <tr><td colSpan={8} style={{ padding: '2rem', textAlign: 'center', color: '#6b7280' }}>Aucun véhicule trouvé</td></tr>
            )}
          </tbody>
        </table>
      </div>
      <div style={{ marginTop: '0.5rem', fontSize: '0.7rem', color: '#6b7280' }}>
        {filtered.length} véhicules affichés sur {vehicles.length} total — Auto-refresh 2 min
      </div>
    </div>
  );
}


// ═══════════════════════════════════════════════════
// REPRISE TAB — Vehicle Appraisal Form
// ═══════════════════════════════════════════════════

const REPRISE_SECTIONS = ['client', 'vehicle', 'options', 'etat', 'photos', 'garanties', 'notes'];
const REPRISE_LABELS = { client: 'Client', vehicle: 'Vehicule', options: 'Options', etat: 'Etat', photos: 'Photos', garanties: 'Garanties', notes: 'Notes' };
const TYPES_TRANSACTION = ['Achat', 'Echange', 'Location'];
const INTERET_CLIENT = ['Neuf', 'Usage', 'Les deux'];
const PROVENANCES = ['Walk-in', 'Telephone', 'Site web', 'Facebook', 'Messenger', 'Reference', 'Autre'];
const ETATS_GENERAL = [
  { label: 'Mauvais', color: '#ef4444', value: 1 },
  { label: 'Faible', color: '#f97316', value: 2 },
  { label: 'Moyen', color: '#eab308', value: 3 },
  { label: 'Bon', color: '#84cc16', value: 4 },
  { label: 'Excellent', color: '#22c55e', value: 5 },
];
const ETATS_PAREBRISE = ['Bon etat', 'Eclat mineur', 'Fissure', 'A remplacer'];
const COULEURS_EXT = ['Blanc', 'Noir', 'Gris', 'Argent', 'Rouge', 'Bleu', 'Vert', 'Brun', 'Beige', 'Orange', 'Jaune', 'Autre'];
const COULEURS_INT = ['Noir', 'Gris', 'Beige', 'Brun', 'Rouge', 'Autre'];
const VEHICLE_OPTIONS = [
  { cat: 'Confort', items: ['Sieges chauffants', 'Sieges ventiles', 'Sieges en cuir', 'Volant chauffant', 'Toit ouvrant / panoramique', 'Climatisation auto 2 zones', 'Demarreur a distance'] },
  { cat: 'Technologie', items: ['Navigation GPS', 'Apple CarPlay', 'Android Auto', 'Camera de recul', 'Camera 360', 'Affichage tete haute', 'Chargeur sans fil', 'Audio premium'] },
  { cat: 'Securite', items: ['Detection angle mort', 'Maintien de voie', 'Freinage d\'urgence auto', 'Regulateur adaptatif', 'Capteurs stationnement', 'Phares LED'] },
  { cat: 'Performance', items: ['4x4 / AWD', 'Mode remorquage', 'Suspension adaptative', 'Turbo / Suralimente'] },
  { cat: 'Exterieur', items: ['Marchepieds', 'Barres de toit', 'Attelage remorquage', 'Roues alliage 18+', 'Vitres teintees'] },
];
const ZONES_DOMMAGES = ['Pare-chocs avant', 'Aile avant G', 'Aile avant D', 'Portiere avant G', 'Portiere avant D', 'Portiere arriere G', 'Portiere arriere D', 'Aile arriere G', 'Aile arriere D', 'Pare-chocs arriere', 'Toit', 'Capot', 'Coffre/Hayon'];

function RepriseTab({ standalone, user }) {
  const [sec, setSec] = useState('client');
  const [vinSpecs, setVinSpecs] = useState(null);
  const [vinLoading, setVinLoading] = useState(false);
  const [vinError, setVinError] = useState('');
  const [vinScanning, setVinScanning] = useState(false);
  const [trimOverride, setTrimOverride] = useState('');
  const [vinNhtsaTrims, setVinNhtsaTrims] = useState([]);
  const [photos, setPhotos] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState({}); // { idx: 0-100 }
  const [submitted, setSubmitted] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [evalId] = useState(() => Math.random().toString(36).slice(2));
  const [form, setForm] = useState({
    prenom: '', nom: '', telephone: '', courriel: '',
    type_transaction: '', solde_du: false, solde_montant: '', institution: '', versement: '', frequence_versement: '', interet: '', provenance: '', notes_client: '',
    vin: '', km: '', couleur_ext: '', couleur_int: '', nombre_cles: '1',
    options: [], etat_general: 3, etat_parebrise: 'Bon etat', etat_mecanique: '', dommages: [],
    garantie_constructeur: false, garantie_constructeur_date: '', garantie_prolongee: false, garantie_prolongee_detail: '', garantie_prolongee_date: '', garantie_prolongee_fournisseur: '',
    commentaires: '', etat_commentaire: '',
  });
  const up = (k, v) => setForm(f => ({ ...f, [k]: v }));
  const togOpt = o => setForm(f => ({ ...f, options: f.options.includes(o) ? f.options.filter(x => x !== o) : [...f.options, o] }));
  const togDmg = z => setForm(f => ({ ...f, dommages: f.dommages.includes(z) ? f.dommages.filter(x => x !== z) : [...f.dommages, z] }));
  const secIdx = REPRISE_SECTIONS.indexOf(sec);
  const goNext = () => { if (secIdx < REPRISE_SECTIONS.length - 1) setSec(REPRISE_SECTIONS[secIdx + 1]); };
  const goPrev = () => { if (secIdx > 0) setSec(REPRISE_SECTIONS[secIdx - 1]); };

  const decodeVin = async () => {
    const v = form.vin.trim().toUpperCase();
    if (v.length !== 17) { setVinError('VIN: 17 caracteres'); return; }
    setVinLoading(true); setVinError(''); setTrimOverride('');
    try {
      const r = await fetch(`${API}/api/vin/${v}`);
      if (!r.ok) throw new Error('VIN non trouve');
      const data = await r.json();
      setVinSpecs(data.specs);
      const rawTrim = data.specs?.trim || '';
      if (rawTrim.includes(',') || rawTrim.includes('/')) {
        setVinNhtsaTrims(rawTrim.split(/[,/]/).map(t => t.trim()).filter(Boolean));
      } else { setVinNhtsaTrims([]); }
    } catch (e) { setVinError(e.message); }
    setVinLoading(false);
  };

  const handleVinScan = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    setVinScanning(true); setVinError(''); setTrimOverride('');
    const fd = new FormData();
    fd.append('file', file);
    try {
      const r = await fetch(`${API}/api/vin/scan-photo`, { method: 'POST', body: fd });
      const data = await r.json();
      if (data.success && data.vin) {
        up('vin', data.vin);
        if (data.specs) {
          setVinSpecs(data.specs);
          const rawTrim = data.specs?.trim || '';
          if (rawTrim.includes(',') || rawTrim.includes('/')) {
            setVinNhtsaTrims(rawTrim.split(/[,/]/).map(t => t.trim()).filter(Boolean));
          } else { setVinNhtsaTrims([]); }
        }
      } else {
        setVinError(data.error || 'VIN non detecte');
        if (data.partial) up('vin', data.partial);
      }
    } catch (err) { setVinError('Erreur scan'); }
    setVinScanning(false);
    e.target.value = '';
  };

  const compressImage = (file, maxW = 1200, q = 0.7) => new Promise((res) => {
    const img = new Image();
    img.onload = () => { const c = document.createElement('canvas'); let w = img.width, h = img.height; if (w > maxW) { h = (h * maxW) / w; w = maxW; } c.width = w; c.height = h; c.getContext('2d').drawImage(img, 0, 0, w, h); c.toBlob(b => res(b), 'image/jpeg', q); };
    img.src = URL.createObjectURL(file);
  });

  const handlePhotos = async (e) => {
    const files = Array.from(e.target.files).slice(0, 10 - photos.length);
    if (!files.length) return;
    setUploading(true);
    for (let fi = 0; fi < files.length; fi++) {
      const file = files[fi];
      const progressKey = `up_${Date.now()}_${fi}`;
      setUploadProgress(p => ({ ...p, [progressKey]: 0 }));
      const compressed = await compressImage(file);
      setUploadProgress(p => ({ ...p, [progressKey]: 30 }));
      const fd = new FormData(); fd.append('file', new File([compressed], file.name.replace(/\.[^.]+$/, '.jpg'), { type: 'image/jpeg' })); fd.append('evaluation_id', evalId);
      try {
        setUploadProgress(p => ({ ...p, [progressKey]: 60 }));
        const r = await fetch(`${API}/api/evaluations/upload-photo`, { method: 'POST', body: fd });
        if (r.ok) {
          const d = await r.json();
          setUploadProgress(p => ({ ...p, [progressKey]: 100 }));
          setPhotos(p => [...p, { url: d.url, name: file.name }]);
        }
      } catch (err) { console.error(err); }
      setTimeout(() => setUploadProgress(p => { const n = { ...p }; delete n[progressKey]; return n; }), 800);
    }
    setUploading(false);
  };

  const handleSubmit = async () => {
    if (!form.prenom || !form.nom || !form.telephone) return;
    setSubmitting(true);
    try {
      const payload = { ...form, vin: form.vin.trim().toUpperCase(), photos: photos.map(p => p.url),
        km: form.km ? parseInt(form.km.replace(/\D/g, '')) : null,
        paiement_restant: form.solde_montant ? parseFloat(form.solde_montant.replace(/\D/g, '')) : null,
        etat_general: ETATS_GENERAL.find(e => e.value === form.etat_general)?.label || '',
        created_by: user?.username || 'client',
      };
      const r = await fetch(`${API}/api/evaluations`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      if (r.ok) setSubmitted(true);
    } catch (e) { console.error(e); }
    setSubmitting(false);
  };

  const canSubmit = form.prenom && form.nom && form.telephone;

  const rs = { // reprise styles
    card: { background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '8px', padding: '1.25rem', marginBottom: '1rem' },
    cardTitle: { fontFamily: 'Chivo', fontWeight: 700, fontSize: '0.95rem', marginBottom: '0.75rem' },
    row: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '0.75rem' },
    label: { display: 'block', fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '4px' },
    input: { width: '100%', padding: '10px 12px', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.9rem', fontFamily: 'IBM Plex Sans', background: 'var(--surface)' },
    chip: (on) => ({ display: 'inline-block', padding: '8px 14px', borderRadius: '6px', fontSize: '0.8rem', fontWeight: 600, cursor: 'pointer', border: on ? '2px solid var(--accent-blue)' : '1px solid var(--border)', background: on ? 'var(--accent-blue-subtle)' : 'var(--surface)', color: on ? 'var(--accent-blue)' : 'var(--text-secondary)', margin: '3px' }),
    dmgChip: (on) => ({ display: 'inline-block', padding: '6px 12px', borderRadius: '6px', fontSize: '0.75rem', fontWeight: 600, cursor: 'pointer', border: on ? '2px solid var(--accent-red)' : '1px solid var(--border)', background: on ? 'var(--accent-red-subtle)' : 'var(--surface)', color: on ? 'var(--accent-red)' : 'var(--text-secondary)', margin: '3px' }),
    etatBar: { display: 'flex', borderRadius: '6px', overflow: 'hidden', border: '1px solid var(--border)' },
    etatSeg: (c, on) => ({ flex: 1, padding: '10px 0', textAlign: 'center', cursor: 'pointer', fontSize: '0.75rem', fontWeight: 700, background: on ? c : 'var(--surface)', color: on ? '#fff' : 'var(--text-secondary)', transition: 'all 0.15s' }),
    navBar: { display: 'flex', gap: '0', overflowX: 'auto', borderBottom: '2px solid var(--border)', marginBottom: '1.25rem' },
    navBtn: (on) => ({ padding: '10px 16px', fontSize: '0.78rem', fontWeight: 600, cursor: 'pointer', border: 'none', borderBottom: on ? '2px solid var(--accent-blue)' : '2px solid transparent', background: 'none', color: on ? 'var(--accent-blue)' : 'var(--text-secondary)', whiteSpace: 'nowrap', fontFamily: 'IBM Plex Sans' }),
    bottomBar: { display: 'flex', gap: '0.5rem', marginTop: '1.5rem', justifyContent: 'flex-end' },
    btnPrimary: { padding: '10px 24px', borderRadius: '6px', border: 'none', fontSize: '0.85rem', fontWeight: 700, cursor: 'pointer', background: 'var(--accent-blue)', color: '#fff' },
    btnSecondary: { padding: '10px 20px', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.85rem', fontWeight: 600, cursor: 'pointer', background: 'var(--surface)', color: 'var(--text-secondary)' },
  };

  if (submitted) {
    return (
      <div style={{ textAlign: 'center', padding: '4rem 1rem' }}>
        <img src="/kennebec-logo.png" alt="Kennebec" style={{ height: '40px', marginBottom: '1.5rem', opacity: 0.8 }} />
        <div style={{ fontSize: '2.5rem', marginBottom: '1rem', color: 'var(--accent-green)' }}>&#10003;</div>
        <h2 style={{ marginBottom: '0.5rem' }}>Demande envoyee!</h2>
        <p style={{ color: 'var(--text-secondary)' }}>Merci {form.prenom}! Notre equipe va analyser votre vehicule et vous contacter rapidement.</p>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginTop: '1rem' }}>Kennebec Dodge Chrysler — 418-222-3939</p>
        {!standalone && <button style={{ ...rs.btnPrimary, marginTop: '1.5rem' }} onClick={() => window.location.reload()}>Nouvelle evaluation</button>}
      </div>
    );
  }

  return (
    <div style={{ padding: '0' }} data-testid="reprise-tab">
      {!standalone && (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
          <h2 className="section-title" style={{ margin: 0 }}>Evaluation de reprise</h2>
          <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>{secIdx + 1}/{REPRISE_SECTIONS.length}</span>
        </div>
      )}
      {standalone && (
        <div style={{ marginBottom: '1rem' }}>
          <h2 style={{ fontFamily: 'Chivo', fontWeight: 700, fontSize: '1.25rem', marginBottom: '0.25rem' }}>Evaluation de votre vehicule</h2>
          <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>Remplissez le formulaire pour obtenir une estimation de reprise. Etape {secIdx + 1} de {REPRISE_SECTIONS.length}.</p>
        </div>
      )}
      {/* ALL SECTIONS SCROLLABLE */}

      {/* 1. CLIENT */}
      <div style={rs.card}><div style={rs.cardTitle}>1. Vos coordonnees</div>
        <div style={rs.row}>
          <div><label style={rs.label}>Votre prenom *</label><input style={rs.input} value={form.prenom} onChange={e => up('prenom', e.target.value)} data-testid="reprise-prenom" placeholder="Prenom" /></div>
          <div><label style={rs.label}>Votre nom *</label><input style={rs.input} value={form.nom} onChange={e => up('nom', e.target.value)} data-testid="reprise-nom" placeholder="Nom" /></div>
        </div>
        <div style={{ ...rs.row, marginTop: '0.75rem' }}>
          <div><label style={rs.label}>Votre telephone *</label><input style={rs.input} type="tel" value={form.telephone} onChange={e => up('telephone', e.target.value)} data-testid="reprise-tel" placeholder="418-555-1234" /></div>
          <div><label style={rs.label}>Votre courriel</label><input style={rs.input} type="email" value={form.courriel} onChange={e => up('courriel', e.target.value)} placeholder="email@exemple.com" /></div>
        </div>
      </div>
      <div style={rs.card}><div style={rs.cardTitle}>Votre projet</div>
        <div style={rs.row}>
          <div><label style={rs.label}>Vous desirez</label><select style={rs.input} value={form.type_transaction} onChange={e => up('type_transaction', e.target.value)}><option value="">—</option>{TYPES_TRANSACTION.map(t => <option key={t}>{t}</option>)}</select></div>
          <div><label style={rs.label}>Vous recherchez</label><select style={rs.input} value={form.interet} onChange={e => up('interet', e.target.value)}><option value="">—</option>{INTERET_CLIENT.map(t => <option key={t}>{t}</option>)}</select></div>
          <div><label style={rs.label}>Comment vous nous avez connu?</label><select style={rs.input} value={form.provenance} onChange={e => up('provenance', e.target.value)}><option value="">—</option>{PROVENANCES.map(p => <option key={p}>{p}</option>)}</select></div>
        </div>
      </div>
      <div style={rs.card}><div style={rs.cardTitle}>Votre financement actuel</div>
        <div><label style={rs.label}>Avez-vous un solde a payer sur votre vehicule?</label>
          <div style={{ display: 'flex', gap: '0.75rem', marginTop: '0.4rem' }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', cursor: 'pointer', fontSize: '0.9rem' }}><input type="radio" name="solde" checked={form.solde_du === true} onChange={() => up('solde_du', true)} /> Oui</label>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', cursor: 'pointer', fontSize: '0.9rem' }}><input type="radio" name="solde" checked={form.solde_du === false} onChange={() => up('solde_du', false)} /> Non</label>
          </div>
        </div>
        {form.solde_du === true && (
          <>
            <div style={{ ...rs.row, marginTop: '0.75rem' }}>
              <div><label style={rs.label}>Balance due (montant restant)</label><input style={rs.input} value={form.solde_montant} onChange={e => up('solde_montant', e.target.value)} placeholder="Ex: 12 000 $" /></div>
              <div><label style={rs.label}>Avec quelle institution financiere?</label><input style={rs.input} value={form.institution} onChange={e => up('institution', e.target.value)} placeholder="Ex: Desjardins, TD, RBC..." /></div>
            </div>
            <div style={{ ...rs.row, marginTop: '0.75rem' }}>
              <div><label style={rs.label}>Votre versement</label><input style={rs.input} value={form.versement} onChange={e => up('versement', e.target.value)} placeholder="Ex: 350 $" /></div>
              <div><label style={rs.label}>Frequence</label><select style={rs.input} value={form.frequence_versement} onChange={e => up('frequence_versement', e.target.value)}><option value="">—</option><option value="semaine">Par semaine</option><option value="2semaines">Aux 2 semaines</option><option value="mois">Par mois</option></select></div>
            </div>
          </>
        )}
      </div>

      {/* 2. VEHICULE */}
      <div style={rs.card}><div style={rs.cardTitle}>2. Votre vehicule</div>
        <label style={rs.label}>Numero de serie (VIN)</label>
        <p style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginBottom: '0.5rem', marginTop: 0 }}>Entrez ou scannez le numero de serie de votre vehicule (17 caracteres)</p>
        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
          <input style={{ ...rs.input, flex: 1, minWidth: '160px', fontFamily: 'IBM Plex Mono', textTransform: 'uppercase' }} value={form.vin} onChange={e => { up('vin', e.target.value.toUpperCase().slice(0, 17)); setVinError(''); }} maxLength={17} placeholder="Ex: 1C6SRFTT7MN517688" data-testid="reprise-vin" />
          <input type="file" accept="image/*" capture="environment" onChange={handleVinScan} style={{ display: 'none' }} id="vin-scan-input" />
          <label htmlFor="vin-scan-input" style={{ ...rs.btnSecondary, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '0.4rem', cursor: vinScanning ? 'wait' : 'pointer', opacity: vinScanning ? 0.6 : 1, whiteSpace: 'nowrap' }} data-testid="vin-scan-btn">{vinScanning ? 'Lecture...' : 'Scanner'}</label>
          <button style={rs.btnPrimary} onClick={decodeVin} disabled={vinLoading} data-testid="reprise-decode">{vinLoading ? '...' : 'Decoder'}</button>
        </div>
        <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: '4px' }}>{form.vin.length}/17</div>
        {vinScanning && <div style={{ color: 'var(--accent-blue)', fontSize: '0.8rem', marginTop: '0.5rem' }}>Analyse de la photo en cours...</div>}
        {vinError && <div style={{ color: 'var(--accent-red)', fontSize: '0.8rem', marginTop: '0.5rem' }}>{vinError}</div>}
        {vinSpecs && (
          <div style={{ marginTop: '1rem', padding: '1rem', background: 'var(--surface-secondary)', borderRadius: '6px', border: '1px solid var(--border)' }}>
            <div style={rs.row}>
              {[['Marque', vinSpecs.make], ['Modele', vinSpecs.model], ['Annee', vinSpecs.year], ['Moteur', `${vinSpecs.engine_cylinders||''}cyl ${vinSpecs.engine_displacement||''}L ${vinSpecs.engine_hp||''}HP`.trim()], ['Motricite', vinSpecs.drive_type]].filter(([,v]) => v && v !== 'cyl LHP').map(([l,v]) => (
                <div key={l} style={{ padding: '6px 0', borderBottom: '1px solid var(--border)' }}><span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{l}</span><br/><span style={{ fontWeight: 600, fontSize: '0.9rem' }}>{v}</span></div>
              ))}
              <div style={{ padding: '6px 0', borderBottom: '1px solid var(--border)' }}>
                <span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>Trim</span><br/>
                <span style={{ fontWeight: 600, fontSize: '0.9rem' }}>{trimOverride || vinSpecs.trim || '—'}</span>
                <div style={{ marginTop: '0.5rem' }}>
                  <select style={{ ...rs.input, fontSize: '0.8rem', padding: '6px 10px' }} value={trimOverride || vinSpecs.trim || ''} onChange={e => setTrimOverride(e.target.value)}>
                    <option value={vinSpecs.trim || ''}>{vinSpecs.trim || '—'} (detecte)</option>
                    {vinNhtsaTrims.filter(t => t !== vinSpecs.trim).map(t => <option key={t} value={t}>{t}</option>)}
                    <option value="__custom">Autre...</option>
                  </select>
                  {trimOverride === '__custom' && (
                    <input style={{ ...rs.input, fontSize: '0.8rem', padding: '6px 10px', marginTop: '0.4rem' }} placeholder="Entrer le trim exact" onChange={e => { if (e.target.value) setTrimOverride(e.target.value); }} autoFocus />
                  )}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
      <div style={rs.card}><div style={rs.cardTitle}>Informations supplementaires</div>
        <div style={rs.row}>
          <div><label style={rs.label}>Kilometrage actuel</label><input style={rs.input} value={form.km} onChange={e => up('km', e.target.value)} data-testid="reprise-km" placeholder="Ex: 85 000" /></div>
          <div><label style={rs.label}>Combien de cles avez-vous?</label><select style={rs.input} value={form.nombre_cles} onChange={e => up('nombre_cles', e.target.value)}>{['0','1','2','3+'].map(n => <option key={n}>{n}</option>)}</select></div>
          <div><label style={rs.label}>Couleur exterieure</label><select style={rs.input} value={form.couleur_ext} onChange={e => up('couleur_ext', e.target.value)}><option value="">—</option>{COULEURS_EXT.map(c => <option key={c}>{c}</option>)}</select></div>
          <div><label style={rs.label}>Couleur interieure</label><select style={rs.input} value={form.couleur_int} onChange={e => up('couleur_int', e.target.value)}><option value="">—</option>{COULEURS_INT.map(c => <option key={c}>{c}</option>)}</select></div>
        </div>
      </div>

      {/* 3. OPTIONS */}
      <div style={rs.card}><div style={rs.cardTitle}>3. Equipements de votre vehicule <span style={{ fontWeight: 400, color: 'var(--text-secondary)', fontSize: '0.8rem' }}>({form.options.length} selectionnees)</span></div>
        <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginBottom: '0.75rem' }}>Cochez les options presentes sur votre vehicule</p>
        {VEHICLE_OPTIONS.map(cat => (
          <div key={cat.cat} style={{ marginBottom: '0.75rem' }}>
            <div style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--accent-blue)', marginBottom: '0.4rem' }}>{cat.cat}</div>
            <div>{cat.items.map(o => <span key={o} style={rs.chip(form.options.includes(o))} onClick={() => togOpt(o)}>{form.options.includes(o) ? '✓ ' : ''}{o}</span>)}</div>
          </div>
        ))}
      </div>

      {/* 4. ETAT */}
      <div style={rs.card}><div style={rs.cardTitle}>4. Decrivez l'etat de votre vehicule</div>
        <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginBottom: '0.75rem', marginTop: 0 }}>Selectionnez l'etat general de votre vehicule</p>
        <div style={rs.etatBar}>{ETATS_GENERAL.map(e => <div key={e.value} style={rs.etatSeg(e.color, form.etat_general >= e.value)} onClick={() => up('etat_general', e.value)} data-testid={`reprise-etat-${e.value}`}>{e.label}</div>)}</div>
        <div style={{ marginTop: '0.75rem' }}>
          <label style={rs.label}>Commentaires sur l'etat (optionnel)</label>
          <textarea style={{ ...rs.input, minHeight: '70px', resize: 'vertical' }} value={form.etat_commentaire || ''} onChange={e => up('etat_commentaire', e.target.value)} placeholder="Decrivez l'etat general de votre vehicule, tout detail pertinent..." />
        </div>
      </div>
      <div style={rs.card}><div style={rs.cardTitle}>Etat du pare-brise</div>
        <div>{ETATS_PAREBRISE.map(e => <span key={e} style={rs.chip(form.etat_parebrise === e)} onClick={() => up('etat_parebrise', e)}>{e}</span>)}</div>
      </div>
      <div style={rs.card}><div style={rs.cardTitle}>Y a-t-il des dommages visibles?</div>
        <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginBottom: '0.5rem' }}>Selectionnez les zones concernees</p>
        <div>{ZONES_DOMMAGES.map(z => <span key={z} style={rs.dmgChip(form.dommages.includes(z))} onClick={() => togDmg(z)}>{form.dommages.includes(z) ? '✕ ' : ''}{z}</span>)}</div>
      </div>
      <div style={rs.card}><div style={rs.cardTitle}>Y a-t-il des problemes mecaniques?</div>
        <textarea style={{ ...rs.input, minHeight: '80px', resize: 'vertical' }} value={form.etat_mecanique} onChange={e => up('etat_mecanique', e.target.value)} placeholder="Bruits, voyants allumes, entretien a faire..." />
      </div>

      {/* 5. PHOTOS */}
      <div style={rs.card}><div style={rs.cardTitle}>5. Photos de votre vehicule ({photos.length}/10)</div>
        <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginBottom: '0.75rem' }}>Prenez des photos des 4 cotes, de l'interieur, du tableau de bord et de tout defaut visible</p>
        <input type="file" accept="image/*" multiple onChange={handlePhotos} style={{ display: 'none' }} id="reprise-photo-input" />
        <label htmlFor="reprise-photo-input" style={{ ...rs.btnSecondary, display: 'block', textAlign: 'center', cursor: 'pointer', padding: '1.5rem' }}>{uploading ? 'Envoi en cours...' : 'Ajouter des photos'}</label>
        {/* Upload progress bars */}
        {Object.entries(uploadProgress).length > 0 && (
          <div style={{ marginTop: '0.75rem', display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {Object.entries(uploadProgress).map(([key, pct]) => (
              <div key={key} style={{ background: 'var(--surface-secondary, #1a1a2e)', borderRadius: '4px', overflow: 'hidden', height: '22px', position: 'relative' }}>
                <div style={{ width: `${pct}%`, height: '100%', background: pct >= 100 ? '#22c55e' : '#0ea5e9', transition: 'width 0.3s ease', borderRadius: '4px' }} />
                <span style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.65rem', fontWeight: 700, color: '#fff' }}>{pct >= 100 ? 'Termine!' : `${pct}%`}</span>
              </div>
            ))}
          </div>
        )}
        {photos.length > 0 && (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(80px, 1fr))', gap: '0.5rem', marginTop: '0.75rem' }}>
            {photos.map((p, i) => <div key={i} style={{ position: 'relative' }}><img src={p.url} alt="" style={{ width: '100%', aspectRatio: '1', objectFit: 'cover', borderRadius: '6px', border: '1px solid var(--border)' }} /><button onClick={() => setPhotos(pr => pr.filter((_, j) => j !== i))} style={{ position: 'absolute', top: 2, right: 2, background: 'var(--accent-red)', color: '#fff', border: 'none', borderRadius: '50%', width: 20, height: 20, fontSize: '0.6rem', cursor: 'pointer' }}>x</button></div>)}
          </div>
        )}
      </div>

      {/* 6. GARANTIES */}
      <div style={rs.card}><div style={rs.cardTitle}>6. Garanties</div>
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer', marginBottom: '0.5rem' }}><input type="checkbox" checked={form.garantie_constructeur} onChange={e => up('garantie_constructeur', e.target.checked)} /> La garantie du fabricant est-elle encore valide?</label>
        {form.garantie_constructeur && <div style={{ marginBottom: '0.75rem', marginLeft: '1.5rem' }}><label style={rs.label}>Date d'expiration de la garantie fabricant</label><input style={rs.input} type="date" value={form.garantie_constructeur_date} onChange={e => up('garantie_constructeur_date', e.target.value)} /></div>}
        <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}><input type="checkbox" checked={form.garantie_prolongee} onChange={e => up('garantie_prolongee', e.target.checked)} /> Avez-vous une garantie prolongee?</label>
        {form.garantie_prolongee && (
          <div style={{ marginTop: '0.5rem', marginLeft: '1.5rem', padding: '0.75rem', background: 'var(--surface-secondary, #1a1a2e)', borderRadius: '6px', border: '1px solid var(--border)' }}>
            <div style={rs.row}>
              <div><label style={rs.label}>Prolongee jusqu'a quelle date?</label><input style={rs.input} type="date" value={form.garantie_prolongee_date || ''} onChange={e => up('garantie_prolongee_date', e.target.value)} /></div>
              <div><label style={rs.label}>Fournisseur de la garantie</label><input style={rs.input} value={form.garantie_prolongee_fournisseur || ''} onChange={e => up('garantie_prolongee_fournisseur', e.target.value)} placeholder="Ex: Sym-Tech, Global Warranty..." /></div>
            </div>
            <div style={{ marginTop: '0.5rem' }}><label style={rs.label}>Details / couverture</label><textarea style={{ ...rs.input, minHeight: '60px' }} value={form.garantie_prolongee_detail} onChange={e => up('garantie_prolongee_detail', e.target.value)} placeholder="Quel type de couverture, quelles exclusions..." /></div>
          </div>
        )}
      </div>

      {/* 7. NOTES */}
      <div style={rs.card}><div style={rs.cardTitle}>7. Autre chose a nous mentionner?</div>
        <textarea style={{ ...rs.input, minHeight: '120px', resize: 'vertical' }} value={form.commentaires} onChange={e => up('commentaires', e.target.value)} placeholder="Accidents, reparations recentes, raison de la vente, ou tout autre detail important..." data-testid="reprise-commentaires" />
      </div>

      {/* SUBMIT */}
      <div style={rs.bottomBar}>
        <button style={{ ...rs.btnPrimary, opacity: canSubmit && !submitting ? 1 : 0.5, padding: '14px 32px', fontSize: '1rem' }} onClick={handleSubmit} disabled={!canSubmit || submitting} data-testid="reprise-submit">
          {submitting ? 'Envoi en cours...' : 'Envoyer ma demande'}
        </button>
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════
// EVALUATIONS TAB — Admin list + detail
// ═══════════════════════════════════════════════════

const EVAL_STATUTS = ['EN ATTENTE', 'PRIX RECU', 'REPRIS', 'REFUSE'];
const EVAL_COLORS = { 'EN ATTENTE': '#eab308', 'PRIX RECU': '#0ea5e9', 'REPRIS': '#22c55e', 'REFUSE': '#ef4444', 'NOUVEAU': '#3b82f6' };

// Brand logos mapping
const BRAND_LOGOS = {
  'JEEP': 'https://www.carlogos.org/car-logos/jeep-logo.png',
  'DODGE': 'https://www.carlogos.org/car-logos/dodge-logo.png',
  'CHRYSLER': 'https://www.carlogos.org/car-logos/chrysler-logo.png',
  'RAM': 'https://www.carlogos.org/car-logos/ram-logo.png',
  'VOLKSWAGEN': 'https://www.carlogos.org/car-logos/volkswagen-logo.png',
  'TOYOTA': 'https://www.carlogos.org/car-logos/toyota-logo.png',
  'HONDA': 'https://www.carlogos.org/car-logos/honda-logo.png',
  'FORD': 'https://www.carlogos.org/car-logos/ford-logo.png',
  'CHEVROLET': 'https://www.carlogos.org/car-logos/chevrolet-logo.png',
  'GMC': 'https://www.carlogos.org/car-logos/gmc-logo.png',
  'HYUNDAI': 'https://www.carlogos.org/car-logos/hyundai-logo.png',
  'KIA': 'https://www.carlogos.org/car-logos/kia-logo.png',
  'NISSAN': 'https://www.carlogos.org/car-logos/nissan-logo.png',
  'MAZDA': 'https://www.carlogos.org/car-logos/mazda-logo.png',
  'SUBARU': 'https://www.carlogos.org/car-logos/subaru-logo.png',
  'BMW': 'https://www.carlogos.org/car-logos/bmw-logo.png',
  'MERCEDES-BENZ': 'https://www.carlogos.org/car-logos/mercedes-benz-logo.png',
  'AUDI': 'https://www.carlogos.org/car-logos/audi-logo.png',
  'BUICK': 'https://www.carlogos.org/car-logos/buick-logo.png',
  'CADILLAC': 'https://www.carlogos.org/car-logos/cadillac-logo.png',
  'LINCOLN': 'https://www.carlogos.org/car-logos/lincoln-logo.png',
  'MITSUBISHI': 'https://www.carlogos.org/car-logos/mitsubishi-logo.png',
  'FIAT': 'https://www.carlogos.org/car-logos/fiat-logo.png',
};

function EvaluationsTab({ user }) {
  const [evals, setEvals] = useState([]);
  const [selected, setSelected] = useState(null);
  const [view, setView] = useState('list'); // list | stats
  const [filter, setFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [searchVin, setSearchVin] = useState('');
  const [searchPhone, setSearchPhone] = useState('');
  const [loading, setLoading] = useState(false);
  const [showPrix, setShowPrix] = useState(false);
  const [prixVal, setPrixVal] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const [users, setUsers] = useState([]);
  const [page, setPage] = useState(1);
  const [lightbox, setLightbox] = useState(null);
  const [showWholesale, setShowWholesale] = useState(null);
  const [wholesaleInline, setWholesaleInline] = useState(null); // eval id for inline wholesale
  const [wsContacts, setWsContacts] = useState([]);
  const [wsChecked, setWsChecked] = useState({});
  const [wsSending, setWsSending] = useState(false);
  const [wsSent, setWsSent] = useState({});
  const perPage = 20;

  // Fetch wholesale contacts once
  useEffect(() => {
    fetch(`${API}/api/wholesale-contacts`).then(r => r.json()).then(d => setWsContacts(d.contacts || [])).catch(() => {});
  }, []);

  const fetchEvals = useCallback(async () => {
    setLoading(true);
    try {
      let url = `${API}/api/evaluations`;
      const params = [];
      if (user?.role) params.push(`role=${user.role}`);
      if (user?.username) params.push(`created_by=${user.username}`);
      if (params.length) url += '?' + params.join('&');
      const r = await fetch(url); const d = await r.json(); setEvals(d.evaluations || []);
    } catch (e) { console.error(e); }
    setLoading(false);
  }, [user]);

  useEffect(() => { fetchEvals(); }, [fetchEvals]);
  useEffect(() => { fetch(`${API}/api/users`).then(r => r.json()).then(d => setUsers(d.users || [])).catch(() => {}); }, []);

  // Get directeur email for reply-to
  const directeur = users.find(u => u.role === 'directeur');
  const replyToEmail = directeur?.email || '';

  const toggleWholesaleInline = (evId) => {
    if (wholesaleInline === evId) { setWholesaleInline(null); setWsChecked({}); }
    else { setWholesaleInline(evId); setWsChecked({}); setWsSent({}); }
  };

  const sendWholesaleChecked = async (ev) => {
    const selected = wsContacts.filter(c => wsChecked[c.id || c.email] && c.email);
    if (selected.length === 0) return;
    setWsSending(true);
    for (const c of selected) {
      try {
        await fetch(`${API}/api/wholesale/send`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ evaluation_id: ev.id, contact_email: c.email, contact_name: c.name, reply_to: replyToEmail }) });
        setWsSent(s => ({ ...s, [c.id || c.email]: true }));
      } catch (e) { console.error(e); }
    }
    setWsSending(false);
  };

  const updateStatus = async (id, st) => {
    await fetch(`${API}/api/evaluations/${id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ status: st }) });
    fetchEvals(); if (selected?.id === id) setSelected(p => ({ ...p, status: st }));
  };

  const submitPrix = async () => {
    if (!prixVal || !selected) return;
    const cu = users.find(u => u.username === selected.created_by);
    await fetch(`${API}/api/evaluations/${selected.id}`, {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prix_reprise: parseFloat(prixVal.replace(/[^0-9.]/g, '')), prix_par: user?.name || 'Directeur', notify_email: cu?.email || '' })
    });
    setShowPrix(false); setPrixVal(''); fetchEvals();
    setSelected(p => ({ ...p, prix_reprise: parseFloat(prixVal.replace(/[^0-9.]/g, '')), prix_par: user?.name, status: 'PRIX RECU' }));
  };

  const statusCounts = {
    all: evals.length,
    'EN ATTENTE': evals.filter(e => e.status === 'EN ATTENTE' || e.status === 'NOUVEAU').length,
    'PRIX RECU': evals.filter(e => e.status === 'PRIX RECU').length,
    'REPRIS': evals.filter(e => e.status === 'REPRIS').length,
    'REFUSE': evals.filter(e => e.status === 'REFUSE').length,
  };
  const pctReprise = evals.length > 0 ? ((statusCounts['REPRIS'] / evals.length) * 100).toFixed(1) : '0';

  const filtered = evals.filter(e => {
    if (filter === 'EN ATTENTE') return e.status === 'EN ATTENTE' || e.status === 'NOUVEAU';
    if (filter !== 'all') return e.status === filter;
    return true;
  }).filter(e => {
    if (search) { const s = search.toLowerCase(); if (!(e.client_name||'').toLowerCase().includes(s)) return false; }
    if (searchVin) { if (!(e.vin||'').toLowerCase().includes(searchVin.toLowerCase())) return false; }
    if (searchPhone) { if (!(e.client_phone||'').includes(searchPhone)) return false; }
    return true;
  });

  const totalPages = Math.ceil(filtered.length / perPage);
  const paginated = filtered.slice((page - 1) * perPage, page * perPage);

  const getBrandLogo = (make) => BRAND_LOGOS[(make||'').toUpperCase()] || null;

  const evalBadge = (st) => {
    const colors = { 'EN ATTENTE': '#eab308', 'NOUVEAU': '#eab308', 'PRIX RECU': '#0ea5e9', 'REPRIS': '#22c55e', 'REFUSE': '#ef4444' };
    const c = colors[st] || '#6b7280';
    return <span style={{ padding: '3px 10px', borderRadius: '4px', fontSize: '0.65rem', fontWeight: 700, background: `${c}18`, color: c, border: `1px solid ${c}30`, textTransform: 'uppercase', letterSpacing: '0.03em' }}>{st === 'NOUVEAU' ? 'EN ATTENTE' : st}</span>;
  };

  // ── DETAIL VIEW ──
  if (selected) {
    const ev = selected; const fd = ev.form_data || {};
    const canSetPrice = user?.role === 'admin' || user?.role === 'directeur';
    const logo = getBrandLogo(ev.make);
    return (
      <div data-testid="eval-detail" className="eval-detail-view">
        <button onClick={() => setSelected(null)} className="eval-back-btn">&#8592; Retour a la liste</button>
        <div className="eval-detail-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            {logo && <img src={logo} alt={ev.make} style={{ height: '36px', objectFit: 'contain' }} onError={e => e.target.style.display='none'} />}
            <div>
              <h3 className="eval-detail-title">{ev.year} {ev.make} {ev.model} {ev.trim}</h3>
              {ev.vin && <div className="eval-detail-vin">{ev.vin}</div>}
            </div>
          </div>
          {evalBadge(ev.status)}
        </div>

        {ev.photos?.length > 0 && (
          <div className="eval-photos-hero" onClick={() => setLightbox({ photos: ev.photos, idx: 0 })} style={{ cursor: 'pointer' }}>
            <img src={ev.photos[0]} alt="" className="eval-hero-img" />
            <span className="eval-photo-count">Cliquez pour voir — {ev.photos.length} photos</span>
          </div>
        )}

        {ev.prix_reprise ? (
          <div className="eval-prix-box eval-prix-set">
            <div className="eval-prix-label">Prix de reprise</div>
            <div className="eval-prix-value">{Number(ev.prix_reprise).toLocaleString('fr-CA')} $</div>
            <div className="eval-prix-by">Evalue par {ev.prix_par}</div>
          </div>
        ) : canSetPrice && (
          <div className="eval-prix-box eval-prix-empty">
            {showPrix ? (
              <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', flexWrap: 'wrap' }}>
                <input className="eval-prix-input" placeholder="Montant $" value={prixVal} onChange={e => setPrixVal(e.target.value)} autoFocus onKeyDown={e => e.key === 'Enter' && submitPrix()} />
                <button className="eval-btn-green" onClick={submitPrix}>Confirmer</button>
                <button className="eval-btn-ghost" onClick={() => setShowPrix(false)}>Annuler</button>
              </div>
            ) : (
              <button className="eval-btn-primary" onClick={() => setShowPrix(true)}>Donner un prix de reprise</button>
            )}
          </div>
        )}

        <div className="eval-specs-grid">
          {[['Kilometrage', ev.km ? `${Number(ev.km).toLocaleString('fr-CA')} km` : null], ['Moteur', ev.engine], ['Motricite', ev.drive_type], ['Carburant', ev.fuel_type], ['Etat', ev.etat_general], ['Couleur ext.', fd.couleur_ext], ['Couleur int.', fd.couleur_int], ['Cles', fd.nombre_cles], ['Pare-brise', fd.etat_parebrise]].filter(([,v]) => v).map(([l,v]) => (
            <div key={l} className="eval-spec-card"><div className="eval-spec-label">{l}</div><div className="eval-spec-value">{v}</div></div>
          ))}
        </div>

        {ev.photos?.length > 1 && <div className="eval-section"><div className="eval-section-title">Photos ({ev.photos.length})</div><div className="eval-photos-grid">{ev.photos.map((u,i) => <img key={i} src={u} alt="" className="eval-photo-thumb" onClick={() => setLightbox({ photos: ev.photos, idx: i })} />)}</div></div>}
        {fd.options?.length > 0 && <div className="eval-section"><div className="eval-section-title">Equipements ({fd.options.length})</div><div className="eval-tags">{fd.options.map(o => <span key={o} className="eval-tag-blue">{o}</span>)}</div></div>}
        {fd.dommages?.length > 0 && <div className="eval-section"><div className="eval-section-title">Dommages ({fd.dommages.length})</div><div className="eval-tags">{fd.dommages.map(d => <span key={d} className="eval-tag-red">{d}</span>)}</div></div>}

        <div className="eval-section eval-client-box">
          <div className="eval-section-title">Client</div>
          <div className="eval-client-name">{ev.client_name}</div>
          <a href={`tel:${ev.client_phone}`} className="eval-client-phone">{ev.client_phone}</a>
          {ev.client_email && <div className="eval-client-email">{ev.client_email}</div>}
          {fd.institution && <div className="eval-client-finance">Financement: {fd.institution} — {fd.versement} / {fd.frequence_versement}</div>}
          {fd.provenance && <div className="eval-client-source">Source: {fd.provenance}</div>}
        </div>

        {fd.commentaires && <div className="eval-section"><div className="eval-section-title">Notes</div><p className="eval-notes">{fd.commentaires}</p></div>}

        <div className="eval-section">
          <div className="eval-section-title">Changer le statut</div>
          <div className="eval-status-btns">
            {EVAL_STATUTS.map(st => <button key={st} onClick={() => updateStatus(ev.id, st)} className={`eval-status-btn ${ev.status === st ? 'active' : ''}`} style={{ '--sc': EVAL_COLORS[st] || '#6b7280' }}>{st}</button>)}
          </div>
        </div>

        {canSetPrice && (
          <div className="eval-section">
            <button className="eval-btn-wholesale" onClick={() => setShowWholesale(ev)}>Envoyer aux grossistes (Wholesale)</button>
          </div>
        )}

        <div className="eval-footer-meta">Recu le {new Date(ev.created_at).toLocaleString('fr-CA')}{ev.created_by && ev.created_by !== 'client' ? ` — par ${ev.created_by}` : ' — soumis par le client'}</div>

        {lightbox && <PhotoSlider photos={lightbox.photos} startIndex={lightbox.idx} onClose={() => setLightbox(null)} />}
        {showWholesale && <WholesalePanel evaluation={showWholesale} onClose={() => setShowWholesale(null)} />}
      </div>
    );
  }

  // ── STATS VIEW ──
  if (view === 'stats') {
    const repris = evals.filter(e => e.status === 'REPRIS');
    const perdus = evals.filter(e => e.status === 'REFUSE');
    const modelCount = (arr) => {
      const m = {};
      arr.forEach(e => { const k = `${e.model} ${e.year}`; m[k] = m[k] || { model: k, make: e.make, count: 0, total: 0 }; m[k].count++; if (e.prix_reprise) m[k].total += Number(e.prix_reprise); });
      return Object.values(m).sort((a,b) => b.count - a.count).slice(0,5);
    };
    return (
      <div data-testid="eval-stats">
        <div className="eval-top-bar">
          <h2 className="eval-page-title">Statistiques des evaluations</h2>
          <div className="eval-view-toggle">
            <button onClick={() => setView('stats')} className="eval-view-btn active">Stats</button>
            <button onClick={() => setView('list')} className="eval-view-btn">Liste</button>
            <button className="eval-btn-primary" onClick={() => setView('list')}>Nouvelle evaluation</button>
          </div>
        </div>
        <div className="eval-stat-cards">
          <div className="eval-stat-card"><div className="eval-stat-sublabel">Total des evaluations</div><div className="eval-stat-big">{evals.length}</div><span className="eval-stat-link" onClick={() => { setView('list'); setFilter('all'); }}>Voir les evaluations</span></div>
          <div className="eval-stat-card" style={{ borderTop: '3px solid #eab308' }}><div className="eval-stat-sublabel">Evaluations {evalBadge('EN ATTENTE')}</div><div className="eval-stat-big">{statusCounts['EN ATTENTE']}</div><span className="eval-stat-link" onClick={() => { setView('list'); setFilter('EN ATTENTE'); }}>Voir en attente</span></div>
          <div className="eval-stat-card" style={{ borderTop: '3px solid #22c55e' }}><div className="eval-stat-sublabel">Vehicules {evalBadge('REPRIS')}</div><div className="eval-stat-big">{statusCounts['REPRIS']}</div><span className="eval-stat-link" onClick={() => { setView('list'); setFilter('REPRIS'); }}>Voir reprises</span></div>
          <div className="eval-stat-card" style={{ borderTop: '3px solid #ef4444' }}><div className="eval-stat-sublabel">Vehicules {evalBadge('REFUSE')}</div><div className="eval-stat-big">{statusCounts['REFUSE']}</div><span className="eval-stat-link" onClick={() => { setView('list'); setFilter('REFUSE'); }}>Voir perdus</span></div>
        </div>
        <div className="eval-stat-row">
          <div className="eval-stat-pct-card"><div className="eval-stat-sublabel">Pourcentage de reprise</div><div className="eval-stat-pct">{pctReprise}%</div></div>
          <div className="eval-stat-pct-card"><div className="eval-stat-sublabel">Prix moyen reprise</div><div className="eval-stat-pct">{repris.length > 0 ? Math.round(repris.reduce((s,e) => s + Number(e.prix_reprise||0), 0) / repris.length).toLocaleString('fr-CA') : '0'} $</div></div>
        </div>
        <div className="eval-tables-row">
          <div className="eval-table-card"><div className="eval-section-title">Top 5 modeles repris</div>
            <table className="eval-mini-table"><thead><tr><th>Modele</th><th>Qte</th><th>Moy. $</th></tr></thead><tbody>
              {modelCount(repris).map(m => <tr key={m.model}><td><strong>{m.model}</strong><br/><span style={{fontSize:'0.7rem',color:'var(--text-secondary)'}}>{m.make}</span></td><td>{m.count}</td><td>{m.total > 0 ? Math.round(m.total/m.count).toLocaleString('fr-CA')+' $' : '—'}</td></tr>)}
              {modelCount(repris).length === 0 && <tr><td colSpan={3} style={{textAlign:'center',color:'var(--text-secondary)'}}>Aucune donnee</td></tr>}
            </tbody></table>
          </div>
          <div className="eval-table-card"><div className="eval-section-title">Top 5 modeles perdus</div>
            <table className="eval-mini-table"><thead><tr><th>Modele</th><th>Qte</th><th>Moy. $</th></tr></thead><tbody>
              {modelCount(perdus).map(m => <tr key={m.model}><td><strong>{m.model}</strong><br/><span style={{fontSize:'0.7rem',color:'var(--text-secondary)'}}>{m.make}</span></td><td>{m.count}</td><td>{m.total > 0 ? Math.round(m.total/m.count).toLocaleString('fr-CA')+' $' : '—'}</td></tr>)}
              {modelCount(perdus).length === 0 && <tr><td colSpan={3} style={{textAlign:'center',color:'var(--text-secondary)'}}>Aucune donnee</td></tr>}
            </tbody></table>
          </div>
        </div>
      </div>
    );
  }

  // ── LIST VIEW (Torque-style table) ──
  return (
    <div data-testid="evaluations-tab" className="eval-container">
      <div className="eval-top-bar">
        <h2 className="eval-page-title">Liste des evaluations</h2>
        <div className="eval-view-toggle">
          <button onClick={() => setView('stats')} className="eval-view-btn">Stats</button>
          <button onClick={() => setView('list')} className="eval-view-btn active">Liste</button>
          <button className="eval-btn-filter" onClick={() => setShowFilters(!showFilters)}>{showFilters ? 'Masquer filtres' : 'Filtrer'}</button>
          <button className="eval-btn-primary" onClick={fetchEvals}>{loading ? '...' : 'Rafraichir'}</button>
        </div>
      </div>

      {/* Filter tabs */}
      <div className="eval-filter-tabs">
        {[
          { id: 'all', label: 'Toutes', count: statusCounts.all },
          { id: 'EN ATTENTE', label: 'En attente', count: statusCounts['EN ATTENTE'] },
          { id: 'PRIX RECU', label: 'Prix recu', count: statusCounts['PRIX RECU'] },
          { id: 'REPRIS', label: 'Repris', count: statusCounts['REPRIS'] },
          { id: 'REFUSE', label: 'Perdu', count: statusCounts['REFUSE'] },
        ].map(f => (
          <button key={f.id} onClick={() => { setFilter(f.id); setPage(1); }} className={`eval-filter-tab ${filter === f.id ? 'active' : ''}`}>
            {f.label} <span className="eval-filter-count">{f.count}</span>
          </button>
        ))}
      </div>

      <div className="eval-content-row">
        {/* Main table */}
        <div className="eval-table-main">
          {paginated.length === 0 ? (
            <div className="eval-empty">{evals.length === 0 ? 'Aucune evaluation' : 'Aucun resultat pour ce filtre'}</div>
          ) : (
            <>
              <div className="table-wrap">
                <table className="eval-table" data-testid="eval-table">
                  <thead><tr>
                    <th>Date</th><th>Vehicule</th><th>Client</th><th>Valeurs</th><th>Statut</th><th></th>
                  </tr></thead>
                  <tbody>
                    {paginated.map(ev => {
                      const logo = getBrandLogo(ev.make);
                      return (
                        <tr key={ev.id} onClick={() => setSelected(ev)} className="eval-row" data-testid={`eval-row-${ev.id}`}>
                          <td className="eval-td-date">{new Date(ev.created_at).toLocaleDateString('fr-CA', { day: 'numeric', month: 'short', year: 'numeric' })}</td>
                          <td className="eval-td-vehicle">
                            <div className="eval-vehicle-info">
                              {logo && <img src={logo} alt="" className="eval-brand-logo" onError={e => e.target.style.display='none'} />}
                              <div>
                                <div className="eval-v-name">{ev.make} {ev.model}</div>
                                <div className="eval-v-year">{ev.year} {ev.trim}</div>
                                <div className="eval-v-km">{ev.km ? `${Number(ev.km).toLocaleString('fr-CA')} KM` : ''}</div>
                                <div className="eval-v-vin">{ev.vin}</div>
                              </div>
                            </div>
                            {ev.photos?.length > 0 && (
                              <div className="eval-row-photos" onClick={e => e.stopPropagation()}>
                                {ev.photos.slice(0, 4).map((url, i) => (
                                  <img key={i} src={url} alt="" className="eval-row-thumb" onClick={() => setLightbox({ photos: ev.photos, idx: i })} />
                                ))}
                                {ev.photos.length > 4 && <span className="eval-row-more">+{ev.photos.length - 4}</span>}
                              </div>
                            )}
                            {/* Wholesale inline menu */}
                            {(user?.role === 'admin' || user?.role === 'directeur') && wholesaleInline === ev.id && (
                              <div className="eval-ws-inline" onClick={e => e.stopPropagation()}>
                                <div className="eval-ws-title">Envoyer aux grossistes {replyToEmail && <span className="eval-ws-reply">Retour: {replyToEmail}</span>}</div>
                                {wsContacts.length === 0 ? (
                                  <div className="eval-ws-empty">Aucun contact wholesale</div>
                                ) : (
                                  <>
                                    {wsContacts.map(c => (
                                      <label key={c.id || c.email} className="eval-ws-contact">
                                        <input type="checkbox" checked={!!wsChecked[c.id || c.email]} onChange={() => setWsChecked(s => ({ ...s, [c.id || c.email]: !s[c.id || c.email] }))} disabled={!c.email} />
                                        <span className="eval-ws-name">{c.name || c.email}</span>
                                        {wsSent[c.id || c.email] && <span className="eval-ws-sent">Envoye</span>}
                                      </label>
                                    ))}
                                    <button className="eval-ws-send-btn" onClick={() => sendWholesaleChecked(ev)} disabled={wsSending || Object.values(wsChecked).filter(Boolean).length === 0}>
                                      {wsSending ? '...' : `Envoyer (${Object.values(wsChecked).filter(Boolean).length})`}
                                    </button>
                                  </>
                                )}
                              </div>
                            )}
                          </td>
                          <td className="eval-td-client">
                            <div className="eval-c-name">{ev.client_name}</div>
                            <div className="eval-c-phone">{ev.client_phone}</div>
                            {ev.form_data?.provenance && <div className="eval-c-src">{ev.form_data.provenance}</div>}
                          </td>
                          <td className="eval-td-values">
                            {ev.prix_reprise ? <div className="eval-val-prix">{Number(ev.prix_reprise).toLocaleString('fr-CA')} $</div> : <div className="eval-val-empty">—</div>}
                          </td>
                          <td className="eval-td-status">{evalBadge(ev.status)}</td>
                          <td className="eval-td-actions" onClick={e => e.stopPropagation()}>
                            <button className="eval-action-btn" title="Voir" onClick={() => setSelected(ev)}>&#9998;</button>
                            {(user?.role === 'admin' || user?.role === 'directeur') && (
                              <button className={`eval-action-btn eval-wholesale-btn ${wholesaleInline === ev.id ? 'active' : ''}`} title="Wholesale" onClick={() => toggleWholesaleInline(ev.id)}>W</button>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <div className="eval-pagination">
                <span className="eval-page-info">Resultats {(page-1)*perPage+1} a {Math.min(page*perPage, filtered.length)} de {filtered.length}</span>
                <div className="eval-page-btns">
                  <button disabled={page<=1} onClick={() => setPage(1)} className="eval-page-btn">&laquo;</button>
                  <button disabled={page<=1} onClick={() => setPage(p=>p-1)} className="eval-page-btn">&lsaquo;</button>
                  {Array.from({length: Math.min(totalPages, 5)}, (_, i) => {
                    const p = page <= 3 ? i + 1 : page + i - 2;
                    if (p < 1 || p > totalPages) return null;
                    return <button key={p} onClick={() => setPage(p)} className={`eval-page-btn ${page === p ? 'active' : ''}`}>{p}</button>;
                  })}
                  <button disabled={page>=totalPages} onClick={() => setPage(p=>p+1)} className="eval-page-btn">&rsaquo;</button>
                  <button disabled={page>=totalPages} onClick={() => setPage(totalPages)} className="eval-page-btn">&raquo;</button>
                </div>
              </div>
            </>
          )}
        </div>

        {/* Right filter panel */}
        {showFilters && (
          <div className="eval-filter-panel">
            <div className="eval-filter-title">Filtrer les evaluations</div>
            <div className="eval-filter-field"><label>Recherche par NIV</label><input value={searchVin} onChange={e => { setSearchVin(e.target.value); setPage(1); }} placeholder="" /></div>
            <div className="eval-filter-field"><label>Nom ou prenom du client</label><input value={search} onChange={e => { setSearch(e.target.value); setPage(1); }} placeholder="" /></div>
            <div className="eval-filter-field"><label>Telephone du client</label><input value={searchPhone} onChange={e => { setSearchPhone(e.target.value); setPage(1); }} placeholder="" /></div>
            <button className="eval-btn-ghost" style={{width:'100%',marginTop:'0.75rem'}} onClick={() => { setSearch(''); setSearchVin(''); setSearchPhone(''); setFilter('all'); setPage(1); }}>Reinitialiser</button>
          </div>
        )}
      </div>
    </div>
  );
}


// ═══════════════════════════════════════════════════
// UTILISATEURS TAB — Admin user management
// ═══════════════════════════════════════════════════

const ROLE_LABELS = { admin: 'Administrateur', directeur: 'Directeur des ventes', conseiller: 'Conseiller' };
const ROLE_COLORS = { admin: '#ef4444', directeur: '#a855f7', conseiller: '#0ea5e9' };

function UtilisateursTab() {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showAdd, setShowAdd] = useState(false);
  const [editUser, setEditUser] = useState(null);
  const [newUser, setNewUser] = useState({ username: '', password: '', name: '', role: 'conseiller', email: '' });

  const fetchUsers = useCallback(async () => {
    setLoading(true);
    try { const r = await fetch(`${API}/api/users`); const d = await r.json(); setUsers(d.users || []); } catch (e) { console.error(e); }
    setLoading(false);
  }, []);

  useEffect(() => { fetchUsers(); }, [fetchUsers]);

  const handleAdd = async () => {
    if (!newUser.username || !newUser.password || !newUser.name) return;
    try {
      const r = await fetch(`${API}/api/users`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(newUser) });
      if (r.ok) { setShowAdd(false); setNewUser({ username: '', password: '', name: '', role: 'conseiller', email: '' }); fetchUsers(); }
    } catch (e) { console.error(e); }
  };

  const handleUpdate = async () => {
    if (!editUser) return;
    try {
      await fetch(`${API}/api/users/${editUser.username}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(editUser.changes) });
      setEditUser(null); fetchUsers();
    } catch (e) { console.error(e); }
  };

  const handleDelete = async (username) => {
    if (!window.confirm(`Supprimer ${username}?`)) return;
    try { await fetch(`${API}/api/users/${username}`, { method: 'DELETE' }); fetchUsers(); } catch (e) { console.error(e); }
  };

  const is = { // input styles
    input: { width: '100%', padding: '10px 12px', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.9rem', fontFamily: 'IBM Plex Sans', background: 'var(--surface)' },
    label: { display: 'block', fontSize: '0.7rem', fontWeight: 600, color: 'var(--text-secondary)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '4px' },
    btn: { padding: '10px 20px', borderRadius: '6px', border: 'none', fontSize: '0.85rem', fontWeight: 700, cursor: 'pointer', background: 'var(--accent-blue)', color: '#fff' },
    btnSec: { padding: '10px 20px', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.85rem', fontWeight: 600, cursor: 'pointer', background: 'var(--surface)', color: 'var(--text-secondary)' },
  };

  return (
    <div data-testid="utilisateurs-tab">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem', flexWrap: 'wrap', gap: '0.5rem' }}>
        <h2 className="section-title" style={{ margin: 0 }}>Gestion des utilisateurs</h2>
        <button style={is.btn} onClick={() => setShowAdd(!showAdd)} data-testid="add-user-btn">{showAdd ? 'Annuler' : '+ Ajouter'}</button>
      </div>

      {showAdd && (
        <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '8px', padding: '1.25rem', marginBottom: '1rem' }}>
          <div style={{ fontFamily: 'Chivo', fontWeight: 700, marginBottom: '0.75rem' }}>Nouvel utilisateur</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '0.75rem' }}>
            <div><label style={is.label}>Nom complet</label><input style={is.input} value={newUser.name} onChange={e => setNewUser(n => ({ ...n, name: e.target.value }))} data-testid="new-user-name" /></div>
            <div><label style={is.label}>Identifiant</label><input style={is.input} value={newUser.username} onChange={e => setNewUser(n => ({ ...n, username: e.target.value.toLowerCase().replace(/\s/g, '') }))} data-testid="new-user-username" /></div>
            <div><label style={is.label}>Mot de passe</label><input style={is.input} value={newUser.password} onChange={e => setNewUser(n => ({ ...n, password: e.target.value }))} data-testid="new-user-password" /></div>
            <div><label style={is.label}>Courriel</label><input style={is.input} type="email" value={newUser.email} onChange={e => setNewUser(n => ({ ...n, email: e.target.value }))} data-testid="new-user-email" placeholder="pour reinitialiser le mot de passe" /></div>
            <div><label style={is.label}>Role</label><select style={is.input} value={newUser.role} onChange={e => setNewUser(n => ({ ...n, role: e.target.value }))} data-testid="new-user-role"><option value="conseiller">Conseiller</option><option value="directeur">Directeur des ventes</option><option value="admin">Administrateur</option></select></div>
          </div>
          <div style={{ marginTop: '1rem' }}><button style={is.btn} onClick={handleAdd} data-testid="save-user-btn">Creer</button></div>
        </div>
      )}

      {editUser && (
        <div style={{ background: 'var(--surface)', border: '2px solid var(--accent-blue)', borderRadius: '8px', padding: '1.25rem', marginBottom: '1rem' }}>
          <div style={{ fontFamily: 'Chivo', fontWeight: 700, marginBottom: '0.75rem' }}>Modifier: {editUser.username}</div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '0.75rem' }}>
            <div><label style={is.label}>Nom</label><input style={is.input} value={editUser.changes.name || ''} onChange={e => setEditUser(u => ({ ...u, changes: { ...u.changes, name: e.target.value } }))} /></div>
            <div><label style={is.label}>Courriel</label><input style={is.input} type="email" value={editUser.changes.email || ''} onChange={e => setEditUser(u => ({ ...u, changes: { ...u.changes, email: e.target.value } }))} placeholder="email@exemple.com" /></div>
            <div><label style={is.label}>Nouveau mot de passe</label><input style={is.input} value={editUser.changes.password || ''} onChange={e => setEditUser(u => ({ ...u, changes: { ...u.changes, password: e.target.value } }))} placeholder="Laisser vide pour ne pas changer" /></div>
            <div><label style={is.label}>Role</label><select style={is.input} value={editUser.changes.role || ''} onChange={e => setEditUser(u => ({ ...u, changes: { ...u.changes, role: e.target.value } }))}><option value="conseiller">Conseiller</option><option value="directeur">Directeur</option><option value="admin">Admin</option></select></div>
          </div>
          <div style={{ marginTop: '1rem', display: 'flex', gap: '0.5rem' }}>
            <button style={is.btn} onClick={handleUpdate}>Sauvegarder</button>
            <button style={is.btnSec} onClick={() => setEditUser(null)}>Annuler</button>
          </div>
        </div>
      )}

      <div>
        {users.map(u => {
          const rc = ROLE_COLORS[u.role] || '#6b7280';
          return (
            <div key={u.username} style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '8px', padding: '1rem', marginBottom: '0.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '0.5rem' }} data-testid={`user-card-${u.username}`}>
              <div>
                <div style={{ fontWeight: 600, fontSize: '0.95rem' }}>{u.name}</div>
                <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', fontFamily: 'IBM Plex Mono' }}>{u.username}</div>
                {u.email && <div style={{ fontSize: '0.7rem', color: '#0ea5e9' }}>{u.email}</div>}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <span style={{ padding: '3px 10px', borderRadius: '10px', fontSize: '0.7rem', fontWeight: 700, background: `${rc}20`, color: rc, border: `1px solid ${rc}40` }}>{ROLE_LABELS[u.role] || u.role}</span>
                <button onClick={() => setEditUser({ username: u.username, changes: { name: u.name, role: u.role, email: u.email || '', password: '' } })} style={{ background: 'none', border: '1px solid var(--border)', borderRadius: '4px', padding: '4px 10px', fontSize: '0.7rem', cursor: 'pointer', color: 'var(--text-secondary)' }}>Modifier</button>
                {u.username !== 'admin' && <button onClick={() => handleDelete(u.username)} style={{ background: 'none', border: '1px solid var(--accent-red)', borderRadius: '4px', padding: '4px 10px', fontSize: '0.7rem', cursor: 'pointer', color: 'var(--accent-red)' }}>Supprimer</button>}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}


export default App;
