import React, { useState, useEffect, useCallback } from 'react';
import './App.css';

const API = process.env.REACT_APP_BACKEND_URL;

function App() {
  const [tab, setTab] = useState('cockpit');
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
      <Header tab={tab} setTab={setTab} status={status} />
      <div className="main-content">
        {loading && tab !== 'reprise' && tab !== 'evaluations' ? <LoadingState /> : (
          <>
            {tab === 'cockpit' && <CockpitTab inventory={inventory} status={status} />}
            {tab === 'compare' && <CompareTab />}
            {tab === 'reprise' && <RepriseTab />}
            {tab === 'evaluations' && <EvaluationsTab />}
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

function Header({ tab, setTab, status }) {
  const [showRunPanel, setShowRunPanel] = useState(false);
  const tabs = [
    { id: 'cockpit', label: 'Cockpit' },
    { id: 'compare', label: 'Kennebec vs FB' },
    { id: 'reprise', label: 'Reprise' },
    { id: 'evaluations', label: 'Evaluations' },
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'inventory', label: 'Inventaire' },
    { id: 'posts', label: 'Posts FB' },
    { id: 'textpreview', label: 'Preview Texte' },
    { id: 'events', label: 'Events' },
    { id: 'architecture', label: 'Architecture' },
    { id: 'changelog', label: 'Changelog' },
  ];
  const connected = status?.supabase_connected;
  return (
    <>
      <header className="header" data-testid="header">
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
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
          <button className="run-btn" onClick={() => setShowRunPanel(!showRunPanel)} data-testid="run-cron-btn">
            RUN CRON
          </button>
          <span className="version-tag" data-testid="version-tag">v{status?.version || '2.1.0'}</span>
        </div>
      </header>
      {showRunPanel && <RunPanel onClose={() => setShowRunPanel(false)} />}
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

function RepriseTab() {
  const [sec, setSec] = useState('client');
  const [vinSpecs, setVinSpecs] = useState(null);
  const [vinLoading, setVinLoading] = useState(false);
  const [vinError, setVinError] = useState('');
  const [photos, setPhotos] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [evalId] = useState(() => Math.random().toString(36).slice(2));
  const [form, setForm] = useState({
    prenom: '', nom: '', telephone: '', courriel: '',
    type_transaction: '', solde_du: false, solde_montant: '', interet: '', provenance: '', notes_client: '',
    vin: '', km: '', couleur_ext: '', couleur_int: '', nombre_cles: '2',
    options: [], etat_general: 3, etat_parebrise: 'Bon etat', etat_mecanique: '', dommages: [],
    garantie_constructeur: false, garantie_constructeur_date: '', garantie_prolongee: false, garantie_prolongee_detail: '',
    commentaires: '',
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
    setVinLoading(true); setVinError('');
    try {
      const r = await fetch(`${API}/api/vin/${v}`);
      if (!r.ok) throw new Error('VIN non trouve');
      setVinSpecs((await r.json()).specs);
    } catch (e) { setVinError(e.message); }
    setVinLoading(false);
  };

  const handlePhotos = async (e) => {
    const files = Array.from(e.target.files).slice(0, 10 - photos.length);
    if (!files.length) return;
    setUploading(true);
    for (const file of files) {
      const fd = new FormData(); fd.append('file', file); fd.append('evaluation_id', evalId);
      try {
        const r = await fetch(`${API}/api/evaluations/upload-photo`, { method: 'POST', body: fd });
        if (r.ok) { const d = await r.json(); setPhotos(p => [...p, { url: d.url, name: file.name }]); }
      } catch (err) { console.error(err); }
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
        <div style={{ fontSize: '2.5rem', marginBottom: '1rem' }}>✅</div>
        <h2 style={{ marginBottom: '0.5rem' }}>Demande envoyee!</h2>
        <p style={{ color: 'var(--text-secondary)' }}>Merci {form.prenom}! Daniel Giroux va analyser votre vehicule.</p>
        <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginTop: '1rem' }}>418-222-3939 — Kennebec Dodge Chrysler</p>
        <button style={{ ...rs.btnPrimary, marginTop: '1.5rem' }} onClick={() => window.location.reload()}>Nouvelle evaluation</button>
      </div>
    );
  }

  return (
    <div style={{ padding: '0' }} data-testid="reprise-tab">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
        <h2 className="section-title">Nouvelle evaluation</h2>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>{secIdx + 1}/{REPRISE_SECTIONS.length}</span>
      </div>
      <div style={rs.navBar} className="reprise-nav">
        {REPRISE_SECTIONS.map((s, i) => (
          <button key={s} style={rs.navBtn(sec === s)} onClick={() => setSec(s)} data-testid={`reprise-nav-${s}`}>
            {i < secIdx ? '✓ ' : ''}{REPRISE_LABELS[s]}
          </button>
        ))}
      </div>

      {sec === 'client' && (
        <>
          <div style={rs.card}><div style={rs.cardTitle}>Informations du client</div>
            <div style={rs.row}>
              <div><label style={rs.label}>Prenom *</label><input style={rs.input} value={form.prenom} onChange={e => up('prenom', e.target.value)} data-testid="reprise-prenom" /></div>
              <div><label style={rs.label}>Nom *</label><input style={rs.input} value={form.nom} onChange={e => up('nom', e.target.value)} data-testid="reprise-nom" /></div>
            </div>
            <div style={{ ...rs.row, marginTop: '0.75rem' }}>
              <div><label style={rs.label}>Telephone *</label><input style={rs.input} type="tel" value={form.telephone} onChange={e => up('telephone', e.target.value)} data-testid="reprise-tel" /></div>
              <div><label style={rs.label}>Courriel</label><input style={rs.input} type="email" value={form.courriel} onChange={e => up('courriel', e.target.value)} /></div>
            </div>
          </div>
          <div style={rs.card}><div style={rs.cardTitle}>Transaction</div>
            <div style={rs.row}>
              <div><label style={rs.label}>Type</label><select style={rs.input} value={form.type_transaction} onChange={e => up('type_transaction', e.target.value)}><option value="">—</option>{TYPES_TRANSACTION.map(t => <option key={t}>{t}</option>)}</select></div>
              <div><label style={rs.label}>Interet</label><select style={rs.input} value={form.interet} onChange={e => up('interet', e.target.value)}><option value="">—</option>{INTERET_CLIENT.map(t => <option key={t}>{t}</option>)}</select></div>
              <div><label style={rs.label}>Provenance</label><select style={rs.input} value={form.provenance} onChange={e => up('provenance', e.target.value)}><option value="">—</option>{PROVENANCES.map(p => <option key={p}>{p}</option>)}</select></div>
            </div>
          </div>
        </>
      )}

      {sec === 'vehicle' && (
        <>
          <div style={rs.card}><div style={rs.cardTitle}>Identification par VIN</div>
            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
              <input style={{ ...rs.input, flex: 1, minWidth: '200px', fontFamily: 'IBM Plex Mono', textTransform: 'uppercase' }} value={form.vin} onChange={e => { up('vin', e.target.value.toUpperCase().slice(0, 17)); setVinError(''); }} maxLength={17} placeholder="17 caracteres" data-testid="reprise-vin" />
              <button style={rs.btnPrimary} onClick={decodeVin} disabled={vinLoading} data-testid="reprise-decode">{vinLoading ? '...' : 'Decoder'}</button>
            </div>
            <div style={{ fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: '4px' }}>{form.vin.length}/17</div>
            {vinError && <div style={{ color: 'var(--accent-red)', fontSize: '0.8rem', marginTop: '0.5rem' }}>{vinError}</div>}
          </div>
          {vinSpecs && (
            <div style={rs.card}><div style={rs.cardTitle}>Vehicule identifie</div>
              <div style={rs.row}>
                {[['Marque', vinSpecs.make], ['Modele', vinSpecs.model], ['Annee', vinSpecs.year], ['Trim', vinSpecs.trim], ['Moteur', `${vinSpecs.engine_cylinders||''}cyl ${vinSpecs.engine_displacement||''}L ${vinSpecs.engine_hp||''}HP`.trim()], ['Motricite', vinSpecs.drive_type]].filter(([,v]) => v && v !== 'cyl LHP').map(([l,v]) => (
                  <div key={l} style={{ padding: '6px 0', borderBottom: '1px solid var(--border)' }}><span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{l}</span><br/><span style={{ fontWeight: 600, fontSize: '0.9rem' }}>{v}</span></div>
                ))}
              </div>
            </div>
          )}
          <div style={rs.card}><div style={rs.cardTitle}>Details</div>
            <div style={rs.row}>
              <div><label style={rs.label}>Kilometrage</label><input style={rs.input} value={form.km} onChange={e => up('km', e.target.value)} data-testid="reprise-km" /></div>
              <div><label style={rs.label}>Cles</label><select style={rs.input} value={form.nombre_cles} onChange={e => up('nombre_cles', e.target.value)}>{['0','1','2','3+'].map(n => <option key={n}>{n}</option>)}</select></div>
              <div><label style={rs.label}>Couleur ext.</label><select style={rs.input} value={form.couleur_ext} onChange={e => up('couleur_ext', e.target.value)}><option value="">—</option>{COULEURS_EXT.map(c => <option key={c}>{c}</option>)}</select></div>
              <div><label style={rs.label}>Couleur int.</label><select style={rs.input} value={form.couleur_int} onChange={e => up('couleur_int', e.target.value)}><option value="">—</option>{COULEURS_INT.map(c => <option key={c}>{c}</option>)}</select></div>
            </div>
          </div>
        </>
      )}

      {sec === 'options' && (
        <>
          <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginBottom: '0.75rem' }}>{form.options.length} option{form.options.length !== 1 ? 's' : ''}</p>
          {VEHICLE_OPTIONS.map(cat => (
            <div key={cat.cat} style={rs.card}><div style={rs.cardTitle}>{cat.cat}</div>
              <div>{cat.items.map(o => <span key={o} style={rs.chip(form.options.includes(o))} onClick={() => togOpt(o)}>{form.options.includes(o) ? '✓ ' : ''}{o}</span>)}</div>
            </div>
          ))}
        </>
      )}

      {sec === 'etat' && (
        <>
          <div style={rs.card}><div style={rs.cardTitle}>Etat general</div>
            <div style={rs.etatBar}>{ETATS_GENERAL.map(e => <div key={e.value} style={rs.etatSeg(e.color, form.etat_general >= e.value)} onClick={() => up('etat_general', e.value)} data-testid={`reprise-etat-${e.value}`}>{e.label}</div>)}</div>
          </div>
          <div style={rs.card}><div style={rs.cardTitle}>Pare-brise</div>
            <div>{ETATS_PAREBRISE.map(e => <span key={e} style={rs.chip(form.etat_parebrise === e)} onClick={() => up('etat_parebrise', e)}>{e}</span>)}</div>
          </div>
          <div style={rs.card}><div style={rs.cardTitle}>Dommages carrosserie</div>
            <div>{ZONES_DOMMAGES.map(z => <span key={z} style={rs.dmgChip(form.dommages.includes(z))} onClick={() => togDmg(z)}>{form.dommages.includes(z) ? '✕ ' : ''}{z}</span>)}</div>
          </div>
          <div style={rs.card}><div style={rs.cardTitle}>Mecanique</div>
            <textarea style={{ ...rs.input, minHeight: '80px', resize: 'vertical' }} value={form.etat_mecanique} onChange={e => up('etat_mecanique', e.target.value)} placeholder="Bruits, problemes connus..." />
          </div>
        </>
      )}

      {sec === 'photos' && (
        <div style={rs.card}><div style={rs.cardTitle}>Photos ({photos.length}/10)</div>
          <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', marginBottom: '0.75rem' }}>Exterieur, interieur, odometre, defauts</p>
          <input type="file" accept="image/*" multiple onChange={handlePhotos} style={{ display: 'none' }} id="reprise-photo-input" />
          <label htmlFor="reprise-photo-input" style={{ ...rs.btnSecondary, display: 'block', textAlign: 'center', cursor: 'pointer', padding: '1.5rem' }}>{uploading ? 'Envoi...' : 'Ajouter des photos'}</label>
          {photos.length > 0 && (
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(80px, 1fr))', gap: '0.5rem', marginTop: '0.75rem' }}>
              {photos.map((p, i) => <div key={i} style={{ position: 'relative' }}><img src={p.url} alt="" style={{ width: '100%', aspectRatio: '1', objectFit: 'cover', borderRadius: '6px', border: '1px solid var(--border)' }} /><button onClick={() => setPhotos(pr => pr.filter((_, j) => j !== i))} style={{ position: 'absolute', top: 2, right: 2, background: 'var(--accent-red)', color: '#fff', border: 'none', borderRadius: '50%', width: 20, height: 20, fontSize: '0.6rem', cursor: 'pointer' }}>✕</button></div>)}
            </div>
          )}
        </div>
      )}

      {sec === 'garanties' && (
        <>
          <div style={rs.card}><div style={rs.cardTitle}>Garantie constructeur</div>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}><input type="checkbox" checked={form.garantie_constructeur} onChange={e => up('garantie_constructeur', e.target.checked)} /> Encore valide</label>
            {form.garantie_constructeur && <div style={{ marginTop: '0.5rem' }}><label style={rs.label}>Date expiration</label><input style={rs.input} type="date" value={form.garantie_constructeur_date} onChange={e => up('garantie_constructeur_date', e.target.value)} /></div>}
          </div>
          <div style={rs.card}><div style={rs.cardTitle}>Garantie prolongee</div>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}><input type="checkbox" checked={form.garantie_prolongee} onChange={e => up('garantie_prolongee', e.target.checked)} /> Oui</label>
            {form.garantie_prolongee && <div style={{ marginTop: '0.5rem' }}><label style={rs.label}>Details</label><textarea style={{ ...rs.input, minHeight: '60px' }} value={form.garantie_prolongee_detail} onChange={e => up('garantie_prolongee_detail', e.target.value)} placeholder="Fournisseur, couverture..." /></div>}
          </div>
        </>
      )}

      {sec === 'notes' && (
        <div style={rs.card}><div style={rs.cardTitle}>Commentaires</div>
          <textarea style={{ ...rs.input, minHeight: '150px', resize: 'vertical' }} value={form.commentaires} onChange={e => up('commentaires', e.target.value)} placeholder="Historique, reparations, raison de vente..." data-testid="reprise-commentaires" />
        </div>
      )}

      <div style={rs.bottomBar}>
        {secIdx > 0 && <button style={rs.btnSecondary} onClick={goPrev}>Precedent</button>}
        {secIdx < REPRISE_SECTIONS.length - 1 ? (
          <button style={rs.btnPrimary} onClick={goNext}>Suivant</button>
        ) : (
          <button style={{ ...rs.btnPrimary, opacity: canSubmit && !submitting ? 1 : 0.5 }} onClick={handleSubmit} disabled={!canSubmit || submitting} data-testid="reprise-submit">
            {submitting ? 'Envoi...' : 'Envoyer'}
          </button>
        )}
      </div>
    </div>
  );
}

// ═══════════════════════════════════════════════════
// EVALUATIONS TAB — Admin list + detail
// ═══════════════════════════════════════════════════

const EVAL_STATUTS = ['NOUVEAU', 'EN EVALUATION', 'OFFRE ENVOYEE', 'ACCEPTE', 'REFUSE'];
const EVAL_COLORS = { 'NOUVEAU': '#3b82f6', 'EN EVALUATION': '#eab308', 'OFFRE ENVOYEE': '#a855f7', 'ACCEPTE': '#22c55e', 'REFUSE': '#ef4444' };

function EvaluationsTab() {
  const [evals, setEvals] = useState([]);
  const [selected, setSelected] = useState(null);
  const [filter, setFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(false);

  const fetchEvals = useCallback(async () => {
    setLoading(true);
    try { const r = await fetch(`${API}/api/evaluations`); const d = await r.json(); setEvals(d.evaluations || []); } catch (e) { console.error(e); }
    setLoading(false);
  }, []);

  useEffect(() => { fetchEvals(); }, [fetchEvals]);

  const updateStatus = async (id, st) => {
    await fetch(`${API}/api/evaluations/${id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ status: st }) });
    fetchEvals();
    if (selected?.id === id) setSelected(p => ({ ...p, status: st }));
  };

  const filtered = evals.filter(e => filter === 'all' || e.status === filter).filter(e => {
    if (!search) return true;
    const s = search.toLowerCase();
    return (e.client_name||'').toLowerCase().includes(s) || (e.vin||'').toLowerCase().includes(s) || (e.make||'').toLowerCase().includes(s) || (e.model||'').toLowerCase().includes(s);
  });

  const evalBadge = (st) => {
    const c = EVAL_COLORS[st] || '#6b7280';
    return <span style={{ padding: '2px 10px', borderRadius: '10px', fontSize: '0.7rem', fontWeight: 700, background: `${c}20`, color: c, border: `1px solid ${c}40` }}>{st}</span>;
  };

  if (selected) {
    const ev = selected;
    const fd = ev.form_data || {};
    return (
      <div data-testid="eval-detail">
        <button onClick={() => setSelected(null)} style={{ border: '1px solid var(--border)', borderRadius: '6px', padding: '6px 14px', cursor: 'pointer', background: 'var(--surface)', fontSize: '0.8rem', marginBottom: '1rem' }}>← Retour</button>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
          <h3 style={{ fontFamily: 'Chivo', fontWeight: 700 }}>{ev.year} {ev.make} {ev.model} {ev.trim}</h3>
          {evalBadge(ev.status)}
        </div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: '0.75rem', marginBottom: '1rem' }}>
          {[['VIN', ev.vin], ['Moteur', ev.engine], ['Motricite', ev.drive_type], ['Km', ev.km ? `${Number(ev.km).toLocaleString('fr-CA')} km` : null], ['Etat', ev.etat_general], ['Couleur ext.', fd.couleur_ext]].filter(([,v]) => v).map(([l,v]) => (
            <div key={l} style={{ background: 'var(--surface-secondary)', padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border)' }}><div style={{ fontSize: '0.65rem', color: 'var(--text-secondary)', textTransform: 'uppercase' }}>{l}</div><div style={{ fontWeight: 600, fontSize: '0.9rem', marginTop: '2px', wordBreak: 'break-all' }}>{v}</div></div>
          ))}
        </div>
        {fd.options?.length > 0 && <div style={{ marginBottom: '1rem' }}><strong style={{ fontSize: '0.8rem' }}>Options ({fd.options.length}):</strong><div style={{ marginTop: '0.5rem' }}>{fd.options.map(o => <span key={o} style={{ display: 'inline-block', padding: '3px 10px', margin: '2px', borderRadius: '4px', fontSize: '0.75rem', background: 'var(--accent-blue-subtle)', color: 'var(--accent-blue)', border: '1px solid var(--accent-blue)' }}>{o}</span>)}</div></div>}
        {ev.photos?.length > 0 && <div style={{ marginBottom: '1rem' }}><strong style={{ fontSize: '0.8rem' }}>Photos ({ev.photos.length}):</strong><div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(80px, 1fr))', gap: '0.5rem', marginTop: '0.5rem' }}>{ev.photos.map((u,i) => <img key={i} src={u} alt="" style={{ width: '100%', aspectRatio: '1', objectFit: 'cover', borderRadius: '6px', border: '1px solid var(--border)', cursor: 'pointer' }} onClick={() => window.open(u)} />)}</div></div>}
        <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '8px', padding: '1rem', marginBottom: '1rem' }}>
          <strong style={{ fontSize: '0.85rem' }}>Client</strong>
          <div style={{ marginTop: '0.5rem', fontSize: '0.9rem', fontWeight: 600 }}>{ev.client_name}</div>
          <div style={{ color: 'var(--accent-blue)', fontSize: '0.85rem' }}><a href={`tel:${ev.client_phone}`} style={{ color: 'inherit' }}>{ev.client_phone}</a></div>
          {ev.client_email && <div style={{ color: 'var(--text-secondary)', fontSize: '0.85rem' }}>{ev.client_email}</div>}
        </div>
        <div style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '8px', padding: '1rem' }}>
          <strong style={{ fontSize: '0.85rem' }}>Changer le statut</strong>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem', marginTop: '0.75rem' }}>
            {EVAL_STATUTS.map(st => <button key={st} onClick={() => updateStatus(ev.id, st)} style={{ padding: '6px 14px', borderRadius: '6px', fontSize: '0.75rem', fontWeight: 700, cursor: 'pointer', border: ev.status === st ? `2px solid ${EVAL_COLORS[st]}` : '1px solid var(--border)', background: ev.status === st ? `${EVAL_COLORS[st]}20` : 'var(--surface)', color: EVAL_COLORS[st] || 'var(--text-secondary)' }} data-testid={`eval-status-${st}`}>{st}</button>)}
          </div>
        </div>
        <div style={{ textAlign: 'center', fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: '1rem' }}>Recu le {new Date(ev.created_at).toLocaleString('fr-CA')}</div>
      </div>
    );
  }

  return (
    <div data-testid="evaluations-tab">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '0.5rem', marginBottom: '1rem' }}>
        <h2 className="section-title" style={{ margin: 0 }}>Evaluations</h2>
        <button onClick={fetchEvals} disabled={loading} style={{ border: '1px solid var(--border)', borderRadius: '6px', padding: '6px 14px', cursor: 'pointer', background: 'var(--surface)', fontSize: '0.8rem' }} data-testid="eval-refresh">{loading ? '...' : 'Rafraichir'}</button>
      </div>
      <div style={{ display: 'flex', gap: '0.25rem', flexWrap: 'wrap', marginBottom: '0.75rem' }}>
        {[{ l: 'Toutes', id: 'all', c: evals.length }, ...EVAL_STATUTS.map(s => ({ l: s, id: s, c: evals.filter(e => e.status === s).length }))].map(f => (
          <button key={f.id} onClick={() => setFilter(f.id)} style={{ padding: '4px 12px', borderRadius: '12px', border: 'none', cursor: 'pointer', fontSize: '0.75rem', fontWeight: 600, background: filter === f.id ? 'var(--accent-blue)' : 'var(--surface-secondary)', color: filter === f.id ? '#fff' : 'var(--text-secondary)' }} data-testid={`eval-filter-${f.id}`}>{f.l} ({f.c})</button>
        ))}
      </div>
      <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Rechercher VIN, client..." style={{ width: '100%', padding: '10px 12px', borderRadius: '6px', border: '1px solid var(--border)', fontSize: '0.85rem', marginBottom: '1rem', fontFamily: 'IBM Plex Sans' }} data-testid="eval-search" />
      {filtered.length === 0 ? (
        <div style={{ textAlign: 'center', color: 'var(--text-secondary)', padding: '3rem' }}>{evals.length === 0 ? 'Aucune evaluation' : 'Aucun resultat'}</div>
      ) : (
        <div className="eval-list">
          {filtered.map(ev => (
            <div key={ev.id} onClick={() => setSelected(ev)} style={{ background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: '8px', padding: '1rem', marginBottom: '0.5rem', cursor: 'pointer' }} className="eval-card" data-testid={`eval-card-${ev.id}`}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '0.4rem' }}>
                <div style={{ fontWeight: 700, fontSize: '0.9rem' }}>{ev.year} {ev.make} {ev.model} {ev.trim}</div>
                {evalBadge(ev.status)}
              </div>
              <div style={{ display: 'flex', gap: '1rem', fontSize: '0.8rem', color: 'var(--text-secondary)', flexWrap: 'wrap' }}>
                <span>{ev.client_name}</span>
                <span>{ev.client_phone}</span>
                {ev.km && <span>{Number(ev.km).toLocaleString('fr-CA')} km</span>}
                <span>{new Date(ev.created_at).toLocaleDateString('fr-CA', { month: 'short', day: 'numeric' })}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}


export default App;
