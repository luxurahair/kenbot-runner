import React, { useState, useCallback, useRef } from 'react';
import { API } from '../App';

const SECTIONS = [
  { id: 'client', label: 'Client', icon: '👤' },
  { id: 'vehicle', label: 'Véhicule', icon: '🚗' },
  { id: 'options', label: 'Options', icon: '⚙️' },
  { id: 'etat', label: 'État', icon: '🔍' },
  { id: 'photos', label: 'Photos', icon: '📸' },
  { id: 'garanties', label: 'Garanties', icon: '🛡️' },
  { id: 'notes', label: 'Notes', icon: '📝' },
];

const PROVENANCES = ['Walk-in', 'Téléphone', 'Site web', 'Facebook', 'Messenger', 'Référence', 'Autre'];
const TYPES_CLIENT = ['Particulier', 'Entreprise'];
const TYPES_TRANSACTION = ['Achat', 'Échange', 'Location'];
const INTERET_CLIENT = ['Neuf', 'Usagé', 'Les deux'];
const ETATS_GENERAL = [
  { label: 'Mauvais', color: '#ef4444', value: 1 },
  { label: 'Passable', color: '#f97316', value: 2 },
  { label: 'Moyen', color: '#eab308', value: 3 },
  { label: 'Bon', color: '#84cc16', value: 4 },
  { label: 'Excellent', color: '#22c55e', value: 5 },
];
const ETATS_PAREBRISE = ['Bon état', 'Éclat mineur', 'Fissuré', 'À remplacer'];
const COULEURS_EXT = ['Blanc', 'Noir', 'Gris', 'Argent', 'Rouge', 'Bleu', 'Vert', 'Brun', 'Beige', 'Orange', 'Jaune', 'Autre'];
const COULEURS_INT = ['Noir', 'Gris', 'Beige', 'Brun', 'Rouge', 'Autre'];

const VEHICLE_OPTIONS = [
  { cat: 'Confort', items: ['Sièges chauffants', 'Sièges ventilés', 'Sièges en cuir', 'Volant chauffant', 'Toit ouvrant / panoramique', 'Climatisation auto 2 zones', 'Climatisation auto 3 zones', 'Démarreur à distance'] },
  { cat: 'Technologie', items: ['Navigation GPS', 'Apple CarPlay', 'Android Auto', 'Écran tactile 8"+', 'Caméra de recul', 'Caméra 360°', 'Affichage tête haute (HUD)', 'Chargeur sans fil', 'Système audio premium'] },
  { cat: 'Sécurité', items: ['Détection angle mort', 'Aide au maintien de voie', 'Freinage d\'urgence auto', 'Régulateur adaptatif', 'Alerte trafic transversal', 'Capteurs de stationnement', 'Phares LED / adaptatifs'] },
  { cat: 'Performance', items: ['4x4 / AWD', 'Mode remorquage', 'Suspension adaptative', 'Turbo / Suralimenté', 'Différentiel arrière verrouillable', 'Échappement performance'] },
  { cat: 'Extérieur', items: ['Marchepieds', 'Barres de toit', 'Attelage de remorquage', 'Roues en alliage 18"+', 'Roues en alliage 20"+', 'Aileron', 'Vitres teintées'] },
];

const ZONES_DOMMAGES = [
  'Pare-chocs avant', 'Aile avant gauche', 'Aile avant droite',
  'Portière avant gauche', 'Portière avant droite',
  'Portière arrière gauche', 'Portière arrière droite',
  'Aile arrière gauche', 'Aile arrière droite',
  'Pare-chocs arrière', 'Toit', 'Capot', 'Coffre/Hayon',
  'Bas de caisse gauche', 'Bas de caisse droite',
];

export default function PublicForm() {
  const [section, setSection] = useState('client');
  const [vinSpecs, setVinSpecs] = useState(null);
  const [vinLoading, setVinLoading] = useState(false);
  const [vinError, setVinError] = useState('');
  const [photos, setPhotos] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const fileRef = useRef(null);
  const [evalId] = useState(() => crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2));

  const [form, setForm] = useState({
    prenom: '', nom: '', telephone: '', courriel: '',
    type_transaction: '', solde_du: false, solde_montant: '',
    interet: '', provenance: '', type_client: 'Particulier', notes_client: '',
    vin: '', km: '', couleur_ext: '', couleur_int: '',
    vu_vehicule: null, nombre_cles: '2',
    options: [],
    etat_general: 3, etat_parebrise: 'Bon état', etat_mecanique: '',
    dommages: [],
    garantie_constructeur: null, garantie_constructeur_date: '',
    garantie_prolongee: null, garantie_prolongee_detail: '',
    commentaires: '',
  });

  const update = (key, val) => setForm(f => ({ ...f, [key]: val }));
  const toggleOption = (opt) => setForm(f => ({
    ...f, options: f.options.includes(opt) ? f.options.filter(o => o !== opt) : [...f.options, opt]
  }));
  const toggleDommage = (zone) => setForm(f => ({
    ...f, dommages: f.dommages.includes(zone) ? f.dommages.filter(z => z !== zone) : [...f.dommages, zone]
  }));

  // VIN Decode
  const decodeVin = useCallback(async () => {
    const v = form.vin.trim().toUpperCase();
    if (v.length !== 17) { setVinError('Le VIN doit contenir exactement 17 caractères'); return; }
    setVinLoading(true); setVinError('');
    try {
      const res = await fetch(`${API}/api/vin/${v}`);
      if (!res.ok) throw new Error('VIN non trouvé');
      const data = await res.json();
      setVinSpecs(data.specs);
    } catch (e) { setVinError(e.message || 'Erreur'); }
    setVinLoading(false);
  }, [form.vin]);

  // Photo Upload
  const handlePhotos = useCallback(async (e) => {
    const files = Array.from(e.target.files).slice(0, 10 - photos.length);
    if (!files.length) return;
    setUploading(true);
    for (const file of files) {
      const fd = new FormData();
      fd.append('file', file);
      fd.append('evaluation_id', evalId);
      try {
        const res = await fetch(`${API}/api/evaluations/upload-photo`, { method: 'POST', body: fd });
        if (res.ok) {
          const data = await res.json();
          setPhotos(prev => [...prev, { url: data.url, name: file.name }]);
        }
      } catch (err) { console.error(err); }
    }
    setUploading(false);
  }, [photos, evalId]);

  // Submit
  const handleSubmit = useCallback(async () => {
    if (!form.prenom || !form.nom || !form.telephone) return;
    setSubmitting(true);
    try {
      const payload = {
        ...form,
        client_name: `${form.prenom} ${form.nom}`,
        client_phone: form.telephone,
        client_email: form.courriel,
        client_notes: form.notes_client,
        vin: form.vin.trim().toUpperCase(),
        photos: photos.map(p => p.url),
        km: form.km ? parseInt(form.km.replace(/\D/g, '')) : null,
        paiement_restant: form.solde_montant ? parseFloat(form.solde_montant.replace(/\D/g, '')) : null,
        etat_general: ETATS_GENERAL.find(e => e.value === form.etat_general)?.label || '',
        vin_decoded: vinSpecs || {},
      };
      const res = await fetch(`${API}/api/evaluations`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      if (res.ok) setSubmitted(true);
    } catch (e) { console.error(e); }
    setSubmitting(false);
  }, [form, photos, vinSpecs]);

  // Styles
  const styles = {
    page: { display: 'flex', minHeight: '100vh', background: '#0b0f19', fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif' },
    sidebar: { width: '220px', background: '#111827', borderRight: '1px solid #1f2937', padding: '1.5rem 0', flexShrink: 0, position: 'sticky', top: 0, height: '100vh', overflowY: 'auto' },
    sidebarHeader: { padding: '0 1.25rem 1.5rem', borderBottom: '1px solid #1f2937', marginBottom: '1rem' },
    sidebarLogo: { fontSize: '1.2rem', fontWeight: 800, color: '#22c55e' },
    sidebarSub: { fontSize: '0.75rem', color: '#6b7280', marginTop: '2px' },
    navItem: (active) => ({ display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '10px 1.25rem', cursor: 'pointer', fontSize: '0.9rem', fontWeight: active ? 700 : 500, color: active ? '#22c55e' : '#9ca3af', background: active ? '#1a2332' : 'transparent', borderLeft: active ? '3px solid #22c55e' : '3px solid transparent', transition: 'all 0.15s' }),
    main: { flex: 1, padding: '1.5rem 2rem', overflowY: 'auto', maxHeight: '100vh' },
    topBar: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' },
    h2: { fontSize: '1.4rem', fontWeight: 700, color: '#e5e7eb' },
    card: { background: '#111827', borderRadius: '10px', border: '1px solid #1f2937', padding: '1.5rem', marginBottom: '1.25rem' },
    cardTitle: { fontSize: '1rem', fontWeight: 700, color: '#e5e7eb', marginBottom: '1rem', display: 'flex', alignItems: 'center', gap: '0.5rem' },
    row: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' },
    row3: { display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '1rem' },
    field: { marginBottom: '1rem' },
    label: { display: 'block', fontSize: '0.78rem', fontWeight: 600, color: '#9ca3af', marginBottom: '0.35rem', textTransform: 'uppercase', letterSpacing: '0.04em' },
    input: { width: '100%', padding: '10px 12px', borderRadius: '8px', border: '1px solid #1f2937', background: '#0b0f19', color: '#e5e7eb', fontSize: '0.95rem', transition: 'border-color 0.2s' },
    select: { width: '100%', padding: '10px 12px', borderRadius: '8px', border: '1px solid #1f2937', background: '#0b0f19', color: '#e5e7eb', fontSize: '0.95rem', appearance: 'none' },
    textarea: { width: '100%', padding: '10px 12px', borderRadius: '8px', border: '1px solid #1f2937', background: '#0b0f19', color: '#e5e7eb', fontSize: '0.95rem', minHeight: '80px', resize: 'vertical' },
    btn: { padding: '10px 20px', borderRadius: '8px', border: 'none', fontSize: '0.9rem', fontWeight: 700, cursor: 'pointer' },
    btnPrimary: { background: '#22c55e', color: '#000' },
    btnSecondary: { background: '#1f2937', color: '#9ca3af', border: '1px solid #374151' },
    btnDanger: { background: '#7f1d1d', color: '#fca5a5' },
    chip: (active) => ({ display: 'inline-flex', alignItems: 'center', padding: '6px 14px', borderRadius: '20px', fontSize: '0.8rem', fontWeight: 600, cursor: 'pointer', border: active ? '2px solid #22c55e' : '2px solid #1f2937', background: active ? '#0a2e1a' : '#0b0f19', color: active ? '#22c55e' : '#9ca3af', transition: 'all 0.15s', margin: '3px' }),
    etatBar: { display: 'flex', borderRadius: '8px', overflow: 'hidden', marginTop: '0.5rem' },
    etatSegment: (c, active) => ({ flex: 1, padding: '10px 0', textAlign: 'center', cursor: 'pointer', fontSize: '0.8rem', fontWeight: 700, background: active ? c : '#1f2937', color: active ? '#000' : '#6b7280', transition: 'all 0.2s' }),
    photoGrid: { display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '0.75rem', marginTop: '0.75rem' },
    photoThumb: { width: '100%', aspectRatio: '1', objectFit: 'cover', borderRadius: '8px', border: '2px solid #1f2937' },
    specCard: { background: '#0b0f19', borderRadius: '8px', padding: '1rem', border: '1px solid #1f2937' },
    specRow: { display: 'flex', justifyContent: 'space-between', padding: '5px 0', borderBottom: '1px solid #111827' },
    toggle: (active) => ({ width: '52px', height: '28px', borderRadius: '14px', background: active === true ? '#22c55e' : active === false ? '#ef4444' : '#374151', position: 'relative', cursor: 'pointer', transition: 'all 0.2s', border: 'none' }),
    toggleDot: (active) => ({ width: '22px', height: '22px', borderRadius: '50%', background: '#fff', position: 'absolute', top: '3px', left: active ? '27px' : '3px', transition: 'all 0.2s' }),
    dommageChip: (active) => ({ padding: '6px 12px', borderRadius: '6px', fontSize: '0.78rem', fontWeight: 600, cursor: 'pointer', border: active ? '2px solid #ef4444' : '1px solid #1f2937', background: active ? '#2a0a0a' : '#0b0f19', color: active ? '#fca5a5' : '#6b7280', margin: '3px', display: 'inline-block' }),
  };

  const Toggle = ({ value, onChange }) => (
    <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
      <button style={styles.toggle(value)} onClick={() => onChange(value === true ? null : true)}>
        <div style={styles.toggleDot(value === true)} />
      </button>
      <span style={{ fontSize: '0.85rem', color: value === true ? '#22c55e' : value === false ? '#fca5a5' : '#6b7280' }}>
        {value === true ? 'Oui' : value === false ? 'Non' : '—'}
      </span>
      {value !== false && value !== null && (
        <button onClick={() => onChange(false)} style={{ ...styles.btn, ...styles.btnDanger, padding: '4px 10px', fontSize: '0.75rem' }}>Non</button>
      )}
    </div>
  );

  if (submitted) {
    return (
      <div style={{ ...styles.page, justifyContent: 'center', alignItems: 'center' }}>
        <div style={{ ...styles.card, textAlign: 'center', maxWidth: '450px' }}>
          <div style={{ fontSize: '3rem', marginBottom: '1rem' }}>✅</div>
          <h2 style={{ color: '#22c55e', marginBottom: '0.5rem' }}>Demande envoyée!</h2>
          <p style={{ color: '#9ca3af', lineHeight: 1.6 }}>Merci {form.prenom}! Daniel Giroux va analyser votre véhicule et vous revenir rapidement.</p>
          <p style={{ color: '#6b7280', fontSize: '0.85rem', marginTop: '1rem' }}>📞 418-222-3939 — Kennebec Dodge Chrysler</p>
        </div>
      </div>
    );
  }

  return (
    <div style={styles.page}>
      {/* Sidebar */}
      <div style={styles.sidebar}>
        <div style={styles.sidebarHeader}>
          <div style={styles.sidebarLogo}>KENBOT REPRISE</div>
          <div style={styles.sidebarSub}>Daniel Giroux — KDC</div>
        </div>
        {SECTIONS.map(s => (
          <div key={s.id} style={styles.navItem(section === s.id)} onClick={() => setSection(s.id)} data-testid={`nav-${s.id}`}>
            <span>{s.icon}</span>
            <span>{s.label}</span>
          </div>
        ))}
      </div>

      {/* Main */}
      <div style={styles.main}>
        <div style={styles.topBar}>
          <h2 style={styles.h2}>Nouvelle évaluation</h2>
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <button style={{ ...styles.btn, ...styles.btnSecondary }} onClick={() => setSection('client')}>Annuler</button>
            <button style={{ ...styles.btn, ...styles.btnPrimary, opacity: submitting || !form.prenom || !form.nom || !form.telephone ? 0.5 : 1 }} onClick={handleSubmit} disabled={submitting || !form.prenom || !form.nom || !form.telephone} data-testid="save-btn">
              {submitting ? 'Envoi...' : 'Sauvegarder et envoyer'}
            </button>
          </div>
        </div>

        {/* ── CLIENT ── */}
        {section === 'client' && (
          <>
            <div style={styles.card}>
              <div style={styles.cardTitle}>👤 Informations du client</div>
              <div style={styles.row}>
                <div style={styles.field}>
                  <label style={styles.label}>Prénom *</label>
                  <input style={styles.input} data-testid="prenom" value={form.prenom} onChange={e => update('prenom', e.target.value)} placeholder="Prénom" />
                </div>
                <div style={styles.field}>
                  <label style={styles.label}>Nom *</label>
                  <input style={styles.input} data-testid="nom" value={form.nom} onChange={e => update('nom', e.target.value)} placeholder="Nom" />
                </div>
              </div>
              <div style={styles.row}>
                <div style={styles.field}>
                  <label style={styles.label}>Téléphone *</label>
                  <input style={styles.input} data-testid="tel" type="tel" value={form.telephone} onChange={e => update('telephone', e.target.value)} placeholder="418-555-1234" />
                </div>
                <div style={styles.field}>
                  <label style={styles.label}>Courriel</label>
                  <input style={styles.input} type="email" value={form.courriel} onChange={e => update('courriel', e.target.value)} placeholder="email@exemple.com" />
                </div>
              </div>
            </div>
            <div style={styles.card}>
              <div style={styles.cardTitle}>📋 Détails de la transaction</div>
              <div style={styles.row3}>
                <div style={styles.field}>
                  <label style={styles.label}>Type de transaction</label>
                  <select style={styles.select} value={form.type_transaction} onChange={e => update('type_transaction', e.target.value)}>
                    <option value="">— Choisir —</option>
                    {TYPES_TRANSACTION.map(t => <option key={t} value={t}>{t}</option>)}
                  </select>
                </div>
                <div style={styles.field}>
                  <label style={styles.label}>Intérêt du client</label>
                  <select style={styles.select} value={form.interet} onChange={e => update('interet', e.target.value)}>
                    <option value="">— Choisir —</option>
                    {INTERET_CLIENT.map(t => <option key={t} value={t}>{t}</option>)}
                  </select>
                </div>
                <div style={styles.field}>
                  <label style={styles.label}>Type de client</label>
                  <select style={styles.select} value={form.type_client} onChange={e => update('type_client', e.target.value)}>
                    {TYPES_CLIENT.map(t => <option key={t} value={t}>{t}</option>)}
                  </select>
                </div>
              </div>
              <div style={styles.row}>
                <div style={styles.field}>
                  <label style={styles.label}>Provenance</label>
                  <select style={styles.select} value={form.provenance} onChange={e => update('provenance', e.target.value)}>
                    <option value="">— Choisir —</option>
                    {PROVENANCES.map(p => <option key={p} value={p}>{p}</option>)}
                  </select>
                </div>
                <div style={styles.field}>
                  <label style={styles.label}>Solde dû sur le véhicule</label>
                  <Toggle value={form.solde_du} onChange={v => update('solde_du', v)} />
                  {form.solde_du && (
                    <input style={{ ...styles.input, marginTop: '0.5rem' }} placeholder="Montant $/mois" value={form.solde_montant} onChange={e => update('solde_montant', e.target.value)} />
                  )}
                </div>
              </div>
              <div style={styles.field}>
                <label style={styles.label}>Notes sur le client</label>
                <textarea style={styles.textarea} value={form.notes_client} onChange={e => update('notes_client', e.target.value)} placeholder="Informations supplémentaires..." />
              </div>
            </div>
          </>
        )}

        {/* ── VÉHICULE ── */}
        {section === 'vehicle' && (
          <>
            <div style={styles.card}>
              <div style={styles.cardTitle}>🔍 Identification par VIN</div>
              <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'flex-end' }}>
                <div style={{ flex: 1 }}>
                  <label style={styles.label}>Numéro de série (VIN) *</label>
                  <input style={styles.input} data-testid="vin-input" value={form.vin} onChange={e => { update('vin', e.target.value.toUpperCase().slice(0, 17)); setVinError(''); }} maxLength={17} placeholder="17 caractères" />
                  <span style={{ fontSize: '0.7rem', color: '#6b7280' }}>{form.vin.length}/17</span>
                </div>
                <button style={{ ...styles.btn, ...styles.btnPrimary, marginBottom: '1rem', opacity: vinLoading ? 0.5 : 1 }} onClick={decodeVin} disabled={vinLoading} data-testid="decode-btn">
                  {vinLoading ? '...' : 'Décoder'}
                </button>
              </div>
              {vinError && <div style={{ color: '#ef4444', fontSize: '0.85rem', marginTop: '0.5rem' }}>{vinError}</div>}
            </div>

            {vinSpecs && (
              <div style={styles.card}>
                <div style={styles.cardTitle}>✅ Véhicule identifié</div>
                <div style={styles.row}>
                  <div style={styles.specCard}>
                    {[['Marque', vinSpecs.make], ['Modèle', vinSpecs.model], ['Année', vinSpecs.year], ['Trim', vinSpecs.trim], ['Carrosserie', vinSpecs.body]].filter(([,v]) => v).map(([l,v]) => (
                      <div key={l} style={styles.specRow}><span style={{ color: '#6b7280', fontSize: '0.85rem' }}>{l}</span><span style={{ color: '#e5e7eb', fontWeight: 600, fontSize: '0.85rem' }}>{v}</span></div>
                    ))}
                  </div>
                  <div style={styles.specCard}>
                    {[['Moteur', `${vinSpecs.engine_cylinders||''}cyl ${vinSpecs.engine_displacement||''}L ${vinSpecs.engine_hp||''}HP`.replace(/\s+/g,' ').trim()], ['Carburant', vinSpecs.fuel_type], ['Transmission', vinSpecs.transmission], ['Motricité', vinSpecs.drive_type], ['Fabrication', `${vinSpecs.plant_city||''} ${vinSpecs.plant_country||''}`.trim()]].filter(([,v]) => v && v !== 'cyl LHP').map(([l,v]) => (
                      <div key={l} style={styles.specRow}><span style={{ color: '#6b7280', fontSize: '0.85rem' }}>{l}</span><span style={{ color: '#e5e7eb', fontWeight: 600, fontSize: '0.85rem' }}>{v}</span></div>
                    ))}
                  </div>
                </div>
              </div>
            )}

            <div style={styles.card}>
              <div style={styles.cardTitle}>📊 Détails supplémentaires</div>
              <div style={styles.row}>
                <div style={styles.field}>
                  <label style={styles.label}>Kilométrage *</label>
                  <input style={styles.input} data-testid="km" value={form.km} onChange={e => update('km', e.target.value)} placeholder="Ex: 85000" />
                </div>
                <div style={styles.field}>
                  <label style={styles.label}>Nombre de clés</label>
                  <select style={styles.select} value={form.nombre_cles} onChange={e => update('nombre_cles', e.target.value)}>
                    {['0', '1', '2', '3+'].map(n => <option key={n} value={n}>{n}</option>)}
                  </select>
                </div>
              </div>
              <div style={styles.row}>
                <div style={styles.field}>
                  <label style={styles.label}>Couleur extérieure</label>
                  <select style={styles.select} value={form.couleur_ext} onChange={e => update('couleur_ext', e.target.value)}>
                    <option value="">— Choisir —</option>
                    {COULEURS_EXT.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <div style={styles.field}>
                  <label style={styles.label}>Couleur intérieure</label>
                  <select style={styles.select} value={form.couleur_int} onChange={e => update('couleur_int', e.target.value)}>
                    <option value="">— Choisir —</option>
                    {COULEURS_INT.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
              </div>
              <div style={styles.field}>
                <label style={styles.label}>Avez-vous vu le véhicule en personne?</label>
                <Toggle value={form.vu_vehicule} onChange={v => update('vu_vehicule', v)} />
              </div>
            </div>
          </>
        )}

        {/* ── OPTIONS ── */}
        {section === 'options' && (
          <>
            {VEHICLE_OPTIONS.map(cat => (
              <div key={cat.cat} style={styles.card}>
                <div style={styles.cardTitle}>{cat.cat}</div>
                <div style={{ display: 'flex', flexWrap: 'wrap' }}>
                  {cat.items.map(opt => (
                    <span key={opt} style={styles.chip(form.options.includes(opt))} onClick={() => toggleOption(opt)}>
                      {form.options.includes(opt) ? '✅ ' : ''}{opt}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </>
        )}

        {/* ── ÉTAT ── */}
        {section === 'etat' && (
          <>
            <div style={styles.card}>
              <div style={styles.cardTitle}>🔍 État général du véhicule</div>
              <label style={styles.label}>Sélectionnez l'état général</label>
              <div style={styles.etatBar}>
                {ETATS_GENERAL.map(e => (
                  <div key={e.value} style={styles.etatSegment(e.color, form.etat_general >= e.value)} onClick={() => update('etat_general', e.value)} data-testid={`etat-${e.value}`}>
                    {e.label}
                  </div>
                ))}
              </div>
            </div>
            <div style={styles.card}>
              <div style={styles.cardTitle}>🪟 État du pare-brise</div>
              <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                {ETATS_PAREBRISE.map(e => (
                  <span key={e} style={styles.chip(form.etat_parebrise === e)} onClick={() => update('etat_parebrise', e)}>{e}</span>
                ))}
              </div>
            </div>
            <div style={styles.card}>
              <div style={styles.cardTitle}>🔧 Dommages carrosserie</div>
              <p style={{ color: '#6b7280', fontSize: '0.8rem', marginBottom: '0.75rem' }}>Sélectionnez les zones endommagées</p>
              <div style={{ display: 'flex', flexWrap: 'wrap' }}>
                {ZONES_DOMMAGES.map(z => (
                  <span key={z} style={styles.dommageChip(form.dommages.includes(z))} onClick={() => toggleDommage(z)}>{form.dommages.includes(z) ? '❌ ' : ''}{z}</span>
                ))}
              </div>
            </div>
            <div style={styles.card}>
              <div style={styles.cardTitle}>⚙️ État mécanique</div>
              <textarea style={styles.textarea} value={form.etat_mecanique} onChange={e => update('etat_mecanique', e.target.value)} placeholder="Bruits, problèmes connus, entretien récent, etc." />
            </div>
          </>
        )}

        {/* ── PHOTOS ── */}
        {section === 'photos' && (
          <div style={styles.card}>
            <div style={styles.cardTitle}>📸 Photos du véhicule ({photos.length}/10)</div>
            <p style={{ color: '#6b7280', fontSize: '0.8rem', marginBottom: '1rem' }}>Extérieur (4 côtés), intérieur, tableau de bord, odomètre, défauts visibles</p>
            <input ref={fileRef} type="file" accept="image/*" multiple onChange={handlePhotos} style={{ display: 'none' }} data-testid="photo-input" />
            <button style={{ ...styles.btn, ...styles.btnSecondary, width: '100%' }} onClick={() => fileRef.current?.click()}>
              {uploading ? 'Envoi en cours...' : '📷 Ajouter des photos'}
            </button>
            {photos.length > 0 && (
              <div style={styles.photoGrid}>
                {photos.map((p, i) => (
                  <div key={i} style={{ position: 'relative' }}>
                    <img src={p.url} alt={p.name} style={styles.photoThumb} />
                    <button onClick={() => setPhotos(prev => prev.filter((_, j) => j !== i))} style={{ position: 'absolute', top: 4, right: 4, background: '#ef4444', color: '#fff', border: 'none', borderRadius: '50%', width: '22px', height: '22px', fontSize: '0.7rem', cursor: 'pointer' }}>✕</button>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* ── GARANTIES ── */}
        {section === 'garanties' && (
          <>
            <div style={styles.card}>
              <div style={styles.cardTitle}>🛡️ Garantie du constructeur</div>
              <label style={styles.label}>La garantie constructeur est-elle encore valide?</label>
              <Toggle value={form.garantie_constructeur} onChange={v => update('garantie_constructeur', v)} />
              {form.garantie_constructeur && (
                <div style={{ ...styles.field, marginTop: '0.75rem' }}>
                  <label style={styles.label}>Date d'expiration</label>
                  <input style={styles.input} type="date" value={form.garantie_constructeur_date} onChange={e => update('garantie_constructeur_date', e.target.value)} />
                </div>
              )}
            </div>
            <div style={styles.card}>
              <div style={styles.cardTitle}>🛡️ Garantie prolongée</div>
              <label style={styles.label}>Y a-t-il une garantie prolongée?</label>
              <Toggle value={form.garantie_prolongee} onChange={v => update('garantie_prolongee', v)} />
              {form.garantie_prolongee && (
                <div style={{ ...styles.field, marginTop: '0.75rem' }}>
                  <label style={styles.label}>Détails de la garantie</label>
                  <textarea style={styles.textarea} value={form.garantie_prolongee_detail} onChange={e => update('garantie_prolongee_detail', e.target.value)} placeholder="Fournisseur, couverture, date expiration..." />
                </div>
              )}
            </div>
          </>
        )}

        {/* ── NOTES ── */}
        {section === 'notes' && (
          <div style={styles.card}>
            <div style={styles.cardTitle}>📝 Commentaires et notes</div>
            <textarea style={{ ...styles.textarea, minHeight: '200px' }} value={form.commentaires} onChange={e => update('commentaires', e.target.value)} placeholder="Historique d'accidents, réparations effectuées, raison de la vente, particularités du véhicule..." />
          </div>
        )}
      </div>
    </div>
  );
}
