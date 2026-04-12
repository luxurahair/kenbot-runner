import React, { useState, useCallback } from 'react';
import { API } from '../App';

const ETATS = ['Excellent', 'Bon', 'Passable', 'À réparer'];

export default function PublicForm() {
  const [step, setStep] = useState(1); // 1=VIN, 2=Photos, 3=Infos, 4=Confirmation
  const [vin, setVin] = useState('');
  const [vinSpecs, setVinSpecs] = useState(null);
  const [vinLoading, setVinLoading] = useState(false);
  const [vinError, setVinError] = useState('');
  const [photos, setPhotos] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [form, setForm] = useState({ km: '', paiement_restant: '', etat_general: 'Bon', client_name: '', client_phone: '', client_email: '', client_notes: '' });
  const [submitted, setSubmitted] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [evalId] = useState(() => crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2));

  // ── VIN Decode ──
  const decodeVin = useCallback(async () => {
    const v = vin.trim().toUpperCase();
    if (v.length !== 17) { setVinError('Le VIN doit contenir exactement 17 caractères'); return; }
    setVinLoading(true); setVinError('');
    try {
      const res = await fetch(`${API}/api/vin/${v}`);
      if (!res.ok) throw new Error('VIN non trouvé');
      const data = await res.json();
      setVinSpecs(data.specs);
      setStep(2);
    } catch (e) { setVinError(e.message || 'Erreur de décodage'); }
    setVinLoading(false);
  }, [vin]);

  // ── Photo Upload ──
  const handlePhotos = useCallback(async (e) => {
    const files = Array.from(e.target.files).slice(0, 10 - photos.length);
    if (!files.length) return;
    setUploading(true);
    for (const file of files) {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('evaluation_id', evalId);
      try {
        const res = await fetch(`${API}/api/evaluations/upload-photo`, { method: 'POST', body: formData });
        if (res.ok) {
          const data = await res.json();
          setPhotos(prev => [...prev, { url: data.url, name: file.name }]);
        }
      } catch (err) { console.error('Upload failed:', err); }
    }
    setUploading(false);
  }, [photos, evalId]);

  const removePhoto = (idx) => setPhotos(prev => prev.filter((_, i) => i !== idx));

  // ── Submit ──
  const handleSubmit = useCallback(async () => {
    if (!form.client_name || !form.client_phone) return;
    setSubmitting(true);
    try {
      const payload = {
        ...form,
        vin: vin.trim().toUpperCase(),
        photos: photos.map(p => p.url),
        km: form.km ? parseInt(form.km.replace(/\D/g, '')) : null,
        paiement_restant: form.paiement_restant ? parseFloat(form.paiement_restant.replace(/\D/g, '')) : null,
      };
      const res = await fetch(`${API}/api/evaluations`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      if (res.ok) setSubmitted(true);
    } catch (e) { console.error('Submit error:', e); }
    setSubmitting(false);
  }, [form, vin, photos]);

  // ── Styles ──
  const s = {
    page: { minHeight: '100vh', background: 'linear-gradient(180deg, #0a0a1a 0%, #111827 100%)', padding: '0 1rem 3rem' },
    header: { textAlign: 'center', padding: '2rem 0 1.5rem' },
    logo: { fontSize: '2rem', fontWeight: 800, color: '#22c55e', letterSpacing: '-0.02em' },
    subtitle: { color: '#9ca3af', fontSize: '0.9rem', marginTop: '0.25rem' },
    card: { background: '#1a1a2e', borderRadius: '12px', padding: '1.5rem', maxWidth: '500px', margin: '0 auto 1rem' },
    label: { display: 'block', color: '#9ca3af', fontSize: '0.8rem', fontWeight: 600, marginBottom: '0.4rem', textTransform: 'uppercase', letterSpacing: '0.05em' },
    input: { width: '100%', padding: '12px 14px', borderRadius: '8px', border: '1px solid #374151', background: '#111827', color: '#e5e7eb', fontSize: '1rem' },
    textarea: { width: '100%', padding: '12px 14px', borderRadius: '8px', border: '1px solid #374151', background: '#111827', color: '#e5e7eb', fontSize: '1rem', minHeight: '80px', resize: 'vertical' },
    btn: { width: '100%', padding: '14px', borderRadius: '8px', border: 'none', fontSize: '1rem', fontWeight: 700, cursor: 'pointer', transition: 'all 0.2s' },
    btnPrimary: { background: '#22c55e', color: '#000' },
    btnSecondary: { background: '#1f2937', color: '#9ca3af', border: '1px solid #374151' },
    specRow: { display: 'flex', justifyContent: 'space-between', padding: '6px 0', borderBottom: '1px solid #1f2937' },
    specLabel: { color: '#9ca3af', fontSize: '0.85rem' },
    specValue: { color: '#e5e7eb', fontSize: '0.85rem', fontWeight: 600 },
    steps: { display: 'flex', justifyContent: 'center', gap: '0.5rem', marginBottom: '1.5rem' },
    stepDot: (active) => ({ width: '10px', height: '10px', borderRadius: '50%', background: active ? '#22c55e' : '#374151', transition: 'all 0.3s' }),
    photoGrid: { display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '0.5rem', marginTop: '0.75rem' },
    photoThumb: { width: '100%', aspectRatio: '1', objectFit: 'cover', borderRadius: '8px', border: '1px solid #374151' },
    select: { width: '100%', padding: '12px 14px', borderRadius: '8px', border: '1px solid #374151', background: '#111827', color: '#e5e7eb', fontSize: '1rem', appearance: 'none' },
  };

  // ── SUBMITTED ──
  if (submitted) {
    return (
      <div style={s.page}>
        <div style={s.header}>
          <div style={s.logo}>KENBOT REPRISE</div>
        </div>
        <div style={{ ...s.card, textAlign: 'center' }}>
          <div style={{ fontSize: '3rem', marginBottom: '1rem' }}>✅</div>
          <h2 style={{ color: '#22c55e', marginBottom: '0.5rem' }}>Demande envoyée!</h2>
          <p style={{ color: '#9ca3af', lineHeight: 1.6 }}>
            Merci {form.client_name}! Daniel Giroux va analyser votre véhicule et vous revenir rapidement.
          </p>
          <p style={{ color: '#6b7280', fontSize: '0.85rem', marginTop: '1rem' }}>
            📞 418-222-3939 — Kennebec Dodge Chrysler, Saint-Georges
          </p>
        </div>
      </div>
    );
  }

  return (
    <div style={s.page}>
      {/* Header */}
      <div style={s.header}>
        <div style={s.logo}>KENBOT REPRISE</div>
        <div style={s.subtitle}>Évaluez votre véhicule en 3 étapes</div>
        <div style={{ ...s.subtitle, fontSize: '0.8rem' }}>Daniel Giroux — Kennebec Dodge Chrysler</div>
      </div>

      {/* Steps */}
      <div style={s.steps}>
        {[1, 2, 3].map(n => <div key={n} style={s.stepDot(step >= n)} />)}
      </div>

      {/* STEP 1: VIN */}
      {step === 1 && (
        <div style={s.card} data-testid="step-vin">
          <h3 style={{ color: '#e5e7eb', marginBottom: '1rem', fontSize: '1.1rem' }}>🔍 Entrez le VIN de votre véhicule</h3>
          <label style={s.label}>Numéro de série (VIN) *</label>
          <input
            data-testid="vin-input"
            style={s.input}
            placeholder="Ex: 1C6SRFST2KN527799"
            value={vin}
            onChange={e => { setVin(e.target.value.toUpperCase().slice(0, 17)); setVinError(''); }}
            maxLength={17}
          />
          <div style={{ color: '#6b7280', fontSize: '0.75rem', marginTop: '0.25rem' }}>{vin.length}/17 caractères</div>
          {vinError && <div style={{ color: '#ef4444', fontSize: '0.85rem', marginTop: '0.5rem' }}>{vinError}</div>}

          <button
            data-testid="decode-btn"
            style={{ ...s.btn, ...s.btnPrimary, marginTop: '1rem', opacity: vinLoading ? 0.6 : 1 }}
            onClick={decodeVin}
            disabled={vinLoading}
          >
            {vinLoading ? 'Décodage en cours...' : 'Décoder le VIN'}
          </button>

          <div style={{ color: '#6b7280', fontSize: '0.75rem', marginTop: '1rem', textAlign: 'center' }}>
            Le VIN se trouve sur le tableau de bord (côté conducteur) ou sur la portière du conducteur.
          </div>
        </div>
      )}

      {/* STEP 2: Photos + VIN Results */}
      {step === 2 && vinSpecs && (
        <div data-testid="step-photos">
          {/* VIN Specs Card */}
          <div style={s.card}>
            <h3 style={{ color: '#22c55e', marginBottom: '1rem', fontSize: '1.1rem' }}>✅ Véhicule identifié</h3>
            {[
              ['Marque', vinSpecs.make],
              ['Modèle', vinSpecs.model],
              ['Année', vinSpecs.year],
              ['Trim', vinSpecs.trim],
              ['Carrosserie', vinSpecs.body],
              ['Moteur', `${vinSpecs.engine_cylinders || ''}cyl ${vinSpecs.engine_displacement || ''}L ${vinSpecs.engine_hp || ''}HP`.trim()],
              ['Carburant', vinSpecs.fuel_type],
              ['Transmission', vinSpecs.transmission],
              ['Motricité', vinSpecs.drive_type],
            ].filter(([, v]) => v && v.trim() && v.trim() !== 'cyl LHP').map(([label, value]) => (
              <div key={label} style={s.specRow}>
                <span style={s.specLabel}>{label}</span>
                <span style={s.specValue}>{value}</span>
              </div>
            ))}
          </div>

          {/* Photos */}
          <div style={s.card}>
            <h3 style={{ color: '#e5e7eb', marginBottom: '0.5rem', fontSize: '1.1rem' }}>📸 Photos de votre véhicule</h3>
            <p style={{ color: '#6b7280', fontSize: '0.8rem', marginBottom: '1rem' }}>
              Extérieur (4 côtés), intérieur, tableau de bord, défauts. Max 10 photos.
            </p>
            <input
              data-testid="photo-upload"
              type="file"
              accept="image/*"
              multiple
              onChange={handlePhotos}
              style={{ display: 'none' }}
              id="photo-input"
            />
            <label htmlFor="photo-input" style={{ ...s.btn, ...s.btnSecondary, display: 'block', textAlign: 'center', cursor: 'pointer' }}>
              {uploading ? 'Envoi en cours...' : `📷 Ajouter des photos (${photos.length}/10)`}
            </label>
            {photos.length > 0 && (
              <div style={s.photoGrid}>
                {photos.map((p, i) => (
                  <div key={i} style={{ position: 'relative' }}>
                    <img src={p.url} alt={p.name} style={s.photoThumb} />
                    <button
                      onClick={() => removePhoto(i)}
                      style={{ position: 'absolute', top: '2px', right: '2px', background: '#ef4444', color: '#fff', border: 'none', borderRadius: '50%', width: '20px', height: '20px', fontSize: '0.7rem', cursor: 'pointer' }}
                    >✕</button>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div style={{ ...s.card, display: 'flex', gap: '0.5rem' }}>
            <button style={{ ...s.btn, ...s.btnSecondary, flex: 1 }} onClick={() => setStep(1)}>← Retour</button>
            <button data-testid="next-step3" style={{ ...s.btn, ...s.btnPrimary, flex: 2 }} onClick={() => setStep(3)}>Continuer →</button>
          </div>
        </div>
      )}

      {/* STEP 3: Info Client */}
      {step === 3 && (
        <div data-testid="step-info">
          <div style={s.card}>
            <h3 style={{ color: '#e5e7eb', marginBottom: '1rem', fontSize: '1.1rem' }}>📝 Informations</h3>

            <label style={s.label}>Kilométrage actuel *</label>
            <input data-testid="km-input" style={s.input} placeholder="Ex: 85000" value={form.km} onChange={e => setForm(f => ({ ...f, km: e.target.value }))} />

            <label style={{ ...s.label, marginTop: '1rem' }}>Paiement restant ($/mois)</label>
            <input style={s.input} placeholder="Ex: 450" value={form.paiement_restant} onChange={e => setForm(f => ({ ...f, paiement_restant: e.target.value }))} />

            <label style={{ ...s.label, marginTop: '1rem' }}>État général *</label>
            <select data-testid="etat-select" style={s.select} value={form.etat_general} onChange={e => setForm(f => ({ ...f, etat_general: e.target.value }))}>
              {ETATS.map(e => <option key={e} value={e}>{e}</option>)}
            </select>

            <label style={{ ...s.label, marginTop: '1rem' }}>Notes (dommages, historique, etc.)</label>
            <textarea style={s.textarea} placeholder="Décrivez l'état du véhicule, historique d'accidents, etc." value={form.client_notes} onChange={e => setForm(f => ({ ...f, client_notes: e.target.value }))} />
          </div>

          <div style={s.card}>
            <h3 style={{ color: '#e5e7eb', marginBottom: '1rem', fontSize: '1.1rem' }}>👤 Vos coordonnées</h3>

            <label style={s.label}>Votre nom *</label>
            <input data-testid="name-input" style={s.input} placeholder="Prénom Nom" value={form.client_name} onChange={e => setForm(f => ({ ...f, client_name: e.target.value }))} />

            <label style={{ ...s.label, marginTop: '1rem' }}>Téléphone *</label>
            <input data-testid="phone-input" style={s.input} placeholder="418-555-1234" type="tel" value={form.client_phone} onChange={e => setForm(f => ({ ...f, client_phone: e.target.value }))} />

            <label style={{ ...s.label, marginTop: '1rem' }}>Courriel</label>
            <input style={s.input} placeholder="email@exemple.com" type="email" value={form.client_email} onChange={e => setForm(f => ({ ...f, client_email: e.target.value }))} />
          </div>

          <div style={{ ...s.card, display: 'flex', gap: '0.5rem' }}>
            <button style={{ ...s.btn, ...s.btnSecondary, flex: 1 }} onClick={() => setStep(2)}>← Retour</button>
            <button
              data-testid="submit-btn"
              style={{ ...s.btn, ...s.btnPrimary, flex: 2, opacity: (!form.client_name || !form.client_phone || submitting) ? 0.5 : 1 }}
              onClick={handleSubmit}
              disabled={!form.client_name || !form.client_phone || submitting}
            >
              {submitting ? 'Envoi...' : 'Envoyer ma demande'}
            </button>
          </div>

          <div style={{ textAlign: 'center', color: '#6b7280', fontSize: '0.75rem', marginTop: '0.5rem', maxWidth: '500px', margin: '0.5rem auto 0' }}>
            En soumettant, vous autorisez Daniel Giroux à vous contacter pour évaluer votre véhicule.
          </div>
        </div>
      )}
    </div>
  );
}
