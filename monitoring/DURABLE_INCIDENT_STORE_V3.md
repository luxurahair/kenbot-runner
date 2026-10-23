# Contrat de stockage durable — Kenbot V3 (proposition, non déployée)

## Décision d'architecture

Utiliser une base PostgreSQL gérée pour l'historique et les verrous, dans un **schéma privé distinct** `kenbot_control` non exposé à l'API publique. Si Supabase est retenu, vérifier d'abord l'identité du projet, la politique d'accès et les migrations existantes; ne pas réutiliser par défaut les tables clients ou les droits service-role d'eTradeValue.

Aucune table, migration, permission ni donnée de production n'est modifiée par ce document.

## Modèle de données proposé

### `incidents`
- `incident_id uuid primary key`
- `service_id text not null`, `environment text not null`, `tenant_scope text not null` (isolation explicite)
- `fingerprint text not null` (empreinte d'erreur expurgée)
- `state text` (OPEN, OBSERVING, DIAGNOSING, BLOCKED, RESOLVED)
- `first_seen timestamptz`, `last_seen timestamptz`, `resolved_at timestamptz null`
- `current_sha text null`, `evidence_refs jsonb` (identifiants de logs, pas de secrets)
- Contrainte unique partielle sur incident ouvert par `service_id/environment/tenant_scope/fingerprint`.

### `probe_observations`
- `observation_id uuid primary key`, `incident_id uuid references incidents`
- `observed_at timestamptz`, `source text`, `status text`, `http_status int null`, `commit_sha text null`
- `evidence_digest text`, `created_at timestamptz default now()`
- Append-only; idempotence par identifiant d'événement source; TTL et rétention définis par politique.

### `recovery_attempts`
- `attempt_id uuid primary key`, `incident_id uuid references incidents`
- `service_id text`, `action text`, `runbook_version text`, `executor_identity text`
- `approval_id text null`, `lease_id uuid`, `started_at timestamptz`, `finished_at timestamptz null`
- `result text` (STARTED, VERIFIED, FAILED, UNKNOWN), `verification_refs jsonb`
- Append-only, y compris pour tentative interrompue ou réponse inconnue; compter les tentatives commencées, pas seulement celles réussies.

### `recovery_leases`
- Clé primaire `service_id/environment/tenant_scope`; `lease_id uuid`, `owner text`
- `expires_at timestamptz`, `heartbeat_at timestamptz`, `fencing_token bigint`
- Acquisition **atomique** en transaction, avec expiration et numéro de génération monotone.
- Une reprise après expiration doit vérifier le `fencing_token` avant toute action. Ne jamais utiliser un simple fichier JSON GitHub comme verrou.

### `incident_events`
- `event_id uuid primary key`, `incident_id`, `timestamp`, `event_type`, `actor`, `payload_redacted jsonb`, `correlation_id`
- Journal append-only; pas de données personnelles clients, tokens, mots de passe ou logs non expurgés.

## Contrat d'accès

1. **Reader** : compte machine en lecture seule pour les observations et compteurs; secrets uniquement côté serveur.
2. **Writer** : identité distincte autorisée à enregistrer observations/incidents, pas à lancer un déploiement.
3. **Executor** : identité à permissions minimales, liste blanche de services/actions, validation indépendante des preuves et de l'approbation.
4. **Auditor** : lecture des événements expurgés, sans droit d'écriture.
5. Le frontend et les agents non fiables n'accèdent jamais directement aux tables privées; aucun `service_role` dans le navigateur ou les artefacts CI.

## Contrat de lecture vérifiée

Le fichier d'entrée V3 actuel contient `history.verified`, **qui est seulement une affirmation de l'appelant**. Il ne constitue pas une preuve. Avant d'autoriser un diagnostic automatisé ou une réparation, le lecteur de l'historique doit :
- interroger la base avec son identité authentifiée;
- lier le résultat à `service_id`, `environment`, `tenant_scope`, `incident_id`;
- compter les tentatives de la fenêtre glissante de 24 heures et calculer le dernier essai;
- rejeter les données absentes, périmées, incohérentes ou les lectures partielles;
- transmettre ces valeurs au moteur V2 dans le **même domaine de confiance**, sans se fier à une valeur JSON modifiable.

Un simple `verified=true` dans un artefact GitHub ne doit jamais activer une réparation. L'interface `decision_adapter.py` reste en lecture seule et émet `execution_allowed=false`.

## Exécution sûre

1. Deux observations réelles distinctes, horodatées, de panne confirmée.
2. Validation de l'identité et de l'état réel du service; les erreurs 401/403/404/429 ne redémarrent pas.
3. Transaction : acquisition du lease + comptage des tentatives + inscription de la tentative (tout ou rien).
4. Vérification juste avant l'action, action unique idempotente et ciblée.
5. Vérification de santé après action; si l'état est inconnu, arrêter et escalader.
6. Libérer le lease avec le bon jeton; expiration contrôlée si crash.
7. Conserver la preuve de résultat et une trace des faux positifs.

## Tests d'acceptation avant migration réelle

- Deux runners simultanés : **une seule** tentative possible.
- Crash avant/après appel Render : pas de répétition aveugle.
- Réseau interrompu pendant écriture : échec fermé.
- Fenêtre glissante 24 h, cooldown 1 h, expiration lease et fencing.
- Séparation stricte entre concessions/tenants.
- Secret absent, identité incorrecte, observation forgée : aucune action.
- Incident résolu entre deux sondes : aucune action.
- Journal immuable et expurgé; vérification post-action obligatoire.
- Test de restauration de la base d'incidents dans un environnement isolé.

## Phasage

A. Valider le schéma et le projet cible sans créer de table.
B. Migration revue, contrôles d'accès, tests d'isolation et de concurrence en environnement isolé.
C. Lecteur d'historique authentifié et tests contractuels.
D. Exécuteur limité aux services explicitement autorisés, canary non critique.
E. Intégration Codex/Mac Rescue par commandes déjà autorisées, sans shell générique, avec journalisation et contrôle de résultat.

**État actuel : conception uniquement.** La PR #7 n'installe pas ces tables et n'active pas les réparations.
