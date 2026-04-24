# datev-buchungstool

Internes DATEV-Buchungstool der Fröhlich Dienste. Eigenständiger FastAPI-Service,
integriert in die bestehende Admin-Oberfläche unter `admin.froehlichdienste.de/admin/buchhaltung`.

## Architektur

- **API**: FastAPI auf `buchhaltung-api.froehlichdienste.de`
- **DB**: Eigene Postgres-Datenbank `datev_buchhaltung` im geteilten `fzr_postgres`-Container
- **Auth**: Geteilte JWT-Sessions mit FrohZeitRakete (gemeinsames `SECRET_KEY`, Cookie `fz_access_token` auf `.froehlichdienste.de`)
- **Frontend**: Erweiterung der bestehenden Next.js-Admin-App in [lucasterix/frohzeitrakete](https://github.com/lucasterix/frohzeitrakete) unter `apps/admin-web/app/admin/buchhaltung/`

## Roadmap

| Phase | Inhalt | Status |
|---|---|---|
| 0 | Grundgerüst, Menüpunkt, leere Tabs, Deployment | in Arbeit |
| 1 | Mitarbeiter-Stammdaten, DATEV Lohn+Gehalt Export | offen |
| 2 | Bank-API-Anbindung, Auto-Buchungen | offen |
| 3 | Buchungen, DATEV Rechnungswesen Export | offen |
| 4 | Bescheinigungen, Monatsabschluss | offen |
| 5 | Patti-Sync, Rechnungsstellung | offen |

## Lokal starten

```bash
cp .env.example .env    # Werte ausfüllen
docker compose up --build
# http://localhost:8000/health
```

## Deployment

Production deploy läuft via GitHub Actions auf `push` nach `main` → SSH zu `deploy@46.224.7.46`, pull & `docker compose up -d --build`.
