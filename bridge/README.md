# DATEV-Bridge

HTTP-Reverse-Proxy für DATEVconnect, der den lokalen Service auf
`localhost:58454` über die Tailscale-IP des Rechners erreichbar macht.

## Warum nicht einfach `tailscale serve`?

Tailscale Serve forwarded TCP roh weiter. Die Microsoft HTTP-API
(`http.sys`), die DATEVconnect benutzt, akzeptiert aber nur Verbindungen
mit Source-IP `127.0.0.1` und schließt alle anderen mit TCP-RST — bevor
HTTP-Auth überhaupt zum Zug kommt. Tested 2026-04-25.

Diese Bridge nimmt eingehende HTTP-Anfragen aus dem Tailnet, **stellt
sie neu** als ausgehende Anfrage von `127.0.0.1` an `localhost:58454`,
hängt Basic-Auth dran, gibt die Antwort zurück. Damit ist die Source-IP
für `http.sys` immer `127.0.0.1` — kein Reset.

## Installation

Auf dem LuG-PC, **PowerShell als Administrator**:

```powershell
cd <Verzeichnis-mit-Bridge-Dateien>
.\install.ps1
```

Der Installer fragt einmal nach Windows-Username (Email bei MS-Konto)
und Passwort, schreibt die in `C:\datev-bridge\bridge.env` (mit
restriktiven Datei-Rechten), legt eine Firewall-Regel an (nur das
Tailscale-Interface darf rein) und richtet einen Scheduled Task ein, der
beim Boot startet und im Fehlerfall automatisch neu hochfährt.

## Was läuft danach

- `C:\datev-bridge\datev_bridge.py` — die Bridge
- `C:\datev-bridge\bridge.env` — Credentials (nur SYSTEM/Administrators lesbar)
- `C:\datev-bridge\run-bridge.bat` — Wrapper, der die env lädt und Python startet
- `C:\datev-bridge\bridge.log` — Logs (rotiert bei 5MB, 3 Backups)
- Scheduled Task `datev-bridge` läuft als `SYSTEM`, beim Boot, restart bei Absturz

## Wartung

Status prüfen:
```powershell
Get-ScheduledTask -TaskName datev-bridge
Get-Content C:\datev-bridge\bridge.log -Tail 20
```

Restart:
```powershell
Stop-ScheduledTask -TaskName datev-bridge
Start-ScheduledTask -TaskName datev-bridge
```

Passwort-Update (nach Windows-Passwort-Wechsel):
```powershell
.\install.ps1 -Force
```

## 503-Antworten

Wenn die Bridge `503 Service Unavailable` zurückgibt mit `"DATEV LuG
nicht erreichbar"` — das heißt: DATEV ist gerade nicht offen oder Stick
ist raus. Die Bridge selbst läuft normal weiter, Du musst nur LuG starten.
