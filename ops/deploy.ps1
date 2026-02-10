# ka-part deployment (Windows)
Set-Location $PSScriptRoot\..

$ProjectRoot = (Get-Location).Path
$VenvPy = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$ParkingRoot = Join-Path $ProjectRoot "services\parking"
$ParkingEnv = Join-Path $ParkingRoot ".env.production"
$CaddyDir = Join-Path $ProjectRoot "tools\caddy"
$CaddyExe = Join-Path $CaddyDir "caddy.exe"
$Caddyfile = Join-Path $ProjectRoot "ops\Caddyfile"

if (!(Test-Path $VenvPy)) {
  Write-Host ".venv not found. Create it first." -ForegroundColor Red
  exit 1
}

& $VenvPy -m pip install -r requirements.txt
& $VenvPy -m uvicorn --version | Out-Null
if (!(Test-Path $ParkingEnv)) {
  Copy-Item (Join-Path $ParkingRoot ".env.production.example") $ParkingEnv -Force
  Write-Host "Created services\\parking\\.env.production (set strong secrets before production)." -ForegroundColor Yellow
}

# Apply migrations (sorted, idempotent scripts 권장)
$MigDir = Join-Path $ProjectRoot "sql\migrations"
if (Test-Path $MigDir) {
  $runner = @'
import pathlib
import sqlite3
import sys

db_path = pathlib.Path(sys.argv[1])
mig_dir = pathlib.Path(sys.argv[2])

conn = sqlite3.connect(str(db_path))
try:
    for p in sorted(mig_dir.glob("*.sql")):
        print(f"Applying migration: {p.name}")
        sql = p.read_text(encoding="utf-8")
        try:
            conn.executescript(sql)
            conn.commit()
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "duplicate column name" in msg or "already exists" in msg:
                conn.rollback()
                print(f"  skip non-fatal: {e}")
                continue
            raise
finally:
    conn.close()
'@
  $tmpRunner = Join-Path $env:TEMP "ka-part-migrate.py"
  Set-Content -Path $tmpRunner -Value $runner -Encoding UTF8
  & $VenvPy $tmpRunner "$ProjectRoot\ka.db" "$MigDir"
  Remove-Item $tmpRunner -Force -ErrorAction SilentlyContinue
}

# Download Caddy if missing (direct exe)
if (!(Test-Path $CaddyExe)) {
  New-Item -ItemType Directory -Force -Path $CaddyDir | Out-Null
  Invoke-WebRequest -Uri "https://caddyserver.com/api/download?os=windows&arch=amd64" -OutFile $CaddyExe
}

# Windows Firewall: allow 80/443 (requires admin)
# New-NetFirewallRule -DisplayName "ka-part-http" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 80
# New-NetFirewallRule -DisplayName "ka-part-https" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 443

# Create services
$ApiSvc = "ka-part-api"
$WebSvc = "ka-part-web"

# Service creation requires admin. Run these in an elevated shell:
# sc.exe create $ApiSvc binPath= "\"$VenvPy\" -m uvicorn app.main:app --host 127.0.0.1 --port 8000" start= auto
# sc.exe create $WebSvc binPath= "\"$CaddyExe\" run --config \"$Caddyfile\"" start= auto
# sc.exe config $ApiSvc obj= .\\YOURUSER password= YOURPASS
# sc.exe config $WebSvc obj= .\\YOURUSER password= YOURPASS
# Start-Service $ApiSvc
# Start-Service $WebSvc

Write-Host "Deploy complete."
