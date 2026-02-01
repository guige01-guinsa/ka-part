# ka-part deployment (Windows)
Set-Location $PSScriptRoot\..

$ProjectRoot = (Get-Location).Path
$VenvPy = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$CaddyDir = Join-Path $ProjectRoot "tools\caddy"
$CaddyExe = Join-Path $CaddyDir "caddy.exe"
$Caddyfile = Join-Path $ProjectRoot "ops\Caddyfile"

if (!(Test-Path $VenvPy)) {
  Write-Host ".venv not found. Create it first." -ForegroundColor Red
  exit 1
}

& $VenvPy -m pip install -r requirements.txt

# Apply migrations
& sqlite3 $ProjectRoot\ka.db ".read $ProjectRoot\sql\migrations\20260201_outsourcing_and_notifications.sql"
& sqlite3 $ProjectRoot\ka.db ".read $ProjectRoot\sql\migrations\20260201_notification_templates.sql"

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
