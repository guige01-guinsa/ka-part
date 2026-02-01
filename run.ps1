# run.ps1 (policy-friendly)
Set-Location $PSScriptRoot

$py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (!(Test-Path $py)) {
  Write-Host "❌ .venv\Scripts\python.exe not found. Create venv first." -ForegroundColor Red
  exit 1
}

& $py -m uvicorn app.main:app --host 0.0.0.0 --port 8000
