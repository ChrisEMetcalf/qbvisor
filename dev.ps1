Write-Host "`n Setting up Python environment with Poetry..." -ForegroundColor Cyan

# Check if Poetry is installed
if (-not (Get-Command poetry -ErrorAction SilentlyContinue)) {
    Write-Host "`n Poetry is not installed. Please install it first:" -ForegroundColor Red
    Write-Host "   https://python-poetry.org/docs/#installation`n"
    exit 1
}

# Install dependencies
poetry install

Write-Host "`n Python environment setup complete." -ForegroundColor Green
Write-Host "`n Next steps:" -ForegroundColor Yellow
Write-Host "   • To activate the environment:    poetry shell"
Write-Host "   • To run your app directly:       poetry run python app.py`n"
