@echo off
chcp 65001 >nul
title TikTok LIVE API
cd /d "%~dp0"

where node >nul 2>nul
if errorlevel 1 (
  echo Node.js kurulu degil. https://nodejs.org adresinden kurup tekrar calistirin.
  pause
  exit /b 1
)

if not exist node_modules (
  echo Gerekli paketler kuruluyor, lutfen bekleyin...
  call npm install
)

echo Sunucu baslatiliyor...
node server.js
pause