$env:PYTHONOPTIMIZE = 2
$env:PYTHONPYCACHEPREFIX = ".\PYCACHE"
pyinstaller.exe `
  -y `
  --clean `
  --onefile `
  --add-data="sft_creds.json;." `
  --add-data="sas_ftp_creds.json;." `
  --add-data="ryo_ftp_creds.json;." `
  --add-data="db-key.json;." `
  --add-data="production.env;." `
  -p ".venv;.venv\Lib\site-packages" `
  --upx-dir "D:\SFT Software Projects\Tools\upx-4.2.4-win64" `
  -i "sft.ico" `
  "src\__main__.py"
Remove-Item Env:PYTHONOPTIMIZE
Remove-Item Env:PYTHONPYCACHEPREFIX