# Smallfish Railway setup

This archive is configured to build with the included Dockerfile and start Smallfish directly.

Required Railway Variables:
- MEXC_API_KEY
- MEXC_API_SECRET
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID

The start command is:
python src/app.py --telegram

The application is configured for MEXC in config/default.yaml.
