web: bash -lc 'if [ "$APP_MODE" = "streamlit" ]; then /app/.venv/bin/python -m streamlit run app/streamlit_app.py --server.address 0.0.0.0 --server.port ${PORT:-8501} --server.headless true; else /app/.venv/bin/python -m uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}; fi'

