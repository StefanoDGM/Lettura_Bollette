FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    ENERGON_WEB_PATH=/Lettura_Bollette_Energon

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY run_web_interface.py .
COPY run_full_pipeline.py .
COPY Logo-Energon-orizz-RGB.jpg .
COPY src ./src

RUN mkdir -p /app/web_runs /app/debug_empty_rows /app/tmp_pdf_debug

EXPOSE 8000

CMD ["waitress-serve", "--host=0.0.0.0", "--port=8000", "run_web_interface:app"]
