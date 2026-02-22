FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY app /app/app
COPY static /app/static
COPY templates /app/templates
COPY fonts /app/fonts
COPY sql /app/sql
COPY docs /app/docs
# data is mounted as volume for persistence; keep empty db in image as fallback
COPY data /app/data
EXPOSE 8000
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000"]
