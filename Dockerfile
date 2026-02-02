FROM python:3.9-slim

# -----------------------------------------------------------------------------
# CONFIGURAÇÕES DE AMBIENTE
# -----------------------------------------------------------------------------
# PYTHONDONTWRITEBYTECODE: Impede que o Python grave arquivos pyc no disco (ganho de performance em containers)
# PYTHONUNBUFFERED: Garante que os logs da aplicação sejam enviados diretamente para o terminal do Docker sem buffer
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# -----------------------------------------------------------------------------
# DEPENDÊNCIAS DO SISTEMA
# -----------------------------------------------------------------------------
# Instala 'build-essential' (gcc, make) para compilar pacotes Python se necessário.
# Instala 'curl' para realizar o Healthcheck do Streamlit.
# Removemos bibliotecas de dev do MySQL pois 'pymysql' não exige compilação C.
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# DEPENDÊNCIAS PYTHON
# -----------------------------------------------------------------------------
COPY requirements.txt .
# Atualiza o pip para evitar avisos e garantir compatibilidade com wheels modernos
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------------
# CÓDIGO FONTE E EXECUÇÃO
# -----------------------------------------------------------------------------
COPY . .

# Documenta que o container escuta na porta 8501
EXPOSE 8501

# HEALTHCHECK: Verifica a cada 30s se o Streamlit está respondendo.
# Se falhar, o Docker marca o container como 'unhealthy' (útil para auto-restart)
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]