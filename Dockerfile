# syntax=docker/dockerfile:1.6
# ------------------------------------------------------------------------------
# Dockerfile - Streamlit App
# Boas práticas:
# - Imagem slim para reduzir tamanho
# - Layer caching: instala deps antes de copiar o código
# - Usuário não-root por segurança
# - Variáveis e healthcheck básicos
# ------------------------------------------------------------------------------

FROM python:3.12-slim AS base

# Evita prompt interativo do apt e melhora consistência
ENV DEBIAN_FRONTEND=noninteractive

# Configurações úteis do Python:
# - PYTHONDONTWRITEBYTECODE: evita criar .pyc (menos sujeira em container)
# - PYTHONUNBUFFERED: logs imediatos (bom para Docker)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Diretório de trabalho da aplicação
WORKDIR /app

# ------------------------------------------------------------------------------
# Dependências do sistema (mínimas)
# - curl: healthcheck e utilidades
# - build-essential: compilar wheels quando necessário (algumas libs exigem)
# - default-libmysqlclient-dev: útil caso use mysqlclient (opcional)
# OBS: Mantemos leve; você pode remover libmysqlclient-dev se não usar mysqlclient.
# ------------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    default-libmysqlclient-dev \
  && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------------------------
# Cria usuário não-root para rodar a aplicação com mais segurança
# ------------------------------------------------------------------------------
RUN useradd --create-home --shell /bin/bash appuser

# ------------------------------------------------------------------------------
# Instala dependências Python
# Separar essa etapa melhora o cache: se o código muda, não reinstala tudo.
# ------------------------------------------------------------------------------
COPY requirements.txt /app/requirements.txt

# Atualiza pip e instala dependências
RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r /app/requirements.txt

# ------------------------------------------------------------------------------
# Copia o código do projeto
# ------------------------------------------------------------------------------
COPY ./app /app/app

# Garante permissões para o usuário não-root
RUN chown -R appuser:appuser /app

# Troca para usuário não-root
USER appuser

# ------------------------------------------------------------------------------
# Streamlit
# - Porta padrão 8501
# - Configurações para rodar bem em container
# ------------------------------------------------------------------------------
EXPOSE 8501

# Configs recomendadas em container:
# - headless true
# - endereço 0.0.0.0 para expor fora do container
# - desativa CORS só se necessário; aqui mantemos padrão seguro (true)
ENV STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Healthcheck simples: verifica se a página inicial responde
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://localhost:8501/_stcore/health || exit 1

# ------------------------------------------------------------------------------
# Comando de execução
# ------------------------------------------------------------------------------
CMD ["streamlit", "run", "app/main.py"]
