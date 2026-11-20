#!/bin/bash
# Radar Cripto - inicia servidor local e abre dashboard no navegador.
# Uso: duplo clique no Finder, ou ./iniciar.command no terminal. Ctrl+C para parar.
cd "$(dirname "$0")" || exit 1
URL="http://localhost:8765"

abrir() { [ -z "$NO_OPEN" ] && open "$URL"; }

if ! command -v python3 >/dev/null; then
  echo "python3 não encontrado. Instale com: xcode-select --install"
  exit 1
fi

if [ ! -f .env ]; then
  cp .env.example .env
  echo "Criado .env vazio. Preencha chave somente-leitura da Binance para ver carteira."
fi

if curl -s -o /dev/null -m 2 "$URL"; then
  echo "Servidor já rodando em $URL"
  abrir
  exit 0
fi

echo "Radar Cripto em $URL  (Ctrl+C para parar)"
( sleep 1; abrir ) &
exec python3 server.py
