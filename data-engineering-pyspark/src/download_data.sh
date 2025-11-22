#!/usr/bin/env bash
set -e

ROOT="/home/ubuntu/environment/data-engineering-pyspark"
INPUT_DIR="$ROOT/data/input"

echo "🧽 Limpando diretórios..."
rm -rf "$ROOT/data/tmp-pagamentos" "$ROOT/data/tmp-pedidos"
mkdir -p "$INPUT_DIR"
rm -rf "$INPUT_DIR"/*

echo ""
echo "⬇ Baixando TODOS os arquivos de PAGAMENTOS (via API GitHub)..."

# lista todos os arquivos da pasta data/pagamentos/
curl -s https://api.github.com/repos/infobarbosa/dataset-json-pagamentos/contents/data/pagamentos \
| grep "download_url" \
| cut -d '"' -f 4 \
| while read url; do
      echo "Baixando: $(basename $url)"
      curl -L "$url" -o "$INPUT_DIR/$(basename $url)"
  done

echo ""
echo "⬇ Baixando TODOS os arquivos de PEDIDOS (pasta data/pedidos)..."

curl -s https://api.github.com/repos/infobarbosa/datasets-csv-pedidos/contents/data/pedidos \
| grep "download_url" \
| cut -d '"' -f 4 \
| while read url; do
      echo "Baixando: $(basename $url)"
      curl -L "$url" -o "$INPUT_DIR/$(basename $url)"
  done

echo ""
echo "📂 Arquivos baixados:"
ls -lh "$INPUT_DIR"

echo ""
echo "✅ Processo concluído com sucesso!"
