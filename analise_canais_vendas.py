#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Análise de Vendas por Canal (Salão vs iFood)
Separa produtos vendidos em cada canal e calcula faturamento
"""

import pandas as pd
import json
from collections import defaultdict
from pathlib import Path

# Ler o CSV - tentar diferentes encodings
print("📊 Lendo banco de dados GMRMPMA...")
try:
    df = pd.read_csv('GMRMPMA (2)(Export).csv', sep=';', encoding='latin-1')
except:
    try:
        df = pd.read_csv('GMRMPMA (2)(Export).csv', sep=';', encoding='cp1252')
    except:
        df = pd.read_csv('GMRMPMA (2)(Export).csv', sep=';', encoding='iso-8859-1')

print(f"✅ Total de registros: {len(df)}")
print(f"✅ Colunas disponíveis: {list(df.columns)}")

# Renomear colunas para facilitar acesso
df.columns = df.columns.str.replace('FtoResumoVendaGeralItem[', '').str.replace(']', '')

print(f"\n📋 Canais de venda encontrados:")
canais = df['modo_venda_descr'].unique()
for canal in canais:
    count = len(df[df['modo_venda_descr'] == canal])
    print(f"  • {canal}: {count} registros")

# Separar dados por canal
salao_df = df[df['modo_venda_descr'].str.lower().isin(['balcão', 'salão'])]
entrega_df = df[df['modo_venda_descr'].str.lower().isin(['entrega', 'ifood'])]

print(f"\n🏪 Salão (balcão): {len(salao_df)} registros")
print(f"🚚 Entrega/iFood: {len(entrega_df)} registros")

# Função para processar dados por canal
def processar_canal(df_canal, nome_canal):
    print(f"\n{'='*60}")
    print(f"📊 ANÁLISE: {nome_canal}")
    print(f"{'='*60}")
    
    # Agrupar por produto
    produtos = defaultdict(lambda: {'quantidade': 0, 'valor_total': 0, 'valor_unit': 0})
    
    for _, row in df_canal.iterrows():
        produto = row['material_descr']
        qtd = float(str(row['qtd']).replace(',', '.'))
        valor = float(str(row['vl_total']).replace(',', '.'))
        
        produtos[produto]['quantidade'] += qtd
        produtos[produto]['valor_total'] += valor
    
    # Ordenar por valor total
    produtos_sorted = sorted(produtos.items(), key=lambda x: x[1]['valor_total'], reverse=True)
    
    # Exibir resultados
    total_faturamento = sum(p[1]['valor_total'] for p in produtos_sorted)
    
    print(f"\n💰 Faturamento Total: R$ {total_faturamento:,.2f}")
    print(f"📦 Total de Produtos Únicos: {len(produtos_sorted)}")
    print(f"\n{'Produto':<50} {'Qtd':>10} {'Faturamento':>15}")
    print("-" * 80)
    
    for produto, dados in produtos_sorted[:20]:  # Top 20
        print(f"{produto:<50} {dados['quantidade']:>10.2f} R$ {dados['valor_total']:>13,.2f}")
    
    if len(produtos_sorted) > 20:
        print(f"\n... e mais {len(produtos_sorted) - 20} produtos")
    
    return {
        'nome_canal': nome_canal,
        'total_faturamento': total_faturamento,
        'total_produtos': len(produtos_sorted),
        'produtos': {p[0]: p[1] for p in produtos_sorted}
    }

# Processar ambos os canais
salao_dados = processar_canal(salao_df, "SALÃO (Balcão)")
entrega_dados = processar_canal(entrega_df, "ENTREGA/iFOOD")

# Análise combinada
print(f"\n{'='*60}")
print(f"📊 ANÁLISE COMBINADA (Todos os Canais)")
print(f"{'='*60}")

total_geral = salao_dados['total_faturamento'] + entrega_dados['total_faturamento']
pct_salao = (salao_dados['total_faturamento'] / total_geral * 100) if total_geral > 0 else 0
pct_entrega = (entrega_dados['total_faturamento'] / total_geral * 100) if total_geral > 0 else 0

print(f"\n💰 Faturamento Total Geral: R$ {total_geral:,.2f}")
print(f"  • Salão: R$ {salao_dados['total_faturamento']:,.2f} ({pct_salao:.1f}%)")
print(f"  • Entrega/iFood: R$ {entrega_dados['total_faturamento']:,.2f} ({pct_entrega:.1f}%)")

# Salvar resultados em JSON
resultado = {
    'salao': salao_dados,
    'entrega': entrega_dados,
    'combinado': {
        'total_faturamento': total_geral,
        'percentual_salao': pct_salao,
        'percentual_entrega': pct_entrega
    }
}

with open('analise_canais.json', 'w', encoding='utf-8') as f:
    json.dump(resultado, f, indent=2, ensure_ascii=False)

print(f"\n✅ Resultados salvos em 'analise_canais.json'")

