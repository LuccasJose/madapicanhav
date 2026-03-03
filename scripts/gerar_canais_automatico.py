#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de Automação: Gera dados de canais de vendas automaticamente
Pode ser executado via GitHub Actions ou agendador de tarefas
"""

import pandas as pd
import json
import sys
from pathlib import Path
from datetime import datetime

def gerar_analise_canais():
    """Gera análise de canais a partir do CSV do banco de dados"""
    
    try:
        # Caminho do arquivo CSV
        csv_path = Path(__file__).parent.parent / 'GMRMPMA (2)(Export).csv'
        
        if not csv_path.exists():
            print(f"❌ Arquivo não encontrado: {csv_path}")
            return False
        
        print(f"📊 Lendo banco de dados: {csv_path}")
        
        # Ler CSV com tratamento de encoding
        try:
            df = pd.read_csv(csv_path, sep=';', encoding='latin-1')
        except:
            df = pd.read_csv(csv_path, sep=';', encoding='cp1252')
        
        # Renomear colunas
        df.columns = df.columns.str.replace('FtoResumoVendaGeralItem[', '').str.replace(']', '')
        
        print(f"✅ Total de registros: {len(df)}")
        
        # Separar por canal
        salao_df = df[df['modo_venda_descr'].str.lower().isin(['balcão', 'salão'])]
        entrega_df = df[df['modo_venda_descr'].str.lower().isin(['entrega', 'ifood'])]
        
        print(f"🏪 Salão: {len(salao_df)} registros")
        print(f"🚚 Entrega: {len(entrega_df)} registros")
        
        # Processar canais
        salao_dados = processar_canal(salao_df, "SALÃO")
        entrega_dados = processar_canal(entrega_df, "ENTREGA")
        
        # Calcular totais
        total_geral = salao_dados['total_faturamento'] + entrega_dados['total_faturamento']
        pct_salao = (salao_dados['total_faturamento'] / total_geral * 100) if total_geral > 0 else 0
        pct_entrega = (entrega_dados['total_faturamento'] / total_geral * 100) if total_geral > 0 else 0
        
        # Montar resultado
        resultado = {
            'salao': salao_dados,
            'entrega': entrega_dados,
            'combinado': {
                'total_faturamento': total_geral,
                'percentual_salao': pct_salao,
                'percentual_entrega': pct_entrega
            },
            'timestamp': datetime.now().isoformat()
        }
        
        # Salvar JSON
        output_path = Path(__file__).parent.parent / 'docs' / 'data' / 'analise_canais.json'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(resultado, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Dados salvos em: {output_path}")
        print(f"💰 Faturamento Total: R$ {total_geral:,.2f}")
        print(f"  • Salão: {pct_salao:.1f}%")
        print(f"  • Entrega: {pct_entrega:.1f}%")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro: {str(e)}")
        return False

def processar_canal(df_canal, nome_canal):
    """Processa dados de um canal específico"""
    
    from collections import defaultdict
    
    produtos = defaultdict(lambda: {'quantidade': 0, 'valor_total': 0})
    
    for _, row in df_canal.iterrows():
        produto = row['material_descr']
        qtd = float(str(row['qtd']).replace(',', '.'))
        valor = float(str(row['vl_total']).replace(',', '.'))
        
        produtos[produto]['quantidade'] += qtd
        produtos[produto]['valor_total'] += valor
    
    # Ordenar por valor
    produtos_sorted = sorted(produtos.items(), key=lambda x: x[1]['valor_total'], reverse=True)
    
    total_faturamento = sum(p[1]['valor_total'] for p in produtos_sorted)
    
    return {
        'nome_canal': nome_canal,
        'total_faturamento': total_faturamento,
        'total_produtos': len(produtos_sorted),
        'produtos': {p[0]: p[1] for p in produtos_sorted}
    }

if __name__ == '__main__':
    sucesso = gerar_analise_canais()
    sys.exit(0 if sucesso else 1)

