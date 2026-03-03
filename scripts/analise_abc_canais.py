#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para gerar análises ABC separadas por canal de vendas (Salão vs iFood).
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pandas as pd

from shared import (
    carregar_dados, limpar_valor_monetario, remover_ruidos,
    COL_LOJA, COL_PRODUTO, COL_VALOR, COL_DATA, COL_QTD
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MODO_VENDA = 'modo_venda_descr'
CANAIS = {
    'balcão': 'salao',
    'entrega': 'ifood'
}

def preparar_dados(df):
    """Prepara dados para análise ABC por canal."""
    df = df.copy()
    
    # Filtrar loja 1
    df = df[df[COL_LOJA].astype(str) != '1'].copy()
    
    # Limpar valores
    df['valor_limpo'] = df[COL_VALOR].apply(limpar_valor_monetario)
    df = df[df['valor_limpo'] > 0]
    
    # Processar datas
    df['data_obj'] = pd.to_datetime(df[COL_DATA], format='%Y-%m-%d', errors='coerce')
    df = df.dropna(subset=['data_obj'])
    
    df['mes'] = df['data_obj'].dt.to_period('M').astype(str)
    
    # Remover ruídos
    df = remover_ruidos(df)
    
    return df

def calcular_abc(df_canal, loja_id):
    """Calcula classificação ABC para um canal e loja."""
    df_loja = df_canal[df_canal[COL_LOJA] == loja_id]
    
    # Agrupar por produto
    produtos = defaultdict(lambda: {'faturamento': 0, 'quantidade': 0})
    
    for _, row in df_loja.iterrows():
        produto = row[COL_PRODUTO]
        produtos[produto]['faturamento'] += row['valor_limpo']
        produtos[produto]['quantidade'] += row[COL_QTD]
    
    # Ordenar por faturamento
    produtos_sorted = sorted(
        produtos.items(),
        key=lambda x: x[1]['faturamento'],
        reverse=True
    )
    
    total = sum(p[1]['faturamento'] for p in produtos_sorted)
    
    # Classificar ABC
    resultado = []
    acumulado = 0
    
    for produto, dados in produtos_sorted:
        acumulado += dados['faturamento']
        percentual = (acumulado / total * 100) if total > 0 else 0
        
        if percentual <= 80:
            classe = 'A'
        elif percentual <= 95:
            classe = 'B'
        else:
            classe = 'C'
        
        resultado.append({
            'produto': produto,
            'faturamento': round(dados['faturamento'], 2),
            'quantidade': int(dados['quantidade']),
            'classe': classe,
            'percentual_acumulado': round(percentual, 2)
        })
    
    return resultado

def gerar_analise_abc_canal(df, canal_nome, canal_key):
    """Gera análise ABC para um canal específico."""
    logger.info(f"Processando ABC para canal: {canal_nome}")
    
    df_canal = df[df[MODO_VENDA] == canal_nome].copy()
    
    resultado = {
        'canal': canal_key,
        'canal_nome': canal_nome,
        'data_geracao': datetime.now().isoformat(),
        'dados_lojas': []
    }
    
    lojas = sorted(df_canal[COL_LOJA].unique())
    
    for loja_id in lojas:
        abc = calcular_abc(df_canal, loja_id)
        resultado['dados_lojas'].append({
            'loja_id': str(loja_id),
            'produtos': abc
        })
    
    return resultado

def main():
    logger.info("=" * 50)
    logger.info("ANÁLISE ABC POR CANAIS DE VENDAS")
    logger.info("=" * 50)
    
    # Carregar dados
    logger.info("Carregando CSV...")
    df = carregar_dados('GMRMPMA (2).csv')
    
    # Preparar
    logger.info("Preparando dados...")
    df = preparar_dados(df)
    
    # Gerar análises ABC por canal
    for canal_nome, canal_key in CANAIS.items():
        logger.info(f"\nGerando ABC para: {canal_nome}")
        resultado = gerar_analise_abc_canal(df, canal_nome, canal_key)
        
        # Salvar JSON
        arquivo_saida = f'docs/data/analise_abc_canais_{canal_key}.json'
        Path('docs/data').mkdir(parents=True, exist_ok=True)
        
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2)
        
        logger.info(f"  Salvo: {arquivo_saida}")
    
    logger.info("\n" + "=" * 50)
    logger.info("✅ Análise ABC por canais concluída!")
    logger.info("=" * 50)

if __name__ == '__main__':
    main()

