#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para gerar análises separadas por canal de vendas (Salão vs iFood).
Gera JSONs com dados diários, semanais e mensais para cada canal.
"""

import json
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

from shared import (
    carregar_dados, limpar_valor_monetario, remover_ruidos,
    COL_LOJA, COL_PRODUTO, COL_VALOR, COL_DATA, COL_QTD, COL_GRUPO
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
    """Prepara dados para análise por canal."""
    df = df.copy()
    
    # Filtrar loja 1
    df = df[df[COL_LOJA].astype(str) != '1'].copy()
    
    # Limpar valores
    df['valor_limpo'] = df[COL_VALOR].apply(limpar_valor_monetario)
    df = df[df['valor_limpo'] > 0]
    
    # Processar datas
    df['data_obj'] = pd.to_datetime(df[COL_DATA], format='%Y-%m-%d', errors='coerce')
    df = df.dropna(subset=['data_obj'])
    
    df['dia'] = df['data_obj'].dt.strftime('%Y-%m-%d')
    df['semana'] = df['data_obj'].dt.strftime('%Y-W%W')
    df['mes'] = df['data_obj'].dt.to_period('M').astype(str)
    
    # Remover ruídos
    df = remover_ruidos(df)
    
    return df

def gerar_analise_canal(df, canal_nome, canal_key):
    """Gera análise para um canal específico."""
    logger.info(f"Processando canal: {canal_nome}")
    
    df_canal = df[df[MODO_VENDA] == canal_nome].copy()
    logger.info(f"  Registros: {len(df_canal)}")
    
    resultado = {
        'canal': canal_key,
        'canal_nome': canal_nome,
        'data_geracao': datetime.now().isoformat(),
        'dados_lojas': []
    }
    
    lojas = sorted(df_canal[COL_LOJA].unique())
    
    for loja_id in lojas:
        df_loja = df_canal[df_canal[COL_LOJA] == loja_id]
        
        analises = {}
        
        # Análise diária
        for dia in sorted(df_loja['dia'].unique()):
            df_dia = df_loja[df_loja['dia'] == dia]
            analises[dia] = {
                'faturamento': float(df_dia['valor_limpo'].sum()),
                'quantidade': int(df_dia[COL_QTD].sum()),
                'produtos': int(df_dia[COL_PRODUTO].nunique())
            }
        
        resultado['dados_lojas'].append({
            'loja_id': str(loja_id),
            'analises': analises
        })
    
    return resultado

def main():
    logger.info("=" * 50)
    logger.info("ANÁLISE POR CANAIS DE VENDAS")
    logger.info("=" * 50)
    
    # Carregar dados
    logger.info("Carregando CSV...")
    df = carregar_dados('GMRMPMA (2).csv')
    
    # Preparar
    logger.info("Preparando dados...")
    df = preparar_dados(df)
    
    # Gerar análises por canal
    for canal_nome, canal_key in CANAIS.items():
        logger.info(f"\nGerando análise para: {canal_nome}")
        resultado = gerar_analise_canal(df, canal_nome, canal_key)
        
        # Salvar JSON
        arquivo_saida = f'docs/data/vendas_canais_{canal_key}.json'
        Path('docs/data').mkdir(parents=True, exist_ok=True)
        
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            json.dump(resultado, f, ensure_ascii=False, indent=2)
        
        logger.info(f"  Salvo: {arquivo_saida}")
    
    logger.info("\n" + "=" * 50)
    logger.info("✅ Análise por canais concluída!")
    logger.info("=" * 50)

if __name__ == '__main__':
    main()

