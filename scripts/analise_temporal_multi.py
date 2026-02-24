# -*- coding: utf-8 -*-
"""
ANÁLISE TEMPORAL MULTI-GRANULARIDADE: DIÁRIO, SEMANAL E MENSAL
Refatorado para usar shared.py — elimina duplicação de funções utilitárias.

Uso:
    py analise_temporal_multi.py [arquivo] [--diario] [--semanal] [--mensal]
    py analise_temporal_multi.py dados.csv --categoria CERVEJAS --mensal
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from shared import (
    COL_LOJA, COL_PRODUTO, COL_VALOR, COL_DATA, COL_GRUPO, COL_TIPO_PRODUTO2,
    carregar_dados, limpar_valor_monetario, validar_colunas,
    remover_ruidos, filtrar_por_categoria, listar_categorias,
    configurar_ia, chamar_ia_com_retry, converter_id_loja,
    salvar_json as _salvar_json,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==========================================
# CONSTANTES ESPECÍFICAS DESTE SCRIPT
# ==========================================
PASTA_SAIDA = "docs/data"
TOP_N = 10
BOTTOM_N = 10
PAUSA_ENTRE_REQUISICOES = 2.0


def preparar_dados(
    df: pd.DataFrame,
    categoria_alvo: Optional[str] = None,
    coluna_filtro: str = COL_GRUPO,
) -> pd.DataFrame:
    """Prepara dados com colunas temporais, remoção de ruídos e filtro por categoria."""
    if not validar_colunas(df):
        return pd.DataFrame()

    df = df.copy()
    df['valor_limpo'] = df[COL_VALOR].apply(limpar_valor_monetario)
    df = df[df['valor_limpo'] > 0]

    df['data_obj'] = pd.to_datetime(df[COL_DATA], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['data_obj'])

    df['dia'] = df['data_obj'].dt.strftime('%Y-%m-%d')
    df['semana'] = df['data_obj'].dt.strftime('%Y-W%W')
    df['mes'] = df['data_obj'].dt.to_period('M').astype(str)

    df['produto'] = df[COL_PRODUTO].astype(str).str.strip().str.upper()
    df['loja_id'] = df[COL_LOJA].astype(str)

    # NOVO: Remove ruídos (modificadores de preparo)
    df = remover_ruidos(df, col_produto='produto')

    # NOVO: Filtra por categoria se especificado
    if categoria_alvo:
        df = filtrar_por_categoria(df, categoria_alvo, coluna_filtro)

    logger.info(f"Dados preparados: {len(df)} registros válidos")
    return df


# ==========================================
# ANÁLISE POR GRANULARIDADE
# ==========================================

def agregar_por_periodo(df: pd.DataFrame, coluna_periodo: str) -> pd.DataFrame:
    """Agrega dados por período (dia, semana ou mês)."""
    return (
        df.groupby(['loja_id', coluna_periodo, 'produto'])['valor_limpo']
        .sum()
        .reset_index()
        .rename(columns={coluna_periodo: 'periodo'})
    )


def selecionar_top_bottom(df_periodo: pd.DataFrame, top_n: int = TOP_N, bottom_n: int = BOTTOM_N) -> pd.DataFrame:
    """Seleciona TOP e BOTTOM produtos de um período."""
    df_sorted = df_periodo.sort_values('valor_limpo', ascending=False)

    # TOP N
    top = df_sorted.head(top_n).copy()
    top['tipo'] = 'TOP'

    # BOTTOM N (com vendas > 0)
    bottom = df_sorted[df_sorted['valor_limpo'] > 0].tail(bottom_n).copy()
    bottom['tipo'] = 'BOTTOM'

    return pd.concat([top, bottom], ignore_index=True)


def analisar_com_ia(modelo: Any, id_loja: str, periodo: str, itens: list, total: float, granularidade: str) -> list:
    """
    Analisa produtos com IA usando shared.chamar_ia_com_retry().
    Prompt otimizado: dados sumarizados, zero-shot, sem exemplos redundantes.
    """
    if not modelo or not itens:
        for item in itens:
            item['analise_ia'] = {"diagnostico": "IA não disponível", "acao": "-"}
        return itens

    # OTIMIZAÇÃO: dados compactos (siglas ao invés de texto completo)
    dados = json.dumps(
        [{"p": i['produto'], "v": i['valor'], "t": i['tipo']} for i in itens],
        ensure_ascii=False
    )

    prompt = (
        f"Loja {id_loja}, {periodo} ({granularidade}), total R${total:.0f}.\n"
        f"Dados: {dados}\n"
        'Retorne JSON: [{"produto":"NOME","diagnostico":"≤80chars","acao":"≤60chars"}]\n'
        "Regras: diagnóstico direto por produto, foco em ação prática."
    )

    resultado = chamar_ia_com_retry(modelo, prompt)
    if resultado:
        dict_analises = {r['produto']: r for r in resultado if isinstance(r, dict)}
        for item in itens:
            analise = dict_analises.get(item['produto'], {})
            item['analise_ia'] = {
                "diagnostico": analise.get('diagnostico', 'Análise indisponível'),
                "acao": analise.get('acao', '-')
            }
    else:
        for item in itens:
            item['analise_ia'] = {"diagnostico": "Erro na análise", "acao": "-"}

    return itens


def processar_granularidade(df: pd.DataFrame, coluna_periodo: str, granularidade: str, modelo: Any) -> dict:
    """Processa análise para uma granularidade específica."""
    logger.info(f"\n{'='*50}")
    logger.info(f"Processando análise {granularidade.upper()}")
    logger.info(f"{'='*50}")

    df_agregado = agregar_por_periodo(df, coluna_periodo)
    lojas = sorted(df_agregado['loja_id'].unique())

    resultado = {"granularidade": granularidade, "gerado_em": datetime.now().isoformat(), "dados_lojas": []}

    for id_loja in lojas:
        df_loja = df_agregado[df_agregado['loja_id'] == id_loja]
        periodos = sorted(df_loja['periodo'].unique())

        logger.info(f"Loja {id_loja}: {len(periodos)} períodos")

        analises = {}
        for periodo in periodos:
            df_periodo = df_loja[df_loja['periodo'] == periodo]
            total = df_periodo['valor_limpo'].sum()

            selecao = selecionar_top_bottom(df_periodo)
            if selecao.empty:
                continue

            itens = [
                {"produto": row['produto'], "valor": round(row['valor_limpo'], 2), "tipo": row['tipo']}
                for _, row in selecao.iterrows()
            ]

            # IA apenas nos últimos 7 períodos (economia de rate limit)
            if periodo in periodos[-7:]:
                itens = analisar_com_ia(modelo, id_loja, periodo, itens, total, granularidade)
            else:
                for item in itens:
                    item['analise_ia'] = {"diagnostico": "Período histórico", "acao": "-"}

            analises[periodo] = {"total": round(total, 2), "itens": itens}

        # Usa converter_id_loja() do shared ao invés de try/except inline
        resultado["dados_lojas"].append({"id_loja": converter_id_loja(id_loja), "analises": analises})

    return resultado


def salvar_json_local(dados: Any, nome_arquivo: str) -> bool:
    """Wrapper que salva JSON na PASTA_SAIDA usando shared.salvar_json."""
    caminho = os.path.join(PASTA_SAIDA, nome_arquivo)
    return _salvar_json(dados, caminho)


# ==========================================
# FUNÇÃO PRINCIPAL
# ==========================================

def parse_args() -> argparse.Namespace:
    """
    Parse argumentos de linha de comando.

    Exemplos:
        py analise_temporal_multi.py dados.csv --mensal
        py analise_temporal_multi.py dados.csv --categoria CERVEJAS --mensal
        py analise_temporal_multi.py dados.csv --listar-categorias
    """
    parser = argparse.ArgumentParser(description="Análise Temporal Multi-Granularidade")
    parser.add_argument('arquivo', nargs='?', default='dados_vendas.xlsx',
                        help='Arquivo de dados (CSV ou XLSX)')
    parser.add_argument('--diario', action='store_true', help='Gerar análise diária')
    parser.add_argument('--semanal', action='store_true', help='Gerar análise semanal')
    parser.add_argument('--mensal', action='store_true', help='Gerar análise mensal')
    parser.add_argument('--all', action='store_true', help='Gerar todas as granularidades')
    parser.add_argument('--categoria', type=str, default=None,
                        help='Filtrar por categoria (ex: CERVEJAS, PICANHA)')
    parser.add_argument('--coluna-filtro', type=str, default='grupo',
                        choices=['grupo', 'tipo_produto2'],
                        help='Coluna de filtro: grupo (grupo_descr) ou tipo_produto2 (macro)')
    parser.add_argument('--listar-categorias', action='store_true',
                        help='Lista categorias disponíveis e sai')
    return parser.parse_args()


def main():
    """Executa análise temporal multi-granularidade."""
    args = parse_args()
    coluna_filtro = COL_TIPO_PRODUTO2 if args.coluna_filtro == 'tipo_produto2' else COL_GRUPO
    inicio = time.time()

    # Se nenhuma granularidade foi selecionada, faz todas
    fazer_diario = args.diario or args.all or not (args.diario or args.semanal or args.mensal)
    fazer_semanal = args.semanal or args.all or not (args.diario or args.semanal or args.mensal)
    fazer_mensal = args.mensal or args.all or not (args.diario or args.semanal or args.mensal)

    logger.info("=" * 60)
    logger.info("ANÁLISE TEMPORAL MULTI-GRANULARIDADE")
    if args.categoria:
        logger.info(f"Filtro: {args.categoria} ({args.coluna_filtro})")
    logger.info(f"Granularidades: D={fazer_diario} S={fazer_semanal} M={fazer_mensal}")
    logger.info("=" * 60)

    # Carrega dados (usa shared.carregar_dados)
    df = carregar_dados(args.arquivo)
    if df is None:
        logger.error("Falha ao carregar dados")
        return 1

    # Modo listar categorias
    if args.listar_categorias:
        cats = listar_categorias(df, coluna_filtro)
        print(f"\nCategorias disponíveis ({len(cats)}):")
        for c in cats:
            print(f"  - {c}")
        return 0

    # Prepara dados (inclui remoção de ruídos + filtro por categoria)
    df = preparar_dados(df, categoria_alvo=args.categoria, coluna_filtro=coluna_filtro)
    if df.empty:
        logger.error("Nenhum dado válido após preparação")
        return 1

    # Configura IA (usa shared.configurar_ia)
    modelo = configurar_ia()

    # Processa cada granularidade
    arquivos_gerados = []

    if fazer_mensal:
        resultado = processar_granularidade(df, 'mes', 'mensal', modelo)
        if salvar_json_local(resultado, 'vendas_mensal.json'):
            arquivos_gerados.append('vendas_mensal.json')

    if fazer_semanal:
        resultado = processar_granularidade(df, 'semana', 'semanal', modelo)
        if salvar_json_local(resultado, 'vendas_semanal.json'):
            arquivos_gerados.append('vendas_semanal.json')

    if fazer_diario:
        resultado = processar_granularidade(df, 'dia', 'diario', modelo)
        if salvar_json_local(resultado, 'vendas_diario.json'):
            arquivos_gerados.append('vendas_diario.json')

    # Gera arquivo consolidado (índice)
    consolidado = {
        "gerado_em": datetime.now().isoformat(),
        "arquivos": arquivos_gerados,
        "lojas": sorted(df['loja_id'].unique().tolist()),
        "periodo_dados": {
            "inicio": df['data_obj'].min().strftime('%Y-%m-%d'),
            "fim": df['data_obj'].max().strftime('%Y-%m-%d'),
        },
    }
    salvar_json_local(consolidado, 'consolidado.json')

    # Estatísticas finais
    tempo_total = time.time() - inicio
    logger.info(f"\nConcluído em {tempo_total:.1f}s | {len(arquivos_gerados)} arquivos gerados")
    return 0


if __name__ == "__main__":
    sys.exit(main())

