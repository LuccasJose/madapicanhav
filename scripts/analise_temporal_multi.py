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
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

# ==========================================
# TIMING E LOGGING DE PERFORMANCE
# ==========================================
class TimingContext:
    """Context manager para medir tempo de execução de etapas."""
    def __init__(self, label: str):
        self.label = label
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        logger.info(f"⏱️  Iniciando: {self.label}")
        return self

    def __exit__(self, *args):
        elapsed = time.time() - self.start_time
        logger.info(f"✅ Concluído: {self.label} ({elapsed:.2f}s)")

from shared import (
    COL_LOJA, COL_PRODUTO, COL_VALOR, COL_DATA, COL_GRUPO, COL_TIPO_PRODUTO2,
    CATEGORIAS_MACRO,
    carregar_dados, limpar_valor_monetario, validar_colunas,
    remover_ruidos, filtrar_por_categoria, listar_categorias,
    configurar_ia, chamar_ia_com_retry, converter_id_loja,
    salvar_json as _salvar_json, gerar_sufixo_categoria,
    DELAY_ENTRE_CHAMADAS, MAX_TENTATIVAS_RATE_LIMIT, DELAY_BASE_RATE_LIMIT, logger,
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
    coluna_filtro: str = COL_TIPO_PRODUTO2,
) -> pd.DataFrame:
    """Prepara dados com colunas temporais, remoção de ruídos e filtro por categoria."""
    if not validar_colunas(df):
        return pd.DataFrame()

    df = df.copy()

    # Filtrar loja 1 (CD VACA BRAVA)
    df = df[df[COL_LOJA].astype(str) != '1'].copy()

    df['valor_limpo'] = df[COL_VALOR].apply(limpar_valor_monetario)
    df = df[df['valor_limpo'] > 0]

    df['data_obj'] = pd.to_datetime(df[COL_DATA], format='%Y-%m-%d', errors='coerce')
    df = df.dropna(subset=['data_obj'])

    df['dia'] = df['data_obj'].dt.strftime('%Y-%m-%d')
    df['semana'] = df['data_obj'].dt.strftime('%Y-W%W')
    df['mes'] = df['data_obj'].dt.to_period('M').astype(str)

    df['produto'] = df[COL_PRODUTO].astype(str).str.strip().str.upper()
    df['loja_id'] = df[COL_LOJA].astype(str)

    # Preserva categoria macro para filtragem client-side no dashboard
    if COL_TIPO_PRODUTO2 in df.columns:
        df['categoria'] = df[COL_TIPO_PRODUTO2].astype(str).str.strip().str.upper()
    else:
        df['categoria'] = 'OUTROS'

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
    """Agrega dados por período (dia, semana ou mês). Preserva coluna 'categoria' se existir."""
    group_cols = ['loja_id', coluna_periodo, 'produto']
    if 'categoria' in df.columns:
        group_cols.append('categoria')
    return (
        df.groupby(group_cols)['valor_limpo']
        .sum()
        .reset_index()
        .rename(columns={coluna_periodo: 'periodo'})
    )


def selecionar_top_bottom(df_periodo: pd.DataFrame, top_n: int = TOP_N, bottom_n: int = BOTTOM_N) -> pd.DataFrame:
    """Retorna TODOS os produtos vendidos em um período, sem duplicatas.

    Cada produto aparece uma única vez com seu valor total no período.
    Ordena por valor decrescente para facilitar visualização.
    """
    # Retorna todos os produtos, ordenados por valor (maior para menor)
    df_sorted = df_periodo.sort_values('valor_limpo', ascending=False).copy()

    # Marca todos como 'VENDIDO' (sem distinção TOP/BOTTOM)
    df_sorted['tipo'] = 'VENDIDO'

    return df_sorted


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
                {
                    "produto": row['produto'],
                    "valor": round(row['valor_limpo'], 2),
                    "tipo": row['tipo'],
                    "categoria": row.get('categoria', ''),
                }
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
    parser.add_argument('--coluna-filtro', type=str, default='tipo_produto2',
                        choices=['grupo', 'tipo_produto2'],
                        help='Coluna de filtro: grupo (grupo_descr) ou tipo_produto2 (macro)')
    parser.add_argument('--listar-categorias', action='store_true',
                        help='Lista categorias disponíveis e sai')
    parser.add_argument('--gerar-por-categoria', action='store_true',
                        help='Gera JSONs separados por macro-categoria (BEBIDAS, COMIDAS, etc.)')
    return parser.parse_args()


def _processar_granularidades(
    df: pd.DataFrame,
    modelo: Any,
    fazer_diario: bool,
    fazer_semanal: bool,
    fazer_mensal: bool,
    sufixo: str = "",
) -> list[str]:
    """
    Processa as granularidades selecionadas e retorna lista de arquivos gerados.
    O sufixo é adicionado ao nome do arquivo (ex: '_bebidas').
    """
    arquivos_gerados = []
    suf = f"_{sufixo}" if sufixo else ""

    if fazer_mensal:
        resultado = processar_granularidade(df, 'mes', 'mensal', modelo)
        nome = f'vendas_mensal{suf}.json'
        if salvar_json_local(resultado, nome):
            arquivos_gerados.append(nome)

    if fazer_semanal:
        resultado = processar_granularidade(df, 'semana', 'semanal', modelo)
        nome = f'vendas_semanal{suf}.json'
        if salvar_json_local(resultado, nome):
            arquivos_gerados.append(nome)

    if fazer_diario:
        resultado = processar_granularidade(df, 'dia', 'diario', modelo)
        nome = f'vendas_diario{suf}.json'
        if salvar_json_local(resultado, nome):
            arquivos_gerados.append(nome)

    return arquivos_gerados


def main(csv_path, all_flag=False, gerar_por_categoria=False):
    """Executa análise temporal multi-granularidade."""
    t0 = time.time()
    logger.info("=" * 50)
    logger.info("ANÁLISE TEMPORAL MULTI-GRANULARIDADE")
    logger.info(f"Arquivo: {csv_path} | All: {all_flag}")
    if gerar_por_categoria:
        logger.info("Modo: gerar JSONs por macro-categoria (BEBIDAS, COMIDAS, etc.)")
    logger.info("=" * 50)

    # Leitura otimizada
    with TimingContext("Carregamento de dados"):
        df = carregar_dados(csv_path)
    if df is None:
        logger.error("Falha ao carregar dados")
        return 1

    # Preparação de dados (limpeza, remoção de ruídos, etc)
    with TimingContext("Preparação de dados"):
        df_base = preparar_dados(df)
    if df_base.empty:
        logger.error("Nenhum dado válido após preparação")
        return 1

    # Configurar IA
    with TimingContext("Configuração de IA"):
        modelo = configurar_ia()

    # Processar granularidades
    # Se nenhuma flag foi especificada, processa todas por padrão
    if not (args.diario or args.semanal or args.mensal or all_flag):
        fazer_diario = True
        fazer_semanal = True
        fazer_mensal = True
    else:
        fazer_diario = args.diario or all_flag
        fazer_semanal = args.semanal or all_flag
        fazer_mensal = args.mensal or all_flag

    arquivos_gerados = []

    # ==========================================
    # MODO: GERAR POR CATEGORIA
    # ==========================================
    if gerar_por_categoria:
        # Gera JSON consolidado (todas as categorias)
        logger.info("=" * 40)
        logger.info("Gerando análise CONSOLIDADA (todas as categorias)")
        with TimingContext("Processamento consolidado"):
            arquivos_gerados.extend(_processar_granularidades(
                df_base, modelo, fazer_diario, fazer_semanal, fazer_mensal, sufixo=""
            ))

        # Gera JSON para cada macro-categoria
        from shared import CATEGORIAS_MACRO, COL_TIPO_PRODUTO2, filtrar_por_categoria
        for cat_nome, cat_sufixo in CATEGORIAS_MACRO.items():
            logger.info("=" * 40)
            logger.info(f"Gerando análise para categoria: {cat_nome}")
            with TimingContext(f"Processamento {cat_nome}"):
                df_cat = filtrar_por_categoria(df_base, cat_nome, COL_TIPO_PRODUTO2)
                if df_cat.empty:
                    logger.warning(f"Categoria '{cat_nome}' sem dados — pulando")
                    continue
                arquivos_gerados.extend(_processar_granularidades(
                    df_cat, modelo, fazer_diario, fazer_semanal, fazer_mensal, sufixo=cat_sufixo
                ))
    else:
        # ==========================================
        # MODO: ANÁLISE ÚNICA (padrão)
        # ==========================================
        with TimingContext("Processamento de granularidades"):
            arquivos_gerados = _processar_granularidades(
                df_base, modelo, fazer_diario, fazer_semanal, fazer_mensal
            )

    tempo_total = time.time() - t0
    logger.info("=" * 50)
    logger.info(f"✅ Análise concluída!")
    logger.info(f"Arquivos gerados: {len(arquivos_gerados)}")
    for arq in arquivos_gerados:
        logger.info(f"  - {arq}")
    logger.info(f"⏱️  TEMPO TOTAL: {tempo_total:.2f}s ({tempo_total/60:.2f}min)")
    logger.info("=" * 50)
    return 0


if __name__ == "__main__":
    args = parse_args()
    sys.exit(main(args.arquivo, args.all, args.gerar_por_categoria))

