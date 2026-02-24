# -*- coding: utf-8 -*-
"""
ANÁLISE TEMPORAL DE VENDAS: TOP/BOTTOM 10 MENSAL COM SAZONALIDADE
Refatorado para usar shared.py — elimina duplicação de funções utilitárias.

Uso:
    py analise_temporal.py [arquivo] [--loja N] [--categoria X]
    py analise_temporal.py dados.csv --loja 5 --categoria CERVEJAS
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from typing import Any, Optional

import pandas as pd

from shared import (
    COL_LOJA, COL_PRODUTO, COL_VALOR, COL_DATA, COL_GRUPO, COL_TIPO_PRODUTO2,
    carregar_dados, limpar_valor_monetario, validar_colunas,
    remover_ruidos, filtrar_por_categoria, listar_categorias,
    configurar_ia, chamar_ia_com_retry,
    obter_contexto_sazonal, extrair_nome_mes, converter_id_loja, salvar_json,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# ==========================================
# CONSTANTES ESPECÍFICAS DESTE SCRIPT
# ==========================================
ARQUIVO_SAIDA = "analise_mensal_sazonal.json"
TOP_N = 10
BOTTOM_N = 10
PAUSA_ENTRE_REQUISICOES = 2.0


# ==========================================
# ANÁLISE COM IA
# ==========================================


def analisar_mes_com_ia(modelo: Any, id_loja: Any, mes_ref: str, lista_itens: list[dict], total_mensal: float) -> list[dict]:
    """
    Analisa desempenho mensal usando shared.chamar_ia_com_retry().

    OTIMIZAÇÕES DE PROMPT (vs. versão anterior ~290 linhas):
    - Prompt reduzido de ~500 tokens para ~150 tokens
    - Dados sumarizados: {produto, tipo, valor} ao invés de texto verboso
    - Zero-shot: sem exemplos redundantes (economia de ~100 tokens)
    - Contexto sazonal em 1 linha ao invés de seções completas
    - Resposta limitada: ≤80 chars diagnóstico, ≤60 chars ação
    """
    if not modelo or not lista_itens:
        return []

    ctx = obter_contexto_sazonal(mes_ref)
    nome_mes = extrair_nome_mes(mes_ref)

    # OTIMIZAÇÃO: dados compactos — envia somente campos essenciais
    dados = json.dumps(
        [{"p": i['produto'], "t": i['tipo'], "v": i.get('venda_este_mes', 0)} for i in lista_itens],
        ensure_ascii=False,
    )

    prompt = (
        f"Loja {id_loja}, {nome_mes}, {ctx['estacao']}, {ctx.get('eventos', '')}.\n"
        f"Total mês: R${total_mensal:.0f}.\n"
        f"Dados: {dados}\n"
        'Retorne JSON: [{"produto":"NOME","diagnostico":"≤80chars","acao":"≤60chars"}]\n'
        "Regras: diagnóstico direto, ação prática, sem comparações entre produtos."
    )

    return chamar_ia_com_retry(modelo, prompt) or []

# carregar_dados() agora vem do shared.py


def preparar_dados(
    df: pd.DataFrame,
    categoria_alvo: Optional[str] = None,
    coluna_filtro: str = COL_GRUPO,
) -> Optional[pd.DataFrame]:
    """
    Limpa e prepara dados para análise temporal.

    NOVO: remoção de ruídos (modificadores de preparo) + filtro por categoria.
    """
    if not validar_colunas(df):
        return None

    df = df.copy()

    # Limpa valores monetários
    df['valor_limpo'] = df[COL_VALOR].apply(limpar_valor_monetario)
    df = df[df['valor_limpo'] > 0]

    # Datas
    df['data_obj'] = pd.to_datetime(df[COL_DATA], dayfirst=True, errors='coerce')
    df['mes_ano'] = df['data_obj'].dt.to_period('M').astype(str)
    df = df.dropna(subset=['mes_ano'])

    # Padroniza produtos
    df['produto'] = df[COL_PRODUTO].astype(str).str.strip().str.upper().str.replace(r'\s+', ' ', regex=True)
    df['loja_id'] = df[COL_LOJA].astype(str)

    # NOVO: Remove ruídos (modificadores de preparo como "AO PONTO")
    df = remover_ruidos(df, col_produto='produto')

    # NOVO: Filtra por categoria se especificado
    if categoria_alvo:
        df = filtrar_por_categoria(df, categoria_alvo, coluna_filtro)

    # Agrupamento mensal
    df_agrupado = (
        df.groupby(['loja_id', 'mes_ano', 'produto'])['valor_limpo']
        .sum()
        .reset_index()
    )

    logger.info(f"Dados preparados: {len(df_agrupado)} registros agregados")
    return df_agrupado


# ==========================================
# 5. PROCESSAMENTO DE RANKING MENSAL
# ==========================================

def selecionar_top_bottom(df_mes: pd.DataFrame) -> pd.DataFrame:
    """Seleciona TOP N e BOTTOM N do mês."""
    df_mes = df_mes.sort_values(by='valor_limpo', ascending=False)
    total = len(df_mes)

    if total == 0:
        return pd.DataFrame()

    if total <= (TOP_N + BOTTOM_N):
        df_mes = df_mes.copy()
        df_mes['tipo_ranking'] = 'GERAL'
        return df_mes

    top = df_mes.head(TOP_N).copy()
    top['tipo_ranking'] = f'TOP {TOP_N}'

    bottom = df_mes.tail(BOTTOM_N).copy()
    bottom['tipo_ranking'] = f'BOTTOM {BOTTOM_N}'

    return pd.concat([top, bottom], ignore_index=True)


def processar_mes(
    df_loja: pd.DataFrame,
    mes_atual: str
) -> tuple[list[dict], float]:
    """
    Processa dados de um mês específico, gerando ranking TOP/BOTTOM.

    Utiliza operações vetorizadas do pandas para melhor performance.
    Retorna também o total mensal de vendas (todos os produtos).

    Args:
        df_loja: DataFrame com dados da loja
        mes_atual: Período atual no formato '2024-01'

    Returns:
        Tupla com (lista de dicionários com dados de cada produto, total mensal)
    """
    df_mes = df_loja[df_loja['mes_ano'] == mes_atual].copy()

    # Calcula o TOTAL MENSAL de todas as vendas (não apenas TOP/BOTTOM)
    total_mensal = df_mes['valor_limpo'].sum()

    selecao = selecionar_top_bottom(df_mes)

    if selecao.empty:
        return [], total_mensal

    # Converte para lista de dicionários (sem comparações)
    itens = selecao.apply(
        lambda row: {
            "produto": row['produto'],
            "tipo": row['tipo_ranking'],
            "venda_este_mes": round(row['valor_limpo'], 2)
        },
        axis=1
    ).tolist()

    return itens, total_mensal


def aplicar_analise_ia(modelo: Any, id_loja: str, mes: str, itens: list[dict], total_mensal: float) -> list[dict]:
    """Aplica análise IA aos itens do mês."""
    if not modelo:
        for item in itens:
            item['analise_ia'] = {"diagnostico": "IA não disponível", "acao": "-"}
        return itens

    resultado_ia = analisar_mes_com_ia(modelo, id_loja, mes, itens, total_mensal)

    dict_analises = {
        it['produto']: it for it in resultado_ia
        if isinstance(it, dict) and 'produto' in it
    }

    for item in itens:
        analise = dict_analises.get(item['produto'], {})
        item['analise_ia'] = {
            "diagnostico": analise.get('diagnostico', 'Análise indisponível'),
            "acao": analise.get('acao', '-'),
        }

    return itens


def processar_loja(df_loja: pd.DataFrame, id_loja: str, modelo: Any) -> dict:
    """Processa todos os meses de uma loja."""
    meses = sorted(df_loja['mes_ano'].unique())
    analises_mensais = {}

    for i, mes_atual in enumerate(meses):
        itens, total_mensal = processar_mes(df_loja, mes_atual)
        if not itens:
            continue

        logger.info(f"  {extrair_nome_mes(mes_atual)}: {len(itens)} itens | R${total_mensal:,.0f}")
        itens = aplicar_analise_ia(modelo, id_loja, mes_atual, itens, total_mensal)

        analises_mensais[mes_atual] = {"total_mensal": round(total_mensal, 2), "itens": itens}

        if modelo and i < len(meses) - 1:
            time.sleep(PAUSA_ENTRE_REQUISICOES)

    return {"id_loja": converter_id_loja(id_loja), "analises_mensais": analises_mensais}


def gerar_estatisticas_execucao(resultado: list[dict]) -> dict[str, Any]:
    """Gera estatísticas da execução para logging."""
    total_lojas = len(resultado)
    total_meses = sum(len(r.get('analises_mensais', {})) for r in resultado)
    total_itens = sum(
        len(m.get('itens', []))
        for r in resultado for m in r.get('analises_mensais', {}).values()
    )
    return {
        'lojas': total_lojas,
        'meses_analisados': total_meses,
        'itens_processados': total_itens,
    }


# ==========================================
# FUNÇÃO PRINCIPAL
# ==========================================

def parse_args() -> argparse.Namespace:
    """
    Parse argumentos de linha de comando.

    NOVO: --loja permite processar apenas 1 loja (substitui os 13 scripts analise_loja_N.py).
    Exemplos:
        py analise_temporal.py dados.csv --loja 5
        py analise_temporal.py dados.csv --categoria CERVEJAS
        py analise_temporal.py dados.csv --listar-categorias
    """
    parser = argparse.ArgumentParser(description="Análise Temporal Mensal TOP/BOTTOM 10 com IA")
    parser.add_argument('arquivo', nargs='?', default='dados_vendas.xlsx',
                        help='Arquivo de dados (CSV ou XLSX)')
    parser.add_argument('--loja', type=str, default=None,
                        help='Processar apenas esta loja (ex: 5)')
    parser.add_argument('--categoria', type=str, default=None,
                        help='Filtrar por categoria (ex: CERVEJAS, PICANHA)')
    parser.add_argument('--coluna-filtro', type=str, default='grupo',
                        choices=['grupo', 'tipo_produto2'],
                        help='Coluna de filtro: grupo (grupo_descr) ou tipo_produto2 (macro)')
    parser.add_argument('--listar-categorias', action='store_true',
                        help='Lista categorias disponíveis e sai')
    return parser.parse_args()


def main() -> None:
    """Executa análise temporal completa de vendas por loja."""
    args = parse_args()
    coluna_filtro = COL_TIPO_PRODUTO2 if args.coluna_filtro == 'tipo_produto2' else COL_GRUPO
    inicio = time.time()

    logger.info("=" * 60)
    logger.info("ANÁLISE TEMPORAL MENSAL - TOP/BOTTOM 10 COM IA")
    if args.categoria:
        logger.info(f"Filtro: {args.categoria} ({args.coluna_filtro})")
    if args.loja:
        logger.info(f"Loja específica: {args.loja}")
    logger.info("=" * 60)

    # 1. Carrega dados (usa shared.carregar_dados)
    df_raw = carregar_dados(args.arquivo)
    if df_raw is None:
        return

    # Modo listar categorias
    if args.listar_categorias:
        cats = listar_categorias(df_raw, coluna_filtro)
        print(f"\nCategorias disponíveis ({len(cats)}):")
        for c in cats:
            print(f"  - {c}")
        return

    # 2. Prepara dados (inclui remoção de ruídos + filtro por categoria)
    df = preparar_dados(df_raw, categoria_alvo=args.categoria, coluna_filtro=coluna_filtro)
    del df_raw
    if df is None:
        return

    # 3. Configura IA (usa shared.configurar_ia)
    modelo = configurar_ia()

    # 4. Filtra loja se especificado (substitui os scripts analise_loja_N.py)
    lojas = sorted(df['loja_id'].unique())
    if args.loja:
        lojas = [l for l in lojas if l == args.loja]
        if not lojas:
            logger.error(f"Loja {args.loja} não encontrada. Lojas: {sorted(df['loja_id'].unique())}")
            return

    total_lojas = len(lojas)
    meses_disponiveis = sorted(df['mes_ano'].unique())
    logger.info(f"Período: {meses_disponiveis[0]} a {meses_disponiveis[-1]}")
    logger.info(f"Processando {total_lojas} lojas...")

    resultado = []
    for idx, id_loja in enumerate(lojas, 1):
        logger.info(f"Loja {id_loja} ({idx}/{total_lojas})")
        df_loja = df[df['loja_id'] == id_loja]
        resultado.append(processar_loja(df_loja, id_loja, modelo))

    # 5. Salva resultado (usa shared.salvar_json)
    if salvar_json(resultado, ARQUIVO_SAIDA):
        stats = gerar_estatisticas_execucao(resultado)
        tempo_total = time.time() - inicio
        logger.info(
            f"Concluído: {stats['lojas']} lojas, {stats['meses_analisados']} meses, "
            f"{stats['itens_processados']} itens em {tempo_total:.1f}s → {ARQUIVO_SAIDA}"
        )
    else:
        logger.error("Falha ao salvar resultado final")


if __name__ == "__main__":
    main()