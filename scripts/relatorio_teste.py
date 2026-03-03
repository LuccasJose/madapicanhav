# -*- coding: utf-8 -*-
"""
CURVA ABC COM ANÁLISE IA - Refatorado para usar módulo compartilhado (shared.py).
Gera relatório ABC com insights de tendência usando Google Gemini.

Mudanças vs versão anterior:
- Funções duplicadas removidas (agora em shared.py)
- Limpeza de ruídos (modificadores de preparo) aplicada automaticamente
- Novo filtro por categoria de produto (--categoria)
- Prompt otimizado: ~60% menos tokens, dados sumarizados ao invés de raw
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# Importa módulo compartilhado (centraliza funções reutilizáveis)
from shared import (
    COL_LOJA, COL_PRODUTO, COL_VALOR, COL_DATA, COL_GRUPO, COL_TIPO_PRODUTO2,
    CATEGORIAS_MACRO,
    carregar_dados, limpar_valor_monetario, validar_colunas,
    remover_ruidos, filtrar_por_categoria, listar_categorias,
    configurar_ia, chamar_ia_com_retry, converter_id_loja, salvar_json,
    gerar_sufixo_categoria,
)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==========================================
# CONSTANTES ESPECÍFICAS DESTE SCRIPT
# ==========================================
PASTA_SAIDA = "docs/data"
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, "analise_abc_final.json")
ARQUIVO_CACHE = os.path.join(PASTA_SAIDA, "cache_analises_ia.json")

# Parâmetros da Curva ABC
LIMITE_CLASSE_A = 80
LIMITE_CLASSE_B = 95

# Parâmetros de processamento IA
TAMANHO_LOTE_IA = 15
PAUSA_ENTRE_LOTES = 2.0


def classificar_abc(valor_acumulado: float) -> str:
    """Classifica item na curva ABC baseado no percentual acumulado."""
    if valor_acumulado <= LIMITE_CLASSE_A:
        return 'A'
    elif valor_acumulado <= LIMITE_CLASSE_B:
        return 'B'
    return 'C'


# ==========================================
# FUNÇÕES DE CACHE (específicas deste script)
# ==========================================

def carregar_cache() -> dict:
    """Carrega cache de análises anteriores: {loja_id: {produto|classe: analise}}."""
    if not os.path.exists(ARQUIVO_CACHE):
        return {}
    try:
        with open(ARQUIVO_CACHE, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        logger.info(f"Cache: {sum(len(v) for v in cache.values())} análises de {len(cache)} lojas")
        return cache
    except (json.JSONDecodeError, IOError):
        return {}


def salvar_cache(cache: dict) -> bool:
    """Salva cache de análises no arquivo JSON."""
    try:
        Path(ARQUIVO_CACHE).parent.mkdir(parents=True, exist_ok=True)
        with open(ARQUIVO_CACHE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        return True
    except IOError as e:
        logger.error(f"Erro ao salvar cache: {e}")
        return False


def obter_analise_cache(cache: dict, id_loja: str, produto: str, classe: str) -> Optional[str]:
    """Busca análise no cache pela chave produto|classe."""
    return cache.get(str(id_loja), {}).get(f"{produto}|{classe}")


def adicionar_ao_cache(cache: dict, id_loja: str, produto: str, classe: str, analise: str) -> None:
    """Adiciona análise ao cache."""
    id_loja_str = str(id_loja)
    if id_loja_str not in cache:
        cache[id_loja_str] = {}
    cache[id_loja_str][f"{produto}|{classe}"] = analise


# ==========================================
# ANÁLISE IA - Prompt otimizado (zero-shot, dados sumarizados)
# ==========================================

def analisar_lote_ia(modelo: Any, id_loja: Any, lote_itens: list[dict]) -> list[dict]:
    """
    Envia lote sumarizado à IA e retorna análises.
    Usa chamar_ia_com_retry() do shared.py (retry + backoff incluídos).

    OTIMIZAÇÕES DE PROMPT:
    - Envia apenas métricas-chave (produto, classe, total), NÃO o histórico raw
    - Prompt zero-shot conciso (~200 tokens vs ~500 antes)
    - Resposta limitada a 60 chars por produto → menos tokens de saída
    """
    if not modelo or not lote_itens:
        return []

    # OTIMIZAÇÃO: envia apenas dados sumarizados, não o histórico mensal completo.
    # Reduz ~70% dos tokens de entrada por chamada.
    dados_sumarizados = json.dumps(
        [{'p': it['produto'], 'c': it['classe']} for it in lote_itens],
        ensure_ascii=False
    )

    # Prompt otimizado: zero-shot, direto, sem exemplos redundantes
    prompt = (
        f"Analise cada produto da Loja {id_loja} (Curva ABC).\n"
        f"Dados: {dados_sumarizados}\n"
        "Retorne JSON: [{\"produto\":\"NOME\",\"analise\":\"ação prática ≤60 chars\"}]\n"
        "Regras: diagnóstico direto por produto, sem comparações, foco em ação."
    )

    resultado = chamar_ia_com_retry(modelo, prompt)
    return resultado or []

# ==========================================
# PROCESSAMENTO DE DADOS
# ==========================================
# carregar_dados() → importado de shared.py

def preparar_dados(
    df: pd.DataFrame,
    categoria_alvo: Optional[str] = None,
    coluna_filtro: str = COL_TIPO_PRODUTO2,
) -> Optional[pd.DataFrame]:
    """
    Limpa e prepara dados para análise ABC.

    Inclui:
    - Validação de colunas obrigatórias
    - Conversão de valores monetários
    - Remoção de ruídos (modificadores de preparo como 'Ao ponto')
    - Filtro por categoria de produto (se especificado)

    Args:
        df: DataFrame bruto
        categoria_alvo: Categoria para filtrar (ex: 'CERVEJAS', 'BEBIDAS'). None = todas.
        coluna_filtro: Coluna para filtro de categoria (COL_GRUPO ou COL_TIPO_PRODUTO2)
    """
    if not validar_colunas(df):
        return None

    df = df.copy()

    # Filtrar loja 1 (CD VACA BRAVA)
    df = df[df[COL_LOJA].astype(str) != '1'].copy()

    # Converte valores monetários
    df['valor_limpo'] = df[COL_VALOR].apply(limpar_valor_monetario)

    # Remove registros sem valor
    registros_antes = len(df)
    df = df[df['valor_limpo'] > 0]
    removidos = registros_antes - len(df)
    if removidos > 0:
        logger.info(f"Removidos {removidos} registros com valor <= 0")

    # Tratamento de data
    df['data_obj'] = pd.to_datetime(df[COL_DATA], format='%Y-%m-%d', errors='coerce')
    datas_invalidas = df['data_obj'].isna().sum()
    if datas_invalidas > 0:
        logger.warning(f"{datas_invalidas} datas inválidas encontradas")

    df['mes_ano'] = df['data_obj'].dt.to_period('M').astype(str)
    df = df.dropna(subset=['mes_ano'])

    # Padroniza nomes de produtos
    df[COL_PRODUTO] = (
        df[COL_PRODUTO].astype(str).str.strip().str.upper()
        .str.replace(r'\s+', ' ', regex=True)
    )
    df = df[df[COL_PRODUTO].notna() & (df[COL_PRODUTO] != '') & (df[COL_PRODUTO] != 'NAN')]

    # NOVO: Remove ruídos (modificadores de preparo como "AO PONTO")
    df = remover_ruidos(df)

    # NOVO: Filtra por categoria se especificado
    if categoria_alvo:
        df = filtrar_por_categoria(df, categoria_alvo, coluna_filtro)
        if df.empty:
            logger.error(f"Nenhum dado encontrado para categoria '{categoria_alvo}'")
            return None

    logger.info(f"Dados preparados: {len(df)} registros válidos")
    return df


def gerar_historico_vendas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gera histórico de vendas mensais agrupado por loja e produto.

    Args:
        df: DataFrame preparado

    Returns:
        DataFrame com totais e histórico por produto/loja
    """
    logger.info("Gerando histórico de vendas...")

    # Agrupa vendas por mês
    historico_mensal = (
        df.groupby([COL_LOJA, COL_PRODUTO, 'mes_ano'])['valor_limpo']
        .sum()
        .reset_index()
    )

    # Cria dicionário de histórico para cada produto
    def criar_dict_historico(grupo: pd.DataFrame) -> dict:
        return dict(zip(grupo['mes_ano'], grupo['valor_limpo'].round(2)))

    df_historico = (
        historico_mensal
        .groupby([COL_LOJA, COL_PRODUTO], group_keys=False)
        .apply(criar_dict_historico, include_groups=False)
        .reset_index(name='historico_vendas')
    )

    # Calcula total de vendas por produto
    df_total = (
        df.groupby([COL_LOJA, COL_PRODUTO])['valor_limpo']
        .sum()
        .reset_index(name='total_vendas')
    )

    # Merge histórico com totais
    df_final = pd.merge(df_total, df_historico, on=[COL_LOJA, COL_PRODUTO])

    # Preserva categoria macro (tipo_produto2) para filtragem client-side no dashboard.
    # Cria mapeamento produto→categoria e adiciona ao DataFrame final.
    if COL_TIPO_PRODUTO2 in df.columns:
        cat_map = (
            df.groupby([COL_LOJA, COL_PRODUTO])[COL_TIPO_PRODUTO2]
            .first()
            .reset_index()
            .rename(columns={COL_TIPO_PRODUTO2: 'categoria'})
        )
        cat_map['categoria'] = cat_map['categoria'].astype(str).str.strip().str.upper()
        df_final = pd.merge(df_final, cat_map, on=[COL_LOJA, COL_PRODUTO], how='left')
        df_final['categoria'] = df_final['categoria'].fillna('OUTROS')

    logger.info(f"Histórico gerado: {len(df_final)} produtos únicos")
    return df_final


def processar_loja(df_loja: pd.DataFrame, id_loja: str, modelo: Any, cache: dict) -> dict:
    """Processa dados de uma loja: curva ABC + análise IA."""
    df_loja = df_loja.sort_values(by='total_vendas', ascending=False).copy()
    total_vendas = df_loja['total_vendas'].sum()

    if total_vendas == 0:
        logger.warning(f"Loja {id_loja} sem vendas válidas")
        return {"id_loja": id_loja, "itens": []}

    df_loja['percentual'] = df_loja['total_vendas'] / total_vendas * 100
    df_loja['acumulado'] = df_loja['percentual'].cumsum()
    df_loja['classe'] = df_loja['acumulado'].apply(classificar_abc)

    itens_loja = df_loja.apply(
        lambda row: {
            "produto": row[COL_PRODUTO],
            "valor_total": round(row['total_vendas'], 2),
            "classe": row['classe'],
            "historico": row['historico_vendas'],
            "categoria": row.get('categoria', ''),
        },
        axis=1,
    ).tolist()

    if modelo:
        itens_loja = processar_analise_ia(modelo, id_loja, itens_loja, cache)

    return {"id_loja": converter_id_loja(id_loja), "itens": itens_loja}


def processar_analise_ia(modelo: Any, id_loja: str, itens: list[dict], cache: dict) -> list[dict]:
    """Processa análise IA em lotes com cache para evitar chamadas duplicadas."""
    analises_finais = []
    itens_novos = []

    # Separa cache hits
    for item in itens:
        analise = obter_analise_cache(cache, id_loja, item['produto'], item['classe'])
        if analise:
            item['analise_ia'] = analise
            analises_finais.append(item)
        else:
            itens_novos.append(item)

    logger.info(f"  Cache: {len(analises_finais)} | Novos: {len(itens_novos)}")

    if not itens_novos:
        return analises_finais

    # Processa itens novos em lotes
    total_lotes = (len(itens_novos) + TAMANHO_LOTE_IA - 1) // TAMANHO_LOTE_IA

    for i, k in enumerate(range(0, len(itens_novos), TAMANHO_LOTE_IA)):
        lote = itens_novos[k:k + TAMANHO_LOTE_IA]
        logger.info(f"  Lote {i + 1}/{total_lotes} ({len(lote)} itens)")

        # Usa analisar_lote_ia() que chama shared.chamar_ia_com_retry() internamente
        resultado_ia = analisar_lote_ia(modelo, id_loja, lote)

        dict_analises = {
            it.get('produto', ''): it.get('analise', '')
            for it in resultado_ia if isinstance(it, dict)
        }

        for item in lote:
            analise = dict_analises.get(item['produto'], "Análise indisponível")
            item['analise_ia'] = analise
            if analise != "Análise indisponível":
                adicionar_ao_cache(cache, id_loja, item['produto'], item['classe'], analise)
            analises_finais.append(item)

        if k + TAMANHO_LOTE_IA < len(itens_novos):
            time.sleep(PAUSA_ENTRE_LOTES)

    return analises_finais


# ==========================================
# FUNÇÃO PRINCIPAL
# ==========================================

def parse_args() -> argparse.Namespace:
    """
    Parse argumentos de linha de comando.

    Novo: --categoria permite filtrar análise por tipo de produto.
    Exemplos:
        py relatorio_teste.py dados_vendas.csv --categoria CERVEJAS
        py relatorio_teste.py dados_vendas.csv --categoria BEBIDAS --coluna-filtro tipo_produto2
    """
    parser = argparse.ArgumentParser(description="Análise Curva ABC com IA")
    parser.add_argument('arquivo', nargs='?', default='dados_vendas.xlsx',
                        help='Arquivo de dados (CSV ou XLSX)')
    parser.add_argument('--categoria', type=str, default=None,
                        help='Filtrar por categoria (ex: CERVEJAS, PICANHA, BEBIDAS)')
    parser.add_argument('--coluna-filtro', type=str, default='tipo_produto2',
                        choices=['grupo', 'tipo_produto2'],
                        help='Coluna de filtro: grupo (grupo_descr) ou tipo_produto2 (macro)')
    parser.add_argument('--listar-categorias', action='store_true',
                        help='Lista todas as categorias disponíveis e sai')
    parser.add_argument('--gerar-por-categoria', action='store_true',
                        help='Gera JSONs separados por macro-categoria (BEBIDAS, COMIDAS, etc.)')
    return parser.parse_args()


def _executar_analise(
    df_preparado: pd.DataFrame,
    modelo: Any,
    cache: dict,
    arquivo_saida: str,
    label: str = "",
) -> list[dict]:
    """
    Executa análise ABC completa para um DataFrame já preparado.
    Reutilizada tanto para análise consolidada quanto por categoria.
    """
    df_processado = gerar_historico_vendas(df_preparado)
    lista_lojas = df_processado[COL_LOJA].unique()
    total_lojas = len(lista_lojas)
    prefixo = f"[{label}] " if label else ""
    logger.info(f"{prefixo}Processando {total_lojas} lojas...")

    resultado_final = []
    for idx, id_loja in enumerate(lista_lojas, 1):
        logger.info(f"{prefixo}Loja {id_loja} ({idx}/{total_lojas})")
        df_loja = df_processado[df_processado[COL_LOJA] == id_loja]
        resultado_final.append(processar_loja(df_loja, id_loja, modelo, cache))

    if salvar_json(resultado_final, arquivo_saida):
        logger.info(f"{prefixo}Concluído: {total_lojas} lojas → {arquivo_saida}")
    else:
        logger.error(f"{prefixo}Falha ao salvar {arquivo_saida}")

    return resultado_final


def main() -> None:
    """Função principal: orquestra o processamento completo."""
    args = parse_args()
    coluna_filtro = COL_TIPO_PRODUTO2 if args.coluna_filtro == 'tipo_produto2' else COL_GRUPO

    logger.info("=" * 50)
    logger.info("ANÁLISE CURVA ABC COM IA")
    if args.categoria:
        logger.info(f"Filtro: {args.categoria} ({args.coluna_filtro})")
    if args.gerar_por_categoria:
        logger.info("Modo: gerar JSONs por macro-categoria (tipo_produto2)")
    logger.info("=" * 50)

    # 1. Carregar dados (usa shared.carregar_dados)
    df = carregar_dados(args.arquivo)
    if df is None:
        return

    # Modo listar categorias
    if args.listar_categorias:
        cats = listar_categorias(df, coluna_filtro)
        print(f"\nCategorias disponíveis ({len(cats)}):")
        for c in cats:
            print(f"  - {c}")
        return

    # 4. Configurar IA (usa shared.configurar_ia)
    modelo = configurar_ia()
    cache = carregar_cache()

    # ==========================================
    # MODO: GERAR POR CATEGORIA
    # Gera 1 JSON por macro-categoria + 1 consolidado (5 JSONs total)
    # ==========================================
    if args.gerar_por_categoria:
        # 2a. Preparar dados base (sem filtro de categoria, apenas limpeza)
        df_base = preparar_dados(df, categoria_alvo=None, coluna_filtro=coluna_filtro)
        if df_base is None:
            return

        # Gera JSON consolidado (todas as categorias)
        logger.info("=" * 40)
        logger.info("Gerando análise CONSOLIDADA (todas as categorias)")
        _executar_analise(df_base, modelo, cache, ARQUIVO_SAIDA, label="CONSOLIDADO")

        # Gera JSON para cada macro-categoria
        for cat_nome, cat_sufixo in CATEGORIAS_MACRO.items():
            logger.info("=" * 40)
            logger.info(f"Gerando análise para categoria: {cat_nome}")
            df_cat = filtrar_por_categoria(df_base, cat_nome, COL_TIPO_PRODUTO2)
            if df_cat.empty:
                logger.warning(f"Categoria '{cat_nome}' sem dados — pulando")
                continue
            arquivo_cat = os.path.join(PASTA_SAIDA, f"analise_abc_{cat_sufixo}.json")
            _executar_analise(df_cat, modelo, cache, arquivo_cat, label=cat_nome)

        salvar_cache(cache)
        logger.info("=" * 40)
        logger.info("Geração multi-categoria concluída!")
        return

    # ==========================================
    # MODO: ANÁLISE ÚNICA (padrão ou com --categoria)
    # ==========================================
    # 2. Preparar dados (inclui remoção de ruídos + filtro por categoria)
    df = preparar_dados(df, categoria_alvo=args.categoria, coluna_filtro=coluna_filtro)
    if df is None:
        return

    _executar_analise(df, modelo, cache, ARQUIVO_SAIDA)
    salvar_cache(cache)


if __name__ == "__main__":
    main()