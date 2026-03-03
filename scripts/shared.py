# -*- coding: utf-8 -*-
"""
MÓDULO COMPARTILHADO: Funções utilitárias reutilizadas por todos os scripts de análise.

Centraliza:
- Constantes de colunas e parâmetros
- Carregamento de dados (CSV/XLSX)
- Limpeza de valores monetários
- Remoção de ruídos (modificadores de preparo)
- Filtragem por categoria de produto
- Configuração do modelo Gemini
- Sistema de retry robusto para chamadas à API
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# Tenta importar o Gemini (graceful fallback)
try:
    import google.generativeai as genai
    from google.api_core import exceptions as google_exceptions
    GEMINI_DISPONIVEL = True
except ImportError:
    genai = None  # type: ignore
    google_exceptions = None  # type: ignore
    GEMINI_DISPONIVEL = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv opcional — em CI usa variáveis de ambiente diretamente

logger = logging.getLogger(__name__)

# ==========================================
# CONSTANTES DE COLUNAS DO DATASET
# ==========================================
COL_LOJA = 'loja_id'
COL_PRODUTO = 'material_descr'
COL_VALOR = 'vl_total'
COL_DATA = 'dt_contabil'
COL_QTD = 'qtd'
COL_GRUPO = 'grupo_descr'
COL_TIPO_PRODUTO = 'Tipo Produto'
COL_TIPO_PRODUTO2 = 'Tipo Produto2'

COLUNAS_OBRIGATORIAS = [COL_LOJA, COL_PRODUTO, COL_VALOR, COL_DATA]

# ==========================================
# CATEGORIAS MACRO (coluna tipo_produto2)
# Usadas para gerar análises segmentadas por tipo de produto.
# As 4 macro-categorias cobrem todo o cardápio dos restaurantes.
# ==========================================
CATEGORIAS_MACRO = {
    'BEBIDAS': 'bebidas',
    'COMIDAS': 'comidas',
    'SOBREMESAS': 'sobremesas',
    'OUTROS': 'outros',
}

# ==========================================
# PARÂMETROS DE CONFIGURAÇÃO
# ==========================================
MAX_TENTATIVAS_API = 5
MAX_TENTATIVAS_RATE_LIMIT = 8
DELAY_BASE_RATE_LIMIT = 15  # segundos (plano pago)
DELAY_ENTRE_CHAMADAS = 12.0  # segundos entre chamadas à API

API_KEY = os.environ.get('GEMINI_API_KEY', '')

# ==========================================
# LISTA DE RUÍDOS: modificadores de preparo que NÃO são produtos reais.
# Estes itens aparecem no sistema de pedidos como observações/modificadores
# de preparo da carne mas não geram receita real. São registrados com
# valores de venda fictícios que poluem rankings e análises ABC.
# ==========================================
MODIFICADORES_PREPARO = {
    'AO PONTO', 'AO PONTO MAIS', 'AO PONTO MENOS',
    'BEM PASSADO', 'BEM PASSADA', 'MAL PASSADO', 'MAL PASSADA',
    'PONTO MAIS', 'PONTO MENOS',
    'SEM SAL', 'SEM TEMPERO', 'SEM CEBOLA', 'SEM ALHO',
    'OBSERVACAO', 'OBSERVAÇÃO',
}

# Padrões regex para capturar variações não previstas na lista fixa
PADROES_RUIDO = re.compile(
    r'^(AO PONTO|BEM PASSAD[OA]|MAL PASSAD[OA]|PONTO (MAIS|MENOS))$',
    re.IGNORECASE
)

# Contexto sazonal brasileiro — usado por análises temporais
CONTEXTO_SAZONAL = {
    '01': {'estacao': 'Verão', 'eventos': 'Férias escolares, calor intenso'},
    '02': {'estacao': 'Verão', 'eventos': 'Carnaval, calor'},
    '03': {'estacao': 'Outono', 'eventos': 'Volta às aulas, fim do verão'},
    '04': {'estacao': 'Outono', 'eventos': 'Páscoa, temperaturas amenas'},
    '05': {'estacao': 'Outono', 'eventos': 'Dia das Mães, friagem'},
    '06': {'estacao': 'Inverno', 'eventos': 'Festa Junina, início frio'},
    '07': {'estacao': 'Inverno', 'eventos': 'Férias escolares, frio intenso'},
    '08': {'estacao': 'Inverno', 'eventos': 'Dia dos Pais, frio'},
    '09': {'estacao': 'Primavera', 'eventos': 'Início primavera, clima variável'},
    '10': {'estacao': 'Primavera', 'eventos': 'Dia das Crianças, esquenta'},
    '11': {'estacao': 'Primavera', 'eventos': 'Black Friday, calor chegando'},
    '12': {'estacao': 'Verão', 'eventos': 'Natal, Ano Novo, férias'},
}


# ==========================================
# FUNÇÕES DE CARREGAMENTO E LIMPEZA
# ==========================================

def limpar_valor_monetario(valor: Any) -> float:
    """Converte valor monetário brasileiro (1.234,56) para float."""
    if pd.isna(valor):
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    if isinstance(valor, str):
        try:
            texto = valor.strip().replace('R$', '').replace(' ', '')
            texto = texto.replace('.', '').replace(',', '.')
            return float(texto)
        except ValueError:
            return 0.0
    return 0.0


def carregar_dados(caminho: str) -> Optional[pd.DataFrame]:
    """
    Carrega arquivo de dados (CSV ou XLSX) com detecção automática de formato.
    Tenta múltiplos encodings para CSV. Retorna None em caso de erro.
    """
    if not os.path.exists(caminho):
        logger.error(f"Arquivo não encontrado: {caminho}")
        return None

    extensao = Path(caminho).suffix.lower()

    if extensao in ('.xlsx', '.xls'):
        try:
            df = pd.read_excel(caminho, engine='openpyxl', dtype={COL_LOJA: str})
            logger.info(f"Excel carregado: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Erro ao carregar Excel: {e}")
            return None

    # CSV — tenta múltiplos encodings e separadores
    for encoding in ('latin1', 'utf-8', 'cp1252', 'iso-8859-1'):
        for sep in (',', ';'):
            try:
                df = pd.read_csv(
                    caminho, sep=sep, encoding=encoding,
                    on_bad_lines='skip', dtype={COL_LOJA: str}
                )
                logger.info(f"CSV carregado ({encoding}, sep='{sep}'): {len(df)} registros")
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                continue

    logger.error("Não foi possível carregar o arquivo com nenhum encoding")
    return None


def validar_colunas(df: pd.DataFrame, colunas: list[str] | None = None) -> bool:
    """Valida se o DataFrame possui as colunas obrigatórias."""
    colunas = colunas or COLUNAS_OBRIGATORIAS
    faltantes = [c for c in colunas if c not in df.columns]
    if faltantes:
        logger.error(f"Colunas faltantes: {faltantes}")
        logger.info(f"Colunas disponíveis: {list(df.columns)}")
        return False
    return True


def remover_ruidos(df: pd.DataFrame, col_produto: str = COL_PRODUTO) -> pd.DataFrame:
    """
    Remove registros de modificadores de preparo que não são produtos reais.

    Filtra por duas estratégias complementares:
    1. Lista fixa de termos exatos (MODIFICADORES_PREPARO)
    2. Padrão regex para capturar variações

    Esses itens (ex: "Ao ponto", "Bem passado") são registrados no PDV como
    observações de preparo mas aparecem como itens de venda, poluindo os
    rankings de produtos mais vendidos.

    Args:
        df: DataFrame com dados de vendas
        col_produto: Nome da coluna de produto (padrão: COL_PRODUTO)

    Returns:
        DataFrame limpo, sem modificadores de preparo
    """
    antes = len(df)

    # Normaliza nomes para comparação
    produto_upper = df[col_produto].astype(str).str.strip().str.upper()

    # Estratégia 1: match exato contra lista fixa
    mask_exato = produto_upper.isin(MODIFICADORES_PREPARO)

    # Estratégia 2: match regex para variações
    mask_regex = produto_upper.str.match(PADROES_RUIDO, na=False)

    # Remove ambos
    mask_ruido = mask_exato | mask_regex
    df_limpo = df[~mask_ruido].copy()

    removidos = antes - len(df_limpo)
    if removidos > 0:
        logger.info(f"Ruídos removidos: {removidos} registros de modificadores de preparo")

    return df_limpo


def filtrar_por_categoria(
    df: pd.DataFrame,
    categoria_alvo: Optional[str] = None,
    coluna_filtro: str = COL_TIPO_PRODUTO2,
) -> pd.DataFrame:
    """
    Filtra o DataFrame por categoria de produto ANTES de calcular rankings.

    Permite comparar produtos da mesma categoria (carnes vs carnes,
    bebidas vs bebidas), tornando a análise mais justa e relevante.

    Args:
        df: DataFrame com dados de vendas
        categoria_alvo: Nome da categoria para filtrar (ex: "CARNES PROCESSADAS",
                        "REFRIGERANTES", "BEBIDAS"). Se None, retorna todos.
        coluna_filtro: Coluna a ser usada para filtro. Opções:
                       - COL_GRUPO (grupo_descr): mais granular (ex: REFRIGERANTES)
                       - COL_TIPO_PRODUTO2 (Tipo Produto2): mais macro (BEBIDAS, COMIDAS)

    Returns:
        DataFrame filtrado pela categoria. Se a categoria não existe, retorna vazio.
    """
    if not categoria_alvo:
        return df

    if coluna_filtro not in df.columns:
        logger.warning(f"Coluna '{coluna_filtro}' não encontrada. Retornando dados sem filtro.")
        return df

    categoria_upper = categoria_alvo.strip().upper()
    valores_coluna = df[coluna_filtro].astype(str).str.strip().str.upper()

    df_filtrado = df[valores_coluna == categoria_upper].copy()

    if df_filtrado.empty:
        categorias_disponiveis = sorted(valores_coluna.unique())
        logger.warning(
            f"Categoria '{categoria_alvo}' não encontrada na coluna '{coluna_filtro}'. "
            f"Categorias disponíveis: {categorias_disponiveis}"
        )
    else:
        logger.info(
            f"Filtro aplicado: '{categoria_alvo}' → {len(df_filtrado)} registros "
            f"(de {len(df)} total)"
        )

    return df_filtrado


def listar_categorias(df: pd.DataFrame, coluna: str = COL_TIPO_PRODUTO2) -> list[str]:
    """Retorna lista de categorias únicas disponíveis no dataset."""
    if coluna not in df.columns:
        return []
    return sorted(df[coluna].dropna().astype(str).str.strip().str.upper().unique().tolist())


# ==========================================
# CONFIGURAÇÃO DO MODELO GEMINI
# ==========================================

def configurar_ia(
    model_name: str = "gemini-2.0-flash-lite",
    temperature: float = 0.2,
) -> Optional[Any]:
    """
    Configura e retorna o modelo Gemini para análise.
    Retorna None se a API Key não está configurada ou o SDK não está disponível.
    """
    if not GEMINI_DISPONIVEL:
        logger.warning("SDK google-generativeai não instalado. IA desabilitada.")
        return None

    if not API_KEY:
        logger.warning("GEMINI_API_KEY não configurada. IA desabilitada.")
        return None

    try:
        genai.configure(api_key=API_KEY)
        modelo = genai.GenerativeModel(
            model_name=model_name,
            generation_config={
                "temperature": temperature,
                "response_mime_type": "application/json",
            }
        )
        logger.info(f"Modelo {model_name} configurado com sucesso")
        return modelo
    except Exception as e:
        logger.error(f"Erro ao configurar modelo Gemini: {e}")
        return None


# ==========================================
# SISTEMA DE RETRY ROBUSTO PARA API
# ==========================================

def chamar_ia_com_retry(
    modelo: Any,
    prompt: str,
    max_tentativas: int = MAX_TENTATIVAS_API,
    max_rate_limit: int = MAX_TENTATIVAS_RATE_LIMIT,
    delay_entre_chamadas: float = DELAY_ENTRE_CHAMADAS,
) -> Optional[list[dict]]:
    """
    Chama a API Gemini com sistema de retry robusto e retorna o JSON parseado.

    Implementa:
    - Exponential backoff para erros de conexão
    - Delays progressivos para rate limit (429)
    - Retry para JSON malformado
    - Limpeza automática de markdown na resposta

    Args:
        modelo: Modelo Gemini configurado
        prompt: Prompt a ser enviado
        max_tentativas: Máximo de tentativas para erros gerais
        max_rate_limit: Máximo de tentativas para rate limit
        delay_entre_chamadas: Pausa entre cada chamada (segundos)

    Returns:
        Lista de dicts parseada do JSON, ou None em caso de falha total
    """
    if not modelo:
        return None

    tentativa = 0
    tentativas_rate_limit = 0
    tentativas_json = 0
    max_json_retries = 3

    while tentativa < max_tentativas or tentativas_rate_limit < max_rate_limit:
        tentativa += 1
        try:
            time.sleep(delay_entre_chamadas)
            resposta = modelo.generate_content(prompt)

            if not resposta or not resposta.text:
                logger.warning(f"Resposta vazia (tentativa {tentativa})")
                continue

            # Limpa markdown da resposta antes de parsear
            texto = _limpar_json_resposta(resposta.text)
            resultado = json.loads(texto)

            if not isinstance(resultado, list):
                logger.warning(f"Resposta não é lista: {type(resultado)}")
                return None

            return resultado

        except json.JSONDecodeError as e:
            tentativas_json += 1
            logger.warning(f"JSON inválido (tentativa {tentativas_json}/{max_json_retries}): {e}")
            if tentativas_json >= max_json_retries:
                return None
            tentativa -= 1  # Não conta como tentativa geral
            continue

        except Exception as e:
            if google_exceptions and isinstance(e, google_exceptions.ResourceExhausted):
                tentativas_rate_limit += 1
                tempo = DELAY_BASE_RATE_LIMIT * tentativas_rate_limit + random.uniform(0, 5)
                logger.warning(
                    f"Rate limit ({tentativas_rate_limit}/{max_rate_limit}). "
                    f"Aguardando {tempo:.0f}s..."
                )
                time.sleep(tempo)
                if tentativas_rate_limit >= max_rate_limit:
                    logger.error("Rate limit persistente. Abortando.")
                    return None
                tentativa -= 1
                continue

            if google_exceptions and isinstance(
                e, (google_exceptions.ServiceUnavailable, google_exceptions.DeadlineExceeded)
            ):
                tempo = (2 ** tentativa) + random.uniform(0, 1)
                logger.warning(f"Erro de conexão (tentativa {tentativa}): {e}")
                if tentativa < max_tentativas:
                    time.sleep(tempo)
                    continue
                logger.error("Falha definitiva após todas as tentativas.")
                return None

            if isinstance(e, ConnectionError):
                tempo = (2 ** tentativa) + random.uniform(0, 1)
                logger.warning(f"ConnectionError (tentativa {tentativa}): {e}")
                if tentativa < max_tentativas:
                    time.sleep(tempo)
                    continue
                return None

            logger.error(f"Erro inesperado: {type(e).__name__}: {e}")
            return None

    return None


def _limpar_json_resposta(texto: str) -> str:
    """Remove markdown code blocks e corrige JSON malformado da resposta da IA."""
    texto = re.sub(r'^```json\s*', '', texto.strip())
    texto = re.sub(r'^```\s*', '', texto)
    texto = re.sub(r'\s*```$', '', texto)
    texto = re.sub(r',\s*]', ']', texto)
    texto = re.sub(r',\s*}', '}', texto)
    match = re.search(r'\[[\s\S]*\]', texto)
    if match:
        texto = match.group(0)
    return texto.strip()


# ==========================================
# UTILITÁRIOS
# ==========================================

def obter_contexto_sazonal(mes_ref: str) -> dict[str, str]:
    """Retorna contexto sazonal brasileiro para o mês (formato '2024-01')."""
    try:
        _, mes = mes_ref.split('-')
        return CONTEXTO_SAZONAL.get(mes, {'estacao': 'N/A', 'eventos': 'Período padrão'})
    except ValueError:
        return {'estacao': 'N/A', 'eventos': 'N/A'}


def extrair_nome_mes(mes_periodo: str) -> str:
    """Converte período '2025-01' para 'Janeiro/2025'."""
    meses = {
        '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março', '04': 'Abril',
        '05': 'Maio', '06': 'Junho', '07': 'Julho', '08': 'Agosto',
        '09': 'Setembro', '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro',
    }
    try:
        ano, mes = mes_periodo.split('-')
        return f"{meses.get(mes, mes)}/{ano}"
    except ValueError:
        return mes_periodo


def converter_id_loja(id_loja: Any) -> int | str:
    """Converte ID de loja para int se possível, senão mantém string."""
    try:
        return int(id_loja)
    except (ValueError, TypeError):
        return str(id_loja)


def gerar_sufixo_categoria(categoria: str) -> str:
    """
    Converte nome da macro-categoria para sufixo de arquivo.
    Ex: 'BEBIDAS' → 'bebidas', 'COMIDAS' → 'comidas'.
    Usa o mapeamento CATEGORIAS_MACRO para consistência.
    """
    return CATEGORIAS_MACRO.get(categoria.strip().upper(), categoria.strip().lower())


def salvar_json(dados: Any, caminho: str) -> bool:
    """Salva dados em arquivo JSON com criação automática de diretório."""
    try:
        Path(caminho).parent.mkdir(parents=True, exist_ok=True)
        with open(caminho, 'w', encoding='utf-8') as f:
            json.dump(dados, f, ensure_ascii=False, indent=2, default=str)
        tamanho_kb = Path(caminho).stat().st_size / 1024
        logger.info(f"Salvo: {caminho} ({tamanho_kb:.1f} KB)")
        return True
    except (IOError, OSError) as e:
        logger.error(f"Erro ao salvar {caminho}: {e}")
        return False
