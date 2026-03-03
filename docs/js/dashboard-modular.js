/**
 * Dashboard Modular - Análise ABC com Filtragem por Canal
 * Organizado em funções modulares para melhor manutenção e escalabilidade
 */

// ============================================================
// ESTADO GLOBAL
// ============================================================

const DashboardState = {
    dadosVendas: null,           // Dados de vendas carregados
    dadosABC: null,              // Dados ABC carregados
    canalAtivo: 'geral',         // 'geral', 'salao', 'ifood'
    lojaAtiva: null,             // ID da loja selecionada
    periodoAtivo: null,          // Período selecionado
    granularidade: 'mensal',     // 'diario', 'semanal', 'mensal'
    chartInstance: null,         // Instância do Chart.js
    chartHistoryInstance: null   // Instância do gráfico de histórico
};

// ============================================================
// 1. CARREGAMENTO DE DADOS
// ============================================================

/**
 * Carrega dados de vendas do JSON apropriado baseado no canal
 * @param {string} canal - 'geral', 'salao', 'ifood'
 * @returns {Promise<Object>} Dados carregados
 */
async function carregarDados(canal = 'geral') {
    try {
        let arquivo;
        
        if (canal === 'salao') {
            arquivo = './data/vendas_canais_salao.json';
        } else if (canal === 'ifood') {
            arquivo = './data/vendas_canais_ifood.json';
        } else {
            arquivo = './data/vendas_mensal.json';
        }
        
        const response = await fetch(arquivo);
        if (!response.ok) throw new Error(`Erro ao carregar ${arquivo}`);
        
        const dados = await response.json();
        console.log(`✅ Dados carregados: ${arquivo}`, dados);
        return dados;
    } catch (erro) {
        console.error('❌ Erro ao carregar dados:', erro);
        return null;
    }
}

/**
 * Carrega dados ABC do JSON apropriado baseado no canal
 * @param {string} canal - 'geral', 'salao', 'ifood'
 * @returns {Promise<Object>} Dados ABC carregados
 */
async function carregarDadosABC(canal = 'geral') {
    try {
        let arquivo;
        
        if (canal === 'salao') {
            arquivo = './data/analise_abc_canais_salao.json';
        } else if (canal === 'ifood') {
            arquivo = './data/analise_abc_canais_ifood.json';
        } else {
            arquivo = './data/analise_abc_final.json';
        }
        
        const response = await fetch(arquivo);
        if (!response.ok) throw new Error(`Erro ao carregar ${arquivo}`);
        
        const dados = await response.json();
        console.log(`✅ Dados ABC carregados: ${arquivo}`, dados);
        return dados;
    } catch (erro) {
        console.error('❌ Erro ao carregar dados ABC:', erro);
        return null;
    }
}

// ============================================================
// 2. FILTRAGEM DE DADOS
// ============================================================

/**
 * Filtra dados por canal selecionado
 * @param {Object} dados - Dados brutos
 * @param {string} canal - Canal a filtrar
 * @returns {Object} Dados filtrados
 */
function filtrarDados(dados, canal = 'geral') {
    if (!dados) return null;
    
    // Se for dados de canal específico, já estão filtrados
    if (canal !== 'geral' && dados.canal) {
        return dados;
    }
    
    // Se for 'geral', retorna todos os dados
    return dados;
}

/**
 * Filtra dados por loja
 * @param {Object} dados - Dados de vendas
 * @param {string|number} lojaId - ID da loja
 * @returns {Object} Dados da loja
 */
function filtrarPorLoja(dados, lojaId) {
    if (!dados || !dados.dados_lojas) return null;
    
    const loja = dados.dados_lojas.find(l => 
        String(l.id_loja || l.loja_id) === String(lojaId)
    );
    
    return loja || null;
}

/**
 * Filtra dados por período
 * @param {Object} dadosLoja - Dados da loja
 * @param {string} periodo - Período a filtrar
 * @returns {Object} Dados do período
 */
function filtrarPorPeriodo(dadosLoja, periodo) {
    if (!dadosLoja || !dadosLoja.analises) return null;
    return dadosLoja.analises[periodo] || null;
}

// ============================================================
// 3. CÁLCULO DA CURVA ABC
// ============================================================

/**
 * Calcula classificação ABC baseado em dados filtrados
 * @param {Array} itens - Array de itens com valor
 * @returns {Array} Itens classificados com classe ABC
 */
function calcularABC(itens) {
    if (!itens || itens.length === 0) return [];
    
    // Ordena por valor decrescente
    const ordenados = [...itens].sort((a, b) => 
        (b.valor || b.faturamento || 0) - (a.valor || a.faturamento || 0)
    );
    
    // Calcula total
    const total = ordenados.reduce((sum, item) => 
        sum + (item.valor || item.faturamento || 0), 0
    );
    
    // Classifica ABC (80/15/5)
    let acumulado = 0;
    return ordenados.map(item => {
        const valor = item.valor || item.faturamento || 0;
        acumulado += valor;
        const percentual = total > 0 ? (acumulado / total) * 100 : 0;
        
        let classe = 'C';
        if (percentual <= 80) classe = 'A';
        else if (percentual <= 95) classe = 'B';
        
        return {
            ...item,
            classe,
            percentualAcumulado: percentual
        };
    });
}

// ============================================================
// 4. ATUALIZAÇÃO DA INTERFACE
// ============================================================

/**
 * Atualiza o gráfico de Pareto (Chart.js)
 * @param {Array} dadosProcessados - Dados classificados ABC
 */
function atualizarGraficoPareto(dadosProcessados) {
    if (!dadosProcessados || dadosProcessados.length === 0) {
        console.warn('Sem dados para atualizar gráfico');
        return;
    }
    
    const ctx = document.getElementById('abcChart');
    if (!ctx) return;
    
    // Prepara dados para o gráfico
    const labels = dadosProcessados.slice(0, 20).map(item => item.produto || item.nome);
    const valores = dadosProcessados.slice(0, 20).map(item => item.valor || item.faturamento);
    const cores = dadosProcessados.slice(0, 20).map(item => {
        if (item.classe === 'A') return '#10b981';
        if (item.classe === 'B') return '#f59e0b';
        return '#ef4444';
    });
    
    // Destrói gráfico anterior se existir
    if (DashboardState.chartInstance) {
        DashboardState.chartInstance.destroy();
    }
    
    // Cria novo gráfico
    DashboardState.chartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Faturamento (R$)',
                data: valores,
                backgroundColor: cores,
                borderColor: cores,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            indexAxis: 'y',
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: { beginAtZero: true }
            }
        }
    });
}

/**
 * Atualiza a tabela ABC
 * @param {Array} dadosProcessados - Dados classificados ABC
 */
function atualizarTabelaABC(dadosProcessados) {
    const tbody = document.getElementById('abcTableBody');
    if (!tbody) return;
    
    tbody.innerHTML = '';
    
    dadosProcessados.forEach((item, idx) => {
        const tr = document.createElement('tr');
        const classeColor = {
            'A': '#10b981',
            'B': '#f59e0b',
            'C': '#ef4444'
        }[item.classe] || '#94a3b8';
        
        tr.innerHTML = `
            <td>${idx + 1}</td>
            <td><span style="background: ${classeColor}; color: white; padding: 2px 8px; border-radius: 4px; font-weight: bold;">${item.classe}</span></td>
            <td>${item.produto || item.nome}</td>
            <td>R$ ${(item.valor || item.faturamento || 0).toLocaleString('pt-BR', {minimumFractionDigits: 2})}</td>
            <td>${item.percentualAcumulado?.toFixed(1) || 0}%</td>
        `;
        tbody.appendChild(tr);
    });
}

/**
 * Atualiza toda a interface
 * @param {Array} dadosProcessados - Dados processados e classificados
 */
function atualizarInterface(dadosProcessados) {
    console.log('🔄 Atualizando interface com', dadosProcessados.length, 'itens');
    atualizarGraficoPareto(dadosProcessados);
    atualizarTabelaABC(dadosProcessados);
}

// ============================================================
// 5. ORQUESTRAÇÃO PRINCIPAL
// ============================================================

/**
 * Processa dados completos: carrega → filtra → calcula ABC → atualiza UI
 */
async function processarDados() {
    console.log('📊 Processando dados...', {
        canal: DashboardState.canalAtivo,
        loja: DashboardState.lojaAtiva,
        periodo: DashboardState.periodoAtivo
    });
    
    // 1. Carrega dados
    const dados = await carregarDados(DashboardState.canalAtivo);
    if (!dados) {
        console.error('Falha ao carregar dados');
        return;
    }
    
    // 2. Filtra por canal
    const dadosFiltrados = filtrarDados(dados, DashboardState.canalAtivo);
    
    // 3. Filtra por loja
    const dadosLoja = filtrarPorLoja(dadosFiltrados, DashboardState.lojaAtiva);
    if (!dadosLoja) {
        console.error('Loja não encontrada');
        return;
    }
    
    // 4. Filtra por período
    const dadosPeriodo = filtrarPorPeriodo(dadosLoja, DashboardState.periodoAtivo);
    if (!dadosPeriodo) {
        console.error('Período não encontrado');
        return;
    }
    
    // 5. Calcula ABC
    const itens = dadosPeriodo.itens || [];
    const dadosABC = calcularABC(itens);
    
    // 6. Atualiza interface
    atualizarInterface(dadosABC);
}

/**
 * Alterna canal e recarrega dados
 * @param {string} novoCanal - 'geral', 'salao', 'ifood'
 */
async function alterarCanal(novoCanal) {
    console.log(`🛣️ Alterando canal para: ${novoCanal}`);
    DashboardState.canalAtivo = novoCanal;
    
    // Atualiza botões
    document.querySelectorAll('[data-canal]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.canal === novoCanal);
    });
    
    // Recarrega dados
    await processarDados();
}

// ============================================================
// 6. INICIALIZAÇÃO
// ============================================================

/**
 * Inicializa o dashboard
 */
async function inicializarDashboard() {
    console.log('🚀 Inicializando dashboard...');
    
    // Define valores padrão
    DashboardState.lojaAtiva = '12';
    DashboardState.periodoAtivo = '2026-02';
    DashboardState.canalAtivo = 'geral';
    
    // Configura event listeners
    document.querySelectorAll('[data-canal]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            alterarCanal(e.target.dataset.canal);
        });
    });
    
    // Carrega dados iniciais
    await processarDados();
}

// Inicializa quando o DOM estiver pronto
document.addEventListener('DOMContentLoaded', inicializarDashboard);

