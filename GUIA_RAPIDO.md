# ⚡ Guia Rápido - Dashboard Modular

## 🚀 Iniciar o Dashboard

```bash
cd c:\Users\lucca\curvaabc\MP_curvaABC
python -m http.server 8080 --directory docs
# Acesse: http://localhost:8080
```

## 📊 Estrutura do Código

### Estado Global (DashboardState)
```javascript
DashboardState = {
    dadosVendas,      // Dados de vendas carregados
    dadosABC,         // Dados ABC carregados
    canalAtivo,       // 'todos', 'salao', 'ifood'
    lojaAtiva,        // ID da loja selecionada
    periodoAtivo,     // Período selecionado
    granularidade,    // 'diario', 'semanal', 'mensal'
    tipoPratoAtivo    // Tipo de prato filtrado
}
```

## 🔄 Fluxo de Dados

```
1. Carregamento (Camada 1)
   ↓
2. Filtragem (Camada 2)
   ↓
3. Cálculo ABC (Camada 3)
   ↓
4. Atualização UI (Camada 4)
   ↓
5. Orquestração (Camada 5)
   ↓
6. Inicialização (Camada 6)
```

## 🎯 Funções Principais

### Carregamento
```javascript
carregarDados(granularidade, sufixo, canal)
carregarDadosABC(sufixo, canal)
```

### Filtragem
```javascript
filtrarDados(dados, canal)
filtrarPorLoja(dados, lojaId)
filtrarPorPeriodo(dadosLoja, periodo)
```

### Cálculo
```javascript
calcularABC(itens)  // Retorna itens com classe A/B/C
```

### Controle
```javascript
toggleTipoPrato(tipo)      // Alterna filtro de tipo
toggleCanalVendas(canal)   // Alterna canal
setGranularity(tipo)       // Alterna granularidade
processData()              // Processa dados completos
```

## 📈 Otimizações

| Técnica | Benefício |
|---------|-----------|
| **Promise.all** | Carregamento paralelo |
| **RequestAnimationFrame** | UI fluida |
| **DocumentFragment** | Renderização eficiente |
| **Memoização** | Cache de classificações |
| **Try/catch** | Tratamento de erros |

## 🧪 Testar Funcionalidades

### 1. Seleção de Loja
- Clique em qualquer botão de loja
- Verifica se período é atualizado

### 2. Seleção de Período
- Mude entre Diário/Semanal/Mensal
- Verifique se dados são recarregados

### 3. Filtro de Tipo
- Clique em "Executivos", "Carnes", etc.
- Verifique se tabela é filtrada

### 4. Filtro de Canal
- Clique em "Salão" ou "iFood"
- Verifique se dados são recarregados

### 5. Gráficos
- Verifique se Pareto é renderizado
- Verifique se histórico é atualizado

## 🐛 Debug

### Ver Estado
```javascript
console.log(DashboardState)
```

### Ver Dados Carregados
```javascript
console.log('Vendas:', JSON_VENDAS)
console.log('ABC:', JSON_ABC)
```

### Ver Erros
```
F12 → Console → Procure por ❌ ou ⚠️
```

## 📁 Arquivos Importantes

```
docs/
├── index.html              ← Dashboard principal
├── data/
│   ├── vendas_mensal.json
│   ├── vendas_diario.json
│   ├── vendas_semanal.json
│   ├── analise_abc_final.json
│   ├── vendas_canais_salao.json
│   ├── vendas_canais_ifood.json
│   ├── analise_abc_canais_salao.json
│   └── analise_abc_canais_ifood.json
└── js/
    └── dashboard-modular.js  ← Referência modular
```

## ✅ Checklist de Validação

- [ ] Dashboard carrega sem erros
- [ ] Lojas aparecem e são selecionáveis
- [ ] Períodos mudam ao selecionar loja
- [ ] Gráficos são renderizados
- [ ] Filtros funcionam (tipo, canal)
- [ ] Insights aparecem
- [ ] Sem erros no console (F12)
- [ ] Performance é boa (< 1s carregamento)

## 🎓 Aprender Mais

- `ARQUITETURA_MODULAR.md` - Detalhes da arquitetura
- `INTEGRACAO_MODULAR.md` - Guia de integração
- `RESUMO_REFATORACAO.md` - Resumo das mudanças
- `docs/js/dashboard-modular.js` - Código modular de referência

## 💡 Dicas

1. **Performance**: Use DevTools (F12 → Performance) para medir
2. **Debug**: Adicione `console.log()` nas funções
3. **Testes**: Teste cada filtro individualmente
4. **Dados**: Verifique se JSONs existem em `docs/data/`
5. **Erros**: Leia mensagens de erro no console

## 🚀 Pronto para Produção!

Dashboard está **otimizado, modular e funcional** para GitHub Pages.

