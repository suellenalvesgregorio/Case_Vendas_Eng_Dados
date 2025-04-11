# Case_Vendas_Eng_Dados

# Apresentação do Case
O case de engenharia de dados para análise de vendas foi desenvolvido com o propósito de validar conhecimentos técnicos na área, sendo apresentado à banca examinadora Data Master. O projeto visa a realização de uma análise detalhada das vendas, considerando aspectos como cliente, produto, fabricante, geografia, segmento e categoria. Além disso, envolve a medição de KPIs essenciais, incluindo o total de vendas e suas variações diárias, mensais e anuais, permitindo uma visão abrangente do desempenho comercial. A partir da interpretação dos dados, busca-se identificar oportunidades de otimização e definir estratégias de crescimento fundamentadas em insights precisos, promovendo uma abordagem orientada por dados para a tomada de decisão.

# I. Objetivo do Case

## Assunto: Análise de Vendas

### Explicado por:
- Cliente
- Produto
- Fabricante
- Geografia
- Segmento
- Categoria
- Tempo

## Objetivos da análise:
Este case visa fornecer uma visão detalhada das vendas, identificando padrões e oportunidades de otimização com base nos dados analisados.

### Métricas analisadas:
- **Total de vendas** (quantidade e valor)
- **Variações de vendas** 
  - Diária (quantidade e valor)
  - Mensal (quantidade e valor)
  - Anual (quantidade e valor)
- **Ticket médio** (quantidade e valor)
- **Total YTD/MTD** (quantidade e valor)
- **Variação do total YTD/MTD** (quantidade e valor)

## Benefícios esperados:
- Melhor compreensão do comportamento de vendas em diferentes dimensões.
- Identificação de oportunidades estratégicas com base em tendências e variações de desempenho.
- Suporte à tomada de decisão por meio de dados estruturados e visualizações analíticas.

## II. Arquitetura de Solução e Arquitetura Técnica 

## 🏗️ Arquitetura Medallion

A Arquitetura Medallion é um modelo de processamento de dados em camadas que organiza os dados de maneira estruturada e otimizada para análise. Ela é composta pelas seguintes etapas:

### 🔹 Landing Zone
Os dados são armazenados de forma bruta, sem qualquer tipo de processamento. Esta camada é essencial para garantir que os dados originais estejam disponíveis para auditoria e rastreamento.

### 🥉 Bronze
Nesta camada, os dados passam por um pré-processamento inicial, que inclui:
- Limpeza dos dados
- Remoção de cópias redundantes
- Desnormalização para facilitar consultas futuras

### 🥈 Silver
Os dados são enriquecidos e transformados em um formato mais legível para relatórios e análises. Algumas das ações aplicadas nesta camada incluem:
- Agregação de informações
- Transformação dos dados para melhor estruturação
- Padronização para facilitar a exploração analítica

### 🏆 Gold
Nesta última camada, os dados estão prontos para análises avançadas e geração de insights estratégicos. Eles são armazenados de maneira otimizada para:
- Análises em larga escala
- Identificação de oportunidades de negócio
- Suporte à tomada de decisão baseada em dados

Essa abordagem escalonada garante um fluxo de dados eficiente e estruturado, promovendo confiabilidade e melhor desempenho analítico.
