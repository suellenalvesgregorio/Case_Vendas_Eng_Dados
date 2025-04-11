# Case_Vendas_Eng_Dados

# Apresentação do Case
Este repositório contém  um case de engenharia de dados, com o objetivo de realizar a análise de vendas e foi desenvolvido com o propósito de validar conhecimentos técnicos na área, sendo apresentado à banca examinadora Data Master. O projeto visa a realização de uma análise detalhada das vendas, considerando aspectos como cliente, produto, fabricante, geografia, segmento e categoria. Além disso, envolve a medição de KPIs essenciais, incluindo o total de vendas e suas variações diárias, mensais e anuais, permitindo uma visão abrangente do desempenho comercial. A partir da interpretação dos dados, busca-se identificar oportunidades de otimização e definir estratégias de crescimento fundamentadas em insights precisos, promovendo uma abordagem orientada por dados para a tomada de decisão.

## I. Objetivo do Case

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

## Modelo Conceitual
![Modelo Conceitual](./Modelo_Conceitual.jpg)

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

## Case de Arquitetura Medallion com PySpark, Databricks e Delta Lake

Este case foi desenvolvido utilizando **notebooks em PySpark**, com a implementação da arquitetura **Medallion** nas camadas **Bronze, Silver e Gold**, por meio do **Databricks e Delta Lake**. Entre as funcionalidades implementadas, destacam-se:

- Criação de **surrogate keys** (chaves substitutas) para as dimensões;
- Otimização da **tabela fato** na camada Gold.

### Estrutura dos Notebooks

Foram desenvolvidos **9 notebooks**, organizados e numerados para facilitar a compreensão da ordem de execução:

1. `000 Criando os Diretórios`  
2. `001 Importando o Arquivo`  
3. `002 Load Camada Bronze`  
4. `003 Transformações Camada Silver`  
5. `004 Load Camada Gold Delta`  
6. `005 Load Gold Delta Incremental`  
7. `006 Consultas Otimizadas`  
8. `007 Criação de tabelas Delta`  
9. `008 Rotinas de Manutenção Delta`  

Cada notebook contém **explicações e justificativas** sobre sua utilização e implementação, proporcionando uma visão clara do processo.

## 🏗️ Arquitetura Medallion

A Arquitetura Medallion é um modelo de processamento de dados em camadas que organiza os dados de maneira estruturada e otimizada para análise. Ela é composta pelas seguintes etapas:

### 🔹 Landing Zone
Os dados são armazenados de forma bruta, sem qualquer tipo de processamento. Esta camada é essencial para garantir que os dados originais estejam disponíveis para auditoria e rastreamento.

### 🥉 Bronze
A camada Bronze é responsável pela importação dos dados brutos. Nesta camada, foi realizado a importação de um arquivo .csv contendo dados fictícios de vendas referentes ao ano de 2012, armazenado neste repositório com o nome dados_vendas_2012.csv. Esse será o único arquivo a ser importado para a camada Bronze. Os detalhes e justificativas sobre a camada bronze podem ser encontrados no notebook 002 Load Camada Bronze.

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

## III. Explicações e Justificativas sobre o case desenvolvido.

## IV. Melhorias e Considerações Finais.
