# Data Engineer Pleno

Este desafio tem como objetivo avaliar as habilidades técnicas e de raciocínio da pessoa candidata à vaga de Data Engineer Pleno na **Pier**. Queremos conhecer como você pensa, estrutura e implementa soluções de dados, da extração à disponibilização para análise.

## Objetivo

Este projeto implementa um **pipeline de ETL totalmente automatizado** para dados das loterias federais brasileiras.  
O pipeline lê um dataset bruto consolidado (JSON), separa por loteria, executa validações e limpeza robusta, gera features derivadas e salva os resultados em um banco SQLite.

O objetivo é fornecer um fluxo **claro, replicável e escalável**, pronto para análises, dashboards e estudos estatísticos dos concursos.

# Principais funcionalidades

- Separação automática dos concursos: Mega-Sena, Lotofácil, Timemania, Dia de Sorte, MaisMilionária.
- Limpeza completa e normalização das listas, datas, valores monetários e tipos variados.
- Validação rigorosa das dezenas (quantidade e intervalo por loteria).
- Feature engineering completo:
  - divisão das dezenas em colunas individuais  
  - frequência de pares/ímpares  
  - range das dezenas  
  - total de ganhadores e valores pagos  
  - extração de informações do local  
  - identificação de ganhadores online  
  - features de data (dia, mês, ano, semana ISO, etc.)  
- Salvamento no SQLite, incluindo colunas de listas convertidas para JSON.
- Script executável: `python run_etl.py`
- Notebook de análise: `Testando Resultados.ipynb`

# Estrutura do Projeto
- data/
  - raw/
    - dataset.json -> Arquivo bruto original
    - megasena.csv
    - lotofacil.csv
    - timemania.csv
    - maismilionaria.csv
    - diadesorte.csv
- src/
  - commons/
    - data_cleaning.py
    - feature_engineering.py
    - transform_to_sqlite.py
    - utils.py
- loterias.db -> Banco SQLite gerado
- run_etl.py -> Executa todo o fluxo
- Testando Resultados.ipynb -> Notebook para exploração e testes
- README.md -> Este arquivo

# Comandos para rodar projeto
### 1. Criar virtual env (recomendado)
```bash
python -m venv venv
source venv/bin/activate       # Linux/macOS
venv\Scripts\activate          # Windows
```
### 2. Instalar dependências
```bash
pip install -r requirements.txt
````
### 3. Execução da Pipeline
```bash
python run_etl.py
```

O script irá:
- Ler ```data/raw/data.json```
- Separar por tipo de loteria
- Aplicar limpezas e validações
- Criar features e normalizações
- Inserir cada loteria como tabela no banco SQLite ```loterias.db```

A execução pode ser acompanhada diretamente do terminal com auxílio de ```tqdm```

E ao final da execução você verá a seguinte mensagem no terminal

```bash
Tabelas salvas em: loterias.db
```

# Notebook para testes/análise
Além da pipeline, o notebook ```Testando resultados.ipynb``` serve para acessar, testar e visualizar os resultados salvos nas tabelas

# Arquitetura do ETL
O ETL foi dividido em três etapas principais, e cada um possui funções próprias, modularizadas e documentadas.

## Etapa 0 - Separação das Tabelas
Arquivo: ```utils.py```

Responsável por:
- Carregar o data.json
- padronizar o nome das loterias
- separar os dados por concurso
- salvar CSVs independentes

## Etapa 1 - Limpeza e Validações
Arquivo: ```data_cleaning.py```

Responsável por:
- Remoção de linhas completamente nulas
- normalização de listas/dicionários via ```ast_literal_eval```
- conversão de tipos (datas, monetários e booleanos)
- validação de dezenas (quiantidade e intervalo permitidos)

## Etapa 2 - Engenharia de Features
Arquivo: ```feature_engineering.py```

Responsável por:
- Features de data
- expansão das dezenas em colunas
- separação de "local" em: nome_local, cidade, estado
- expansão da lista de premiações
- total de ganhadores
- total pago
- média real por ganhador
- extração de ufGanhador, municipioGanhador
- flag ```ticketGanhadorOnline```
- cálculos adicionais (pares/ímpares, range de dezenas e razão estimado/acumulado)

## Etapa 3 - Salvando no SQLite
Arquivo: ```transform_to_sqlite.py```

Responsável por:
- Detecção automática de colunas com listas
- conversão dessas colunas para JSON
- salvamento no SQLite usando to_sql

Cada tabela do banco leva o **nome da loteria**:
- megasena
- lotofacil
- diadesorte
- timemania
- maismilionaria

# Decisões de Design
- Pipeline Modularizada:
  - facilidade de manutenção
  - possibilidade de testes isolados
  - possibilidade de expansação e adaptação futura
- SQLite como destino
  - exigência do desafio
  - simples, local e portátil
- Colunas dinâmicas
  - premiações geram colunas conforme número de "faixa"
  - dezenas geram colunas conforme o total da loteria
  - solução escalável para diferentes concursos
- Validação rígida
  - garante integridade
  - evita números fora da faixa (inválidos)
  - remove entradas malformadas
- Conversão JSON para listas
  - SQLite não suporta listas e dicionários
  - saída legível
  - conversão de volta simples

# Evolução e Processo de Desenvolvimento
O desenvolvimento deste projeto seguiu uma progressão natural, partindo de uma abordagem exploratória até atingir uma arquitetura modular e escalável:

- Implementação inicial em notebook

  O processamento foi inicialmente desenvolvido em um Jupyter Notebook, focado exclusivamente nos dados da **Mega-Sena**.  
Nesta fase, o objetivo era validar hipóteses, entender a estrutura dos dados e construir as primeiras rotinas de limpeza e transformação.
- Generalização para múltiplos concursos:

  Após a validação da lógica para a Mega-Sena, o código foi adaptado para suportar **todos os concursos da loteria federal**, tratando diferenças importantes como:  
   - quantidade de dezenas por modalidade;  
   - faixas e regras de premiação distintas;  
   - colunas específicas (ex.: *trevos*, *timeCoracao*, *mesSorte*);  
   - intervalos válidos de dezenas.

   Essa etapa consolidou regras dinâmicas que permitem que a pipeline funcione para qualquer modalidade, presente ou futura.
- Modularização e construção da Pipeline final
  
  Por fim, todo o código foi organizado em módulos independentes, formando uma pipeline completa de ETL, executada por meio do script ```run_etl.py```

  Essa versão final trouxe:
  - separação clara das responsabilidades
  - reusabilidade
  - escalabilidade
  - compatibilidade com SQLite
  - execução automatizada

Essa evolução garantiu que o projeto fosse desenvolvido de forma progressiva, validando cada etapa antes de avançar para a arquitetura final.

# Próximos passos
- API de consulta de dados
- Dashboard para visualização completa
- Testes unitários
- Orquestração de Pipeline com Databricks Workflow, AirFlow, etc

# Suporte
Em caso de dúvidas, sugestões ou melhorias, fique à vontade para abrir uma issue ou me chamar diretamente.