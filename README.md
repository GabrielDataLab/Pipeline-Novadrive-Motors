# Pipeline NovaDrive Motors 

Pipeline de dados completo para uma montadora de veículos fictícia — extrai dados transacionais de um banco PostgreSQL, carrega incrementalmente em um Data Warehouse no Snowflake via Apache Airflow, e transforma os dados em camadas analíticas com dbt, seguindo arquitetura medallion (Stage → Dimensions/Facts → Analytics).

---

## Arquitetura

```
┌─────────────────────┐
│  PostgreSQL          │
│  (Banco transacional)│
│  7 tabelas de vendas │
└────────┬────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Apache Airflow 3               │
│  EC2 Ubuntu (AWS) via Docker    │
│  Carga incremental por ID       │
│  DAG: postgres_to_snowflake     │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Snowflake — Schema STAGE       │
│  7 tabelas brutas               │
│  cidades, clientes,             │
│  concessionarias, estados,      │
│  veiculos, vendas, vendedores   │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  dbt — Transformações           │
│                                 │
│  Stage (views)                  │
│  → limpeza e padronização       │
│                                 │
│  Dimensions + Facts (tables)    │
│  → modelagem dimensional        │
│  → Star Schema                  │
│                                 │
│  Analyses (tables)              │
│  → agregações analíticas        │
└────────┬────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│  Snowflake — Schema ANALYTIC    │
│  11 tabelas prontas             │
│  6 dimensões + 1 fato +         │
│  4 análises de vendas           │
└─────────────────────────────────┘
```

---

## Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Fonte de dados | PostgreSQL |
| Orquestração | Apache Airflow 3 |
| Infraestrutura | AWS EC2 (Ubuntu) + Docker |
| Data Warehouse | Snowflake |
| Transformação | dbt (data build tool) |

---

## Estrutura do Projeto

```
pipeline-novadrive-motors/
├── dags/
│   └── postgres_to_snowflake.py    # DAG de carga incremental
├── dbt/
│   ├── models/
│   │   ├── stage/                  # Views de limpeza e padronização
│   │   │   ├── stg_cidades.sql
│   │   │   ├── stg_clientes.sql
│   │   │   ├── stg_concessionarias.sql
│   │   │   ├── stg_estados.sql
│   │   │   ├── stg_veiculos.sql
│   │   │   ├── stg_vendas.sql
│   │   │   └── stg_vendedores.sql
│   │   ├── dimensions/             # Tabelas dimensão (Star Schema)
│   │   │   ├── dim_cidades.sql
│   │   │   ├── dim_clientes.sql
│   │   │   ├── dim_concessionarias.sql
│   │   │   ├── dim_estados.sql
│   │   │   ├── dim_veiculos.sql
│   │   │   └── dim_vendedores.sql
│   │   ├── facts/                  # Tabela fato de vendas
│   │   │   └── fct_vendas.sql
│   │   └── analyses/               # Análises agregadas prontas para consumo
│   │       ├── analise_vendas_concessionaria.sql
│   │       ├── analise_vendas_temporal.sql
│   │       ├── analise_vendas_veiculo.sql
│   │       └── analise_vendas_vendedor.sql
│   ├── tests/
│   │   └── test.sql                # Teste de validação de preço de venda
│   ├── source.yml                  # Definição das fontes no Snowflake STAGE
│   └── dbt_project.yml             # Configuração do projeto dbt
├── sql/
│   └── setup_snowflake.sql         # Criação do database, schema e tabelas no Snowflake
├── scripts/
│   └── ec2_setup.sh                # Instalação do Docker e Airflow no EC2
└── README.md
```

---

## Detalhes Técnicos

### Carga Incremental (DAG)

A DAG `postgres_to_snowflake` realiza carga incremental das 7 tabelas transacionais. Para cada tabela, o pipeline:

1. Consulta o `MAX(id)` atual no Snowflake
2. Extrai apenas os registros novos do PostgreSQL (onde `id > max_id`)
3. Insere os novos registros no Snowflake (schema STAGE)

Isso garante que reexecuções não duplicam dados e o pipeline processa apenas o delta de cada execução.

### Modelagem Dimensional (dbt)

Os dados seguem arquitetura medallion em três camadas:

**Stage (views)** — limpeza e padronização dos dados brutos:
- Padronização de strings com `INITCAP()`
- Tratamento de nulos com `COALESCE(data_atualizacao, data_inclusao)`
- Referência às fontes via `{{ source() }}`

**Dimensions + Facts (tables)** — Star Schema:
- 6 dimensões: cidades, clientes, concessionárias, estados, veículos, vendedores
- 1 tabela fato: vendas (métricas de valor, referências às dimensões)
- Referências entre modelos via `{{ ref() }}`
- `fct_vendas` materializada como `incremental` com `unique_key='venda_id'`

**Analyses (tables)** — agregações prontas para consumo:
- Vendas por concessionária (quantidade, total, valor médio)
- Vendas por período temporal
- Vendas por veículo
- Vendas por vendedor

### Teste de Qualidade

O projeto inclui teste de validação de regra de negócio: verifica se o valor de venda respeitou a faixa permitida (entre 95% e 100% do valor sugerido pelo fabricante). Vendas fora dessa faixa são reportadas para investigação.

---

## Como Reproduzir

### Pré-requisitos

- Conta AWS com instância EC2 (Ubuntu) criada (recomendado com no mínimo 8GIB de Memória e 20GIB de Armazenamento)
- Conta Snowflake (trial gratuito disponível)
- Conta dbt (trial gratuito disponível)
- Acesso ao banco PostgreSQL da NovaDrive Motors

### Banco de Dados NovaDrive Motors

O banco PostgreSQL utilizado neste projeto é o banco público disponibilizado
pelo curso. Use as credenciais abaixo para conectar:

| Parâmetro | Valor |
|---|---|
| Host | 159.223.187.110 |
| Database | novadrive |
| User | etlreadonly |
| Password | novadrive376A@ |
| Port | 5432 |

> O usuário `etlreadonly` tem permissão apenas de leitura — ideal para
> extração de dados sem risco de alteração na fonte.

### 1. Configurar o ambiente no EC2

> **Importante:** após criar a instância EC2, acesse as configurações de
> **Grupos de Segurança** da instância, clique em **Editar regras de entrada**
> e adicione uma regra liberando a porta **8080** para qualquer origem
> (0.0.0.0/0). Sem isso, o Airflow não ficará acessível pelo navegador.

```bash
# Execute o script de setup na instância EC2
bash scripts/ec2_setup.sh
```

O script instala Docker, Docker Compose e sobe o Apache Airflow 3. Acesse o Airflow em `<Endereço IPv4 Público>:8080`.

### 2. Configurar conexões no Airflow

No Airflow, crie duas conexões:
- `postgres` — apontando para o banco PostgreSQL da NovaDrive
- `snowflake` — apontando para o seu Warehouse no Snowflake

### 3. Configurar o Snowflake

Execute o script `sql/setup_snowflake.sql` no Snowflake para criar o database `NOVADRIVE`, o schema `STAGE` e as 7 tabelas.

### 4. Executar a DAG

Copie `dags/postgres_to_snowflake.py` para a pasta `dags/` mapeada no Airflow, ative a DAG `postgres_to_snowflake` e dispare manualmente.


### 5. Configurar e executar o dbt

1. Inicie um novo projeto e conecte o projeto ao Snowflake usando as credenciais do seu Warehouse
3. No **File Explorer** do dbt studio, crie a estrutura de pastas dentro de `models/`:
   - `models/stage/` — cole os arquivos `stg_*.sql`
   - `models/dimensions/` — cole os arquivos `dim_*.sql`
   - `models/facts/` — cole o arquivo `fct_vendas.sql`
   - `models/analyses/` — cole os arquivos `analise_*.sql`
4. Na raiz do projeto, substitua o `dbt_project.yml` gerado automaticamente
   pelo arquivo `dbt_project.yml` deste repositório
5. Crie o arquivo `source.yml` na raiz de `models/` com o conteúdo
   do arquivo `source.yml` deste repositório
6. Na pasta `tests/`, cole o arquivo `test.sql`
7. Execute no terminal do dbt:

```bash
   dbt run      # materializa todos os modelos no schema ANALYTIC
   dbt test     # executa os testes de qualidade
```

---

## Resultado Final

Schema `ANALYTIC` no Snowflake com **11 tabelas** e **7 views** prontas para consumo analítico:

**Views — Camada Stage (limpeza e padronização):**

| View | Descrição |
|---|---|
| `STG_CIDADES` | Cidades com nome padronizado via INITCAP |
| `STG_CLIENTES` | Clientes com data de atualização tratada |
| `STG_CONCESSIONARIAS` | Concessionárias com campos normalizados |
| `STG_ESTADOS` | Estados com sigla e nome padronizados |
| `STG_VEICULOS` | Veículos com valor sugerido e tipo |
| `STG_VENDAS` | Transações brutas preparadas para a fato |
| `STG_VENDEDORES` | Vendedores com referência à concessionária |

**Tables — Camada Analítica (Star Schema + Análises):**

| Tabela | Tipo | Descrição |
|---|---|---|
| `DIM_CIDADES` | Dimensão | Cidades com nome padronizado |
| `DIM_CLIENTES` | Dimensão | Clientes da montadora |
| `DIM_CONCESSIONARIAS` | Dimensão | Rede de concessionárias |
| `DIM_ESTADOS` | Dimensão | Estados brasileiros |
| `DIM_VEICULOS` | Dimensão | Catálogo de veículos com preço sugerido |
| `DIM_VENDEDORES` | Dimensão | Equipe de vendas |
| `FCT_VENDAS` | Fato | Transações de venda com métricas |
| `ANALISE_VENDAS_CONCESSIONARIA` | Análise | Ranking de vendas por concessionária |
| `ANALISE_VENDAS_TEMPORAL` | Análise | Evolução temporal das vendas |
| `ANALISE_VENDAS_VEICULO` | Análise | Performance de vendas por modelo |
| `ANALISE_VENDAS_VENDEDOR` | Análise | Ranking de vendedores |

---

## Destaques de Engenharia

- **Dupla carga incremental:** tanto a DAG (por MAX id) quanto a `fct_vendas` no dbt (`materialized='incremental'` + `unique_key`) garantem que nenhuma execução reprocessa dados já carregados
- **Arquitetura medallion:** separação clara entre dados brutos (STAGE), transformados (dimensões/fato) e analíticos (analyses)
- **Modelagem dimensional:** Star Schema com 6 dimensões e 1 fato — padrão de Data Warehouse para consultas analíticas performáticas
- **Teste de qualidade:** validação de regra de negócio no dbt garante integridade dos dados antes do consumo
- **Infraestrutura reproduzível:** script de setup automatiza instalação do ambiente no EC2

---

## Agendamento com dbt

O projeto está configurado com um **Environment de Produção** no dbt 
apontando para o schema `ANALYTIC` no Snowflake. Um **Job** foi configurado
para executar `dbt run` e `dbt test` sequencialmente, garantindo que os
modelos sejam materializados e validados a cada execução.

Para configurar o mesmo ambiente:
1. No dbt, crie um Environment apontando para o Snowflake
2. Crie um Job com os comandos `dbt run` e `dbt test`
3. Configure o agendamento conforme a frequência de atualização desejada

---

## Autor

**Gabriel Medeiros** — [LinkedIn](https://linkedin.com/in/gabrielhdata) · [GitHub](https://github.com/GabrielDataLab)
