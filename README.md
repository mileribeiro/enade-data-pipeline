# Desafio Técnico - Analista de Dados Pleno

Pipeline em AWS para transformar os Microdados ENADE 2023 em dados analíticos e um dashboard para a coordenação acadêmica da Unifor.

## Objetivo

Responder às três perguntas do desafio:

1. A Unifor está no ENADE 2023? Quais cursos, áreas e modalidades ela possui?
2. A nota geral média difere entre Presencial e EaD?
3. Quais são os 10 cursos da Unifor com maior nota geral média?

## Arquitetura

```text
Fonte oficial do INEP
        ↓
AWS Glue Job de ingestão → S3 Bronze
        ↓
AWS Glue Job de transformação → S3 Silver
        ↓
AWS Glue Job de modelagem → S3 Gold
        ↓
Glue Data Catalog → Athena → QuickSight
```

Os Glue Jobs são executados em ordem: Bronze, Silver e Gold. Cada job executa suas validações de qualidade e falha quando encontra um erro crítico, impedindo a publicação da camada seguinte. Logs e erros ficam registrados no CloudWatch.

## Camadas de dados

| Camada | Armazenamento | Conteúdo |
| Bronze | Bucket S3 Bronze | Arquivos originais do INEP, sem alteração. |
| Silver | Bucket S3 Silver | TXT normalizados e convertidos em Parquet, um dataset por arquivo; metadados do dicionário normalizados. |
| Gold | Bucket S3 Gold | Modelo dimensional pronto para consulta no Athena, em Parquet. |

Os buckets terão acesso privado, criptografia e versionamento. Dados brutos não serão versionados no Git.

## Desenvolvimento local

Crie e ative o ambiente virtual com:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

O ambiente é usado para ferramentas auxiliares e testes locais. As bibliotecas `awsglue` e Spark são fornecidas pelo runtime do AWS Glue e não precisam ser instaladas neste ambiente.

## Pipeline

### 1. Ingestão - Bronze

O ZIP oficial dos [Microdados ENADE 2023](https://download.inep.gov.br/microdados/microdados_enade_2023.zip) e a referência pública e-MEC `ies.xls` ficam na pasta local `data/`. O Terraform os publica em `2023/bronze/archive/` durante o `terraform apply`. Um AWS Glue Job em Python valida o pacote e grava os TXT extraídos e o dicionário na raiz da Bronze. Não há limpeza ou transformação nesta etapa.

### 2. Transformação - Silver

Um AWS Glue Job (PySpark) lê os TXT da Bronze e o dicionário oficial. Cada TXT é tratado independentemente e gravado em Parquet em `silver/<arquivo>/`.

As transformações são seguras e reversíveis: padronização de nomes de colunas, remoção de espaços nas extremidades e conversão de campos vazios para `NULL`. Códigos permanecem como texto para preservar zeros à esquerda; `NT_GER` é convertido para `DECIMAL(5,2)`, com `.` também tratado como ausência de nota. O job também publica:

- `silver/dim_variable`: definição, tipo, tamanho e regra de domínio de cada variável;
- `silver/dim_variable_value`: códigos e rótulos para variáveis discretas;
- `silver/dim_ies_reference`: referência e-MEC normalizada, com código, nome, sigla, localização, categoria e proveniência.

Um segundo job valida exclusivamente a Silver contra essas tabelas: colunas documentadas, datasets não vazios, domínios enumerados, intervalos numéricos e vetores de respostas. Para `NT_GER`, registra volume e percentual de nulos e falha em caso de conversão inválida, tipo diferente de decimal ou nota fora de 0 a 100.

### 3. Modelagem - Gold

Um AWS Glue Job (PySpark) lê os agregados Silver e cria:

- dimensões de curso, IES, área e modalidade; a dimensão de IES é enriquecida pela referência pública e-MEC e a dimensão de área usa os rótulos de `CO_GRUPO` do dicionário oficial;
- fato de desempenho, com uma linha por `CO_CURSO`;
- quantidade total de registros, notas válidas, notas nulas, soma e média de `NT_GER`.

Os dados Gold serão registrados no Glue Data Catalog e consultados pelo Athena.

## Regra essencial de LGPD

Os 32 arquivos do ENADE foram embaralhados por variáveis diferentes. Portanto, **não é permitido unir arquivos no nível de estudante**, nem por posição de linha.

O único relacionamento permitido é por `CO_CURSO`, depois de cada arquivo ser agregado independentemente no nível de curso. Essa é a principal regra do pipeline.

## Qualidade de dados

Cada Glue Job executa as validações da sua própria camada. Falhas críticas impedem a publicação da próxima camada.

- arquivos e colunas esperadas existem;
- `NT_GER` é convertida corretamente e está entre 0 e 100 quando preenchida;
- notas válidas + nulas correspondem ao total de registros;
- um curso não possui atributos conflitantes;
- a fato Gold possui uma única linha por `CO_CURSO`;
- todas as chaves da fato encontram suas dimensões.

## Consultas e dashboard

As três respostas serão escritas em SQL, executadas no Athena e usadas diretamente no dashboard.

- **Q1:** cursos distintos da Unifor, por área e modalidade;
- **Q2:** média ponderada por estudante com nota válida: `SUM(soma_nt_ger) / SUM(qtde_notas_validas)`;
- **Q3:** Top 10 cursos da Unifor por média de `NT_GER`.

O dashboard será feito no **Amazon QuickSight**, escolhido por sua integração nativa com Athena. Ele exibirá as respostas, quantidade de notas válidas, filtros simples e uma nota metodológica sobre a regra de agregação.

## Identificação da Unifor

O ENADE informa somente o código numérico da IES (`CO_IES`), sem o nome da instituição. A referência pública [Cadastro e-MEC](https://emec.mec.gov.br/) é preservada na Bronze, normalizada na Silver e usada para enriquecer `gold/dim_ies`.

A dimensão final inclui `CO_IES`, nome, sigla, município, UF e categoria administrativa. A proveniência da referência permanece na Silver para auditoria. Códigos de IES sem correspondência na referência atual são registrados como alerta; a qualidade Gold falha se a Unifor (`CO_IES=555`) não for enriquecida. Assim, a Unifor é identificada sem suposição manual.

## Entrega incremental - análises opcionais

As análises abaixo serão desenvolvidas somente após as três perguntas obrigatórias, os testes e o dashboard estarem concluídos.

### Benchmark de IES

Para cada área (`CO_GRUPO`) em que a Unifor atua, será identificado o melhor desempenho de IES do Brasil. O dashboard exibirá a média da IES líder, a média da Unifor e a diferença entre ambas, sempre usando notas válidas e a mesma regra de ponderação da Q2.

### Perfil socioeconômico e percepção

Serão explorados `QE_I08` (faixa de renda) e, se houver tempo, as respostas de percepção do curso no `arq4`. Cada arquivo adicional será agregado separadamente por `CO_CURSO` antes de ser relacionado à média de nota do curso.

A análise será descritiva e no nível de curso: ela não associa resposta e nota de um mesmo estudante, não sugere causalidade e respeita a restrição de LGPD dos microdados.

## Execução local

Os mesmos scripts Python serão empacotados em Docker. O Docker Compose permitirá validar a pipeline localmente com os dados extraídos do INEP antes da publicação em AWS. A configuração e os comandos exatos serão adicionados junto da implementação.

## Limitações

- O ENADE 2023 contempla apenas as áreas avaliadas naquela edição.
- Notas ausentes não serão imputadas; serão excluídas das médias e reportadas.
- Os microdados não permitem análises que combinem dados individuais de arquivos diferentes.