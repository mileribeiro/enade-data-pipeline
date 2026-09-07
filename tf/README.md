# Infraestrutura Terraform

Este diretório cria um único bucket S3 e o AWS Glue Job responsável pela camada Bronze.

```text
s3://<bucket>/
├── <ano>/
│   ├── bronze/     # usado pelo job criado nesta etapa
│   ├── silver/     # reservado para a próxima etapa
│   └── gold/       # reservado para a próxima etapa
└── scripts/
    ├── bronze_ingestion.py
    └── bronze_quality.py
```

O Glue usa `s3://<bucket>/_temporary/` apenas para arquivos técnicos temporários. Esse prefixo fica fora das camadas de dados e é removido automaticamente após sete dias.

Envie manualmente o ZIP oficial antes de executar o job, no caminho `s3://<bucket>/<ano>/bronze/archive/microdados_enade_<ano>.zip`. O job exige somente o parametro `--YEAR`; ele le esse ZIP, valida sua estrutura e grava os TXT originais diretamente em `<ano>/bronze/`, usando somente o nome original de cada arquivo. O XLSX da pasta Leia-me e mantido sem alteracoes como `<ano>/bronze/dictionary.xlsx`. Inicie o workflow com a propriedade `YEAR` para que os dois jobs usem o mesmo ano; por exemplo, `aws glue start-workflow-run --name <projeto>-pipeline --run-properties '{"YEAR":"2023"}'`. O workflow do Glue executa a ingestao e, somente quando ela conclui com sucesso, executa o job de qualidade. O job de qualidade le o dicionario, valida cada cabecalho TXT e grava `<ano>/bronze/quality.json`. A ausencia de `microdados2023_arq33.txt` e a divergencia de schema de `microdados2023_arq3` no material oficial de 2023 sao registradas como alertas explicativos; os demais desvios sao criticos. A role nao possui acesso a Silver ou Gold.

## Como provisionar

1. Configure credenciais AWS com permissão para criar S3, IAM e Glue.
2. Copie o arquivo de exemplo e escolha um nome de bucket globalmente único.

```bash
cd tf
cp terraform.tfvars.example terraform.tfvars
```

3. Edite `terraform.tfvars`, então execute:

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan
terraform apply
```

O `terraform apply` cria recursos na conta AWS configurada. Não execute-o até conferir o plano.
