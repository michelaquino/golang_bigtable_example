# Golang Bigtable Example
Exemplo de integração com Bigtable usando Go

## Dependências
* Golang 1.22+
* GNU Make
* [GCloud CLI](https://cloud.google.com/sdk/gcloud)

## Como usar 
1. Inicie o emulador do Bigtable:

`$ gcloud beta emulators bigtable start`

2. Exporte as variáveis de ambiente

`$ export BIGTABLE_EMULATOR_HOST="localhost:8086" BIGTABLE_PROJECT_ID="local" BIGTABLE_INSTANCE_ID="local-instance"`

3. Configure o banco

`$ setup-db`

4. Rode os exemplos
```
$ make insert-one
$ make insert-conditional
$ make insert-batch
$ make read-one
$ make read-multiple
$ make read-partialKey
$ make delete
```

## CBT 
CBT é a interface em linha de comando para fazer algumas operações no Bigtable

https://cloud.google.com/bigtable/docs/cbt-overview

## Comandos úteis
* Listar tabelas: 
```
$ cbt -project local -instance local-instance -admin-endpoint localhost:8086 -data-endpoint localhost:8086 ls
```

* Ler uma tabela específica
```
$ cbt -project local -instance local-instance -admin-endpoint localhost:8086 -data-endpoint localhost:8086 read media_progress
```