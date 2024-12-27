# 🚀 Itaú Flux Control - Lambda Scheduler

![Python](https://img.shields.io/badge/python-3.9-blue.svg) ![AWS Lambda](https://img.shields.io/badge/AWS-Lambda-orange.svg) ![Boto3](https://img.shields.io/badge/boto3-1.35.68-green.svg) ![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0.36-red.svg)

## 📖 Visão Geral

O **Itaú Flux Control** é uma Lambda responsável pelo controle de gatilhos e processamento de tabelas. A solução gerencia dependências entre tabelas, valida partições e dispara execuções de tarefas utilizando o AWS EventBridge Scheduler. 

Essa Lambda permite o gerenciamento robusto de tabelas com as seguintes funcionalidades:

- 🚀 Gatilhos automáticos para execução de tabelas dependentes.
- ⏳ Controle de debounce configurável por tabela.
- 🛠️ Integração com diferentes serviços como Glue, Step Functions, e SQS.
- 🔄 Gerenciamento de partições obrigatórias e opcionais.

---

## 📁 Estrutura do Projeto

```plaintext
src/
├── models/
├── service/
├── repository/
├── provider/
└── config/
```

- **models/**: Modelos para mapeamento ORM (e.g., Tabelas, Partições, Dependências).
- **service/**: Regras de negócio para execuções, validações e registros.
- **repository/**: Acesso ao banco de dados.
- **provider/**: Configurações de sessão e injeção de dependências.
- **config/**: Configurações gerais e constantes.

---

## ⚙️ Instalação

### Requisitos

- Python 3.9+
- Pip
- AWS CLI configurado
- LocalStack para testes locais

### Passos

1. Clone este repositório:
   ```bash
   git clone https://github.com/seu-repo/itau-flux-control.git
   cd itau-flux-control
   ```

2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

3. Execute localmente utilizando LocalStack:
   ```bash
   make run-local
   ```

---

## 🚦 Uso

### Exemplos de Uso

#### Adicionar Duas Tabelas com Dependência

**Requisição:**
```http
POST /tables HTTP/1.1
Content-Type: application/json

{
  "data": [
    {
      "name": "tbjf001_op_pdz_prep",
      "description": "Tabela de operações preparadas do PDZ",
      "requires_approval": false,
      "partitions": [
        {"name": "ano_mes_referencia", "type": "int", "is_required": true}
      ],
      "dependencies": [],
      "tasks": []
    },
    {
      "name": "tb_op_enriquecido",
      "description": "Tabela de operações enriquecidas",
      "requires_approval": true,
      "partitions": [
        {"name": "ano_mes_referencia", "type": "int", "is_required": true}
      ],
      "dependencies": [
        {"dependency_name": "tbjf001_op_pdz_prep", "is_required": true}
      ],
      "tasks": []
    }
  ],
  "user": "lrcxpnu"
}
```

#### Registrar uma Execução na Primeira Tabela

**Requisição:**
```http
POST /register_execution HTTP/1.1
Content-Type: application/json

{
  "data": [
    {
      "table_name": "tbjf001_op_pdz_prep",
      "partitions": [
        {"partition_name": "ano_mes_referencia", "value": "202305"}
      ],
      "source": "glue"
    }
  ],
  "user": "lrcxpnu"
}
```

#### Aprovar o Fluxo para Executar a Segunda Tabela

**Requisição:**
```http
POST /approve HTTP/1.1
Content-Type: application/json

{
  "approval_status_id": 123,
  "user": "lrcxpnu"
}
```