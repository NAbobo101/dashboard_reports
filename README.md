# Relatórios Stellar Beauty — MySQL + Streamlit

Este projeto implementa uma arquitetura simples e prática para **armazenar dados em MySQL** e **consumir/visualizar esses dados via Streamlit**, com suporte a relatórios e gráficos.

A ideia central é:
- **MySQL** = fonte única de verdade (armazenamento e organização dos dados)
- **Streamlit** = interface para explorar tabelas, validar cargas e evoluir para relatórios e dashboards

---

## Arquitetura

### Visão geral

```text
+---------------------------+                 +----------------------+
|        Fontes de Dados    |                 |      Streamlit       |
|---------------------------|                 |----------------------|
| WordPress / WooCommerce   |   dados no      | - Navegar tabelas    |
| ActiveCampaign (CSV)      |   MySQL         | - Preview paginado   |
| Outras fontes futuras     +---------------->| - Relatórios/gráficos|
+-------------+-------------+                 +----------+-----------+
              |                                          |
              | ETL / Importações                         |
              v                                          v
        +-----+--------------------------------------------+-----+
        |                       MySQL                            |
        |--------------------------------------------------------|
        | staging          core          wordpress  active_campaign|
        | (bruto)          (modelo)      (limpo)    (limpo)       |
        +--------------------------------------------------------+
