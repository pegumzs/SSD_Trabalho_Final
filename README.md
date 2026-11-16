# MVP: Sistema de Suporte à Decisão para a Segurança nas Rodovias Federais (PRF)

| | |
|---|---|
| **Instituição** | Universidade de Brasília (UnB), Departamento de Engenharia de Produção |
| **Disciplina** | Sistemas de Suporte à Decisão |
| **Professor** | André Luiz Marques Serrano |
| **Aluno** | Pedro Augusto de Menezes Filho (180114425) |
| **Plataforma** | Google Colab (Python) + Google Drive (camadas bronze, prata e ouro) |
| **Dados** | Dados Abertos da PRF: acidentes por ocorrência e por pessoa, 2024 e 2025 |

Pipeline de dados em nuvem que transforma os dados abertos de acidentes da Polícia Rodoviária Federal em um esquema estrela, mostra **onde, quando, como e com quem** acontecem os acidentes graves e propõe uma **linha de tomada de decisão**: gatilhos, um painel de trechos prioritários, um modelo de *machine learning* para priorizar chamados e dois simuladores.

> **Resultado em uma frase:** os acidentes graves não são aleatórios. Os 10% de trechos mais críticos concentram 49% deles, e esses trechos se repetem de um ano para o outro. A gravidade é maior à noite, três tipos de acidente causam 59% das mortes e um modelo de ML prioriza os chamados graves melhor que uma regra simples (54,7% contra 46,9% dos graves capturados com 30% dos chamados).

---

## 1. Objetivo

A PRF tem efetivo, viaturas e equipes de socorro limitados para cobrir as rodovias federais. O MVP apoia duas decisões:

1. **Planejamento:** em quais trechos, dias e horários concentrar a fiscalização.
2. **Atendimento:** quais chamados devem receber socorro prioritário, antes de se saber quantas vítimas graves houve.

É um problema de **alocação de recursos escassos**, típico da Engenharia de Produção.

**Hipótese:** os acidentes graves (com morto ou ferido grave) concentram-se em poucos trechos, em certos horários e em certos tipos de acidente e de vítima; por isso, uma pequena parte dos trechos cobre grande parte dos graves, e um modelo de ML consegue indicar os chamados de maior risco melhor do que uma regra simples.

### Perguntas de negócio e critérios definidos antes da análise

| Nº | Pergunta | Critério |
|---|---|---|
| P1 | **Onde** se concentram os acidentes graves? | 10% dos trechos de 10 km concentram ≥ 40% dos graves |
| P2 | **Quando** os acidentes são mais graves? | Taxa de gravidade à noite ≥ 1,2 vez a do dia |
| P3 | **Como** acontecem os acidentes que mais matam? | 3 tipos de acidente respondem por ≥ 50% das mortes |
| P4 | **Quem** são as vítimas mais expostas? | Letalidade de pedestres e motociclistas ≥ 2 vezes a média |
| P5 | O ML prevê a gravidade melhor que uma regra simples? | Com 30% dos chamados priorizados, o melhor modelo captura ≥ 5 p.p. a mais de graves que a regra |

## 2. Dados

| Item | Descrição |
|---|---|
| Fonte | [Dados Abertos da PRF](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf), também listados no Portal Brasileiro de Dados Abertos |
| Conjuntos | Acidentes agrupados por ocorrência (`datatran`) e por pessoa, todas as causas e tipos (`acidentes`), 2024 e 2025 |
| Licença | Dados abertos do governo federal (Decreto nº 8.777/2016): uso e redistribuição livres, com citação da fonte |
| Formato original | `.zip` com CSV separado por ponto e vírgula, codificação latin-1 e vírgula decimal |
| Volume | 145.685 acidentes e 1.187.225 linhas de pessoas |

**Coleta.** O notebook baixa os arquivos oficiais pelos identificadores do Google Drive publicados na página da PRF. Se o download automático falhar (o Google Drive às vezes bloqueia), ele usa uma cópia pública dos mesmos arquivos em Parquet, mantida no GitHub pelo projeto [tidyprf-dados](https://github.com/bonijoao/tidyprf-dados). A origem, a data, o volume e o formato de cada arquivo ficam em `1_bronze/registro_coleta.csv`. Testamos os dois caminhos e eles produzem os mesmos resultados.

**Ética e LGPD (Lei nº 13.709/2018).** A tabela de pessoas tem idade, sexo e naturalidade. Pelo princípio da necessidade, a idade vira faixa etária e saem o identificador da pessoa, a naturalidade, a nacionalidade, a marca e o identificador do veículo antes da carga (regra R6). Os resultados são sempre agregados. Os dados brutos não são versionados neste repositório: o notebook os baixa da fonte.

## 3. Pipeline

```
PRF (4 arquivos) → 1_bronze (brutos + registro) → 2_prata (limpeza, conciliação, R1–R12)
                 → 3_ouro (esquema estrela + catálogo) → análise (qualidade, P1–P5) → decisão
```

### Regras de negócio

| Regra | O que faz | Por quê |
|---|---|---|
| R1 | Números com vírgula decimal convertidos; BR igual a 0 e coordenada fora do Brasil viram nulo | Formato do CSV e valores impossíveis |
| R2 | Data, hora e faixa horária | Análise por horário |
| R3 | "Ignorado", "Não Informado" e "(null)" viram nulo | Não são categorias reais |
| R4 | Sol → Céu Claro; Garoa/Chuvisco → Chuva | Categorias sobrepostas |
| R5 | Uma linha por pessoa em cada acidente; linhas sem pessoa (reboques) saem da tabela de pessoas | Define o grão |
| R6 | LGPD: idade em faixa; descarte de identificadores e dados pessoais não usados | Minimização |
| R7 | Idade acima de 100 anos e ano do veículo antes de 1950 ou depois do acidente viram nulo | Valores implausíveis |
| R8 | Conciliação de mortos, feridos graves e pessoas entre as duas tabelas | Consistência |
| R9 | **Acidente grave** = pelo menos 1 morto ou 1 ferido grave | Variável de decisão |
| R10 | **Trecho** = BR + UF + segmento de 10 km | Unidade de planejamento |
| R11 | Região do IBGE, fim de semana e feriados nacionais | Enriquecimento |
| R12 | Acidente com moto, pedestre ou veículo pesado (vem da tabela de pessoas) | Une as duas tabelas |

### Modelo de dados (esquema estrela)

```
                dim_tempo (731 dias)
                     |
dim_trecho (5.977) — fato_acidente (145.685) — fato_envolvido (356.593)
```

O catálogo, com tipo, descrição, domínio, obrigatoriedade e linhagem dos 41 atributos, está em [`catalogo/catalogo_dados.csv`](catalogo/catalogo_dados.csv).

### Carga

As tabelas da camada ouro são gravadas em CSV no Google Drive e **relidas**; todas as análises usam as tabelas relidas. As linhas e a soma de mortos relidas são iguais às gravadas, e os **53 testes de domínio** derivados do catálogo passaram ([`catalogo/testes_dominio.csv`](catalogo/testes_dominio.csv)). O projeto ocupa cerca de 148 MB no Drive (menos de 1% dos 15 GB gratuitos), sem custo.

## 4. Qualidade dos dados

| Dimensão | Achado | Tratamento |
|---|---|---|
| Unicidade | 1.187.225 linhas de pessoas para 356.593 pessoas: cada pessoa se repete para cada causa e tipo de acidente | R5 |
| Consistência | 114.442 linhas sem pessoa, quase todas reboques e semirreboques | R5 |
| Consistência | Mortos (12.203) e feridos graves (40.362) batem nas duas tabelas; o total de pessoas **diverge em 21.751 acidentes, sempre por 1 pessoa e sempre em acidentes com reboque**: o reboque é contado como pessoa | R8 (vale o total recontado) |
| Conformidade | 353 acidentes na "BR-0"; categorias de tempo sobrepostas (Sol e Céu Claro) | R1, R4 |
| Acurácia | Todas as 73.156 coordenadas de 2024 fora do Brasil (sem separador decimal); 1.487 idades acima de 100 anos (máximo 2024); 61.164 anos de veículo antes de 1950 (a maioria zero) | R1, R7 |
| Completude | Estado físico vazio em 15,6% das linhas de pessoas e condição do tempo "Ignorado" em 1,3% dos acidentes | R3 |
| Atualidade | De 01/01/2024 a 31/12/2025 | — |

Relatórios: [`catalogo/relatorio_qualidade.csv`](catalogo/relatorio_qualidade.csv), [`catalogo/relatorio_completude.csv`](catalogo/relatorio_completude.csv) e [`catalogo/conciliacao.csv`](catalogo/conciliacao.csv).

## 5. Resultados

**Panorama:** 145.685 acidentes, 41.118 graves (28,2%) e 12.203 mortos (6.160 em 2024 e 6.043 em 2025).

**P1, onde.** Dos 5.977 trechos de 10 km com acidentes, os 10% mais críticos concentram **49,3%** dos graves. Os trechos críticos se repetem: a correlação entre os graves de cada trecho em 2024 e em 2025 é de 0,90. Escolhendo os trechos com os dados de 2024, 5% da malha teria coberto **33,4%** dos graves de 2025 (validação fora do tempo).

**P2, quando.** A taxa de gravidade é de 32,3% em plena noite, contra 25,4% em pleno dia (**1,27 vez**). O volume tem pico às 18h, mas a gravidade fica acima da média entre 18h e 5h. A taxa sobe de 27,0% na segunda-feira para 30,4% no domingo.

**P3, como.** Colisão frontal, atropelamento de pedestre e saída de pista são 25,3% dos acidentes e **58,6% das mortes**. A colisão frontal sozinha tem 31,5% das mortes. A pista simples tem gravidade de 33,7%, contra 23,4% na dupla.

**P4, quem.** A letalidade média é de 3,66 óbitos por 100 envolvidos. Entre os pedestres, chega a 27,8 (**7,6 vezes**) e, entre os ciclistas, a 11,6. Os motociclistas têm letalidade de 4,9 (**1,3 vez**, abaixo do critério), mas respondem por 33,1% dos óbitos: o peso deles está no volume, não no risco individual.

**P5, machine learning.** Treino com 2024 e teste com 2025, usando só o que se sabe no momento do chamado (55 variáveis; a causa fica de fora, por ser apurada depois).

| Modelo | AUC | Graves capturados com 30% dos chamados |
|---|---:|---:|
| Regra (taxa do tipo de acidente em 2024) | 0,655 | 46,9% |
| Regressão logística | 0,734 | 54,0% |
| Árvore de decisão | 0,709 | 51,5% |
| Random forest | 0,741 | 54,3% |
| **Gradient boosting** | **0,742** | **54,7%** |

### Verificação da hipótese

| P | Critério | Resultado | Atendido? |
|---|---|---|---|
| P1 | 10% dos trechos ≥ 40% dos graves | 49,3% | Sim |
| P2 | Noite ≥ 1,2 × dia | 1,27 × | Sim |
| P3 | 3 tipos ≥ 50% das mortes | 58,6% | Sim |
| P4 | Pedestres e motociclistas ≥ 2 × a média | 7,6 × e 1,3 × | **Não** |
| P5 | ML ≥ 5 p.p. acima da regra | +7,8 p.p. | Sim |

A hipótese foi **confirmada parcialmente** (4 de 5 critérios).

## 6. Linha de tomada de decisão

| Gatilho | Limite | Decisão | Responsável |
|---|---|---|---|
| G1 | Trecho entre os 5% com mais graves no ano anterior | D1: fiscalização fixa e radar móvel | Planejamento operacional, anual |
| G2 | Gravidade do trecho ≥ 1,5 × a nacional, com ≥ 10 acidentes | D2: vistoria de engenharia com o DNIT | Superintendência, semestral |
| G3 | ≥ 60% dos graves do trecho entre 18h e 6h (média: 46%) | D3: operação noturna | Delegacia, mensal |
| G4 | Chamado entre os 30% de maior risco previsto | D4: socorro prioritário | Central de atendimento, tempo real |

**Painel** ([`evidencias/painel_trechos.csv`](evidencias/painel_trechos.csv)): 300 trechos de prioridade alta (G1), que cobriram 33,5% dos graves de 2025; 983 de prioridade média (G2 ou G3); 4.694 em monitoramento. Os trechos mais críticos ficam na BR-101 (SC, PE, PB e ES), na BR-230/PB, na BR-381/MG e na BR-116/SP.

**Simuladores.** Fiscalização: 1% dos trechos cobriria 13,6% dos graves, e 20%, 62,4%. Atendimento: priorizar 20% dos chamados captura 41,5% dos graves, e 50%, 74,0%.

## 7. Autoavaliação

**Quais perguntas foram respondidas?** Todas as cinco. Quatro critérios foram atendidos. O da P4 não: esperava-se que os motociclistas tivessem letalidade de pelo menos o dobro da média, mas ela é de 1,3 vez. O resultado mudou a leitura do problema: o risco individual mais alto é o dos pedestres e ciclistas, e o dos motociclistas está no volume de vítimas.

**Que limitações dos dados condicionaram os resultados?** O período é curto (dois anos). Não há volume de tráfego, então não é possível calcular risco por veículo-quilômetro: um trecho com muitos acidentes pode só ter muito tráfego. As coordenadas de 2024 vieram corrompidas. Faltam variáveis decisivas para a gravidade, como velocidade, cinto, capacete e tempo de chegada do socorro, o que limita o desempenho do ML (AUC de 0,74). O estado físico é o registrado pela PRF no local e pode não captar mortes posteriores.

**O que eu faria de outra forma?** Definiria os trechos pelo volume de tráfego do DNIT, para comparar taxas e não só contagens; usaria uma validação temporal mês a mês, e não só um ano de teste; e começaria a análise pela tabela de pessoas, que revelou os problemas de grão e de contagem mais cedo do que eu esperava.

**O que falta para virar uma solução de uso contínuo?** Rodar o pipeline mensalmente em uma plataforma como o Databricks, com cargas incrementais; publicar o painel de trechos em uma ferramenta de visualização; integrar o modelo de risco à central de atendimento; e medir o efeito das decisões comparando trechos fiscalizados com trechos semelhantes não fiscalizados.

## 8. Como reproduzir

1. Abra [`notebooks/MVP_PRF_Acidentes.ipynb`](notebooks/MVP_PRF_Acidentes.ipynb) no Google Colab.
2. Execute todas as células (*Ambiente de execução → Executar tudo*) e autorize o acesso ao Google Drive.
3. As camadas são criadas em `Meu Drive/MVP_PRF_Acidentes/` (`1_bronze`, `2_prata`, `3_ouro`, `catalogo`, `evidencias`). A execução leva de 5 a 10 minutos.

Fora do Colab, o notebook usa uma pasta local com a mesma estrutura (`pip install -r requirements.txt`).

## 9. Estrutura do repositório

| Caminho | Conteúdo |
|---|---|
| `README.md` | Este documento |
| `notebooks/` | Notebook do pipeline completo (coleta, transformação, carga, análise e decisão) |
| `scripts/` | `figuras_artigo.py`: gera as figuras do artigo a partir de `evidencias/` |
| `catalogo/` | Catálogo de dados, testes de domínio, relatórios de qualidade e conciliação |
| `evidencias/` | Figuras, tabelas de resultados, painel e capturas de tela da execução no Colab e no Drive |
| `artigo/` | Short paper (PDF e LaTeX) |
| `LICENSE` | Licença do código (MIT) e registro da licença dos dados |
