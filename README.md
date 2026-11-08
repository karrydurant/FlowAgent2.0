# FlowAgent2.0

<p>
  <img alt="Java" src="https://img.shields.io/badge/Java-17-orange?logo=openjdk&logoColor=white">
  <img alt="Spring Boot" src="https://img.shields.io/badge/Spring%20Boot-3.2.5-6DB33F?logo=springboot&logoColor=white">
  <img alt="Spring AI" src="https://img.shields.io/badge/Spring%20AI-1.0.0-6DB33F?logo=spring&logoColor=white">
  <img alt="Maven" src="https://img.shields.io/badge/Maven-3.9-C71A36?logo=apachemaven&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white">
</p>
<p>
  <img alt="Redis" src="https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white">
  <img alt="Neo4j" src="https://img.shields.io/badge/Neo4j-5-008CC1?logo=neo4j&logoColor=white">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-16%20+%20pgvector-336791?logo=postgresql&logoColor=white">
  <img alt="MySQL" src="https://img.shields.io/badge/MySQL-8-4479A1?logo=mysql&logoColor=white">
</p>

这是一个用 Java 和 Spring AI 写的多 Agent 工作流编排平台，把多个 Agent "怎么串成流程，怎么'协商'、互相委派" 做成了一套基础设施。

协商：这里的协商是 "召集者把同一个议题分发给每个成员，各成员生成结构化表态后再按模式算结论"，不是"多个 Agent 间互相聊天、辩论" 

互相委派：通过将一个 Agent 注册为工具的方式使得其它 Agent 能委派该 Agent

## [工作流执行引擎](docs/workflow-engine.md)

## [多 Agent 协商引擎](docs/a2a-engine.md)
