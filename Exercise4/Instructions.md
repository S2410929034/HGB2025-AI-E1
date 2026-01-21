# Exercise: Big Data Processing Architectures and Their Role in the Future of AI Systems

## Objective

The goal of this exercise is to critically analyze how modern data processing architectures—originally designed for analytical workloads and event-driven systems—interact, converge, and evolve to support current and future Artificial Intelligence (AI) workloads, including LLMs, Retrieval-Augmented Generation (RAG), and AI agents.

Students are expected to demonstrate a solid technical understanding, connect concepts across systems, and take a clear architectural position supported by reasoning.

---

## Background

Over the past decades, data-intensive systems have evolved along multiple axes:

- Analytical processing systems (data warehouses, OLAP engines)
- Indexing techniques (B+ trees, full-text indexes, LSM-based indexes)
- Materialized views and incremental view maintenance
- Stream processing systems (e.g., Spark Structured Streaming, Flink)
- Event streaming platforms (e.g., Kafka)
- Change Data Capture (CDC) pipelines

Today, these systems increasingly serve AI-driven workloads, which introduce:

- High-volume data ingestion and transformation
- Continuous updates and low-latency access
- Massive read amplification from agents and applications
- New data types (embeddings, vectors, metadata, unstructured data)

This exercise asks you to reason about how these architectures fit together, where they overlap, and where new abstractions are emerging.

---

## Tasks

### 1. Analytical Processing Foundations

Explain the role of data warehouses and analytical engines in modern data architectures.

- What distinguishes analytical processing from transactional processing?
- How do indexes, materialized views, and query optimization support analytical workloads?
- Why is incremental view maintenance critical at scale?

You should relate classical OLAP concepts to modern distributed systems.

---

### 2. Streaming, Event Processing, and CDC

Analyze how stream processing systems, event streaming platforms, and CDC pipelines complement or replace batch-oriented analytics.

Discuss:

- Differences and overlaps between stream processing and incremental analytics
- The role of CDC in keeping analytical views and downstream systems up to date
- Trade-offs between latency, consistency, and complexity

---

### 3. Implications for AI Systems

Analyze how the above systems support or constrain modern AI workloads, such as:

- Training and fine-tuning LLMs
- Retrieval-Augmented Generation (RAG)
- AI agents that continuously generate data, feedback, and queries

Address questions such as:

- Why do AI systems amplify the importance of incremental computation?
- How do high query rates, freshness requirements, and data lineage affect system design?
- What challenges arise when combining structured data, unstructured data, and embeddings?

---

### 4. Vector Databases and New Indexing Paradigms

Position vector databases and embedding indexes within the broader data ecosystem.

- How do they compare to traditional indexing and materialized views?
- Are vector stores a new class of analytical system or a specialized serving layer?
- How should they integrate with streaming, CDC, and warehouses?

Discuss limitations and open challenges (e.g., consistency, updates, hybrid queries).

---

### 5. Technical Positioning and Future Outlook

Take a clear technical position on the future:

- Will AI workloads push data systems toward unified architectures, or deeper specialization?
- Which components become more central (streaming, incremental views, serving layers)?
- What architectural principles will matter most in the next 5–10 years?

Your argument should be grounded in system design.

---

## Expected Deliverables

- **Format**: Technical report or essay  
- **Length**: ~2,000–3,500 words  
- **Style**: Clear structure, diagrams encouraged, precise terminology  
- **References**: Academic papers, Books, system documentation, or credible technical sources  

---

## Notes

This is not a catalog of tools. You are expected to reason about principles, trade-offs, and system design choices. Strong answers demonstrate synthesis, not enumeration or itemization.
