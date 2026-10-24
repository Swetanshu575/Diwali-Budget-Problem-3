# Bharat Bazaar - Problem 3: Diwali Week Goes Wrong (System Design & Day-9 Post-Mortem)

This repo/doc covers the production rollout design for Bharat Bazaar (1cr docs, 10cr embeddings, Rs 40L/month cap) and our incident response when things go wrong on Day 9.

There's no application code for Problem 3. I've included generate_problem3_pdf.py just in case you want to compile this straight into a 4 page PDF report for submission.

---

## Part A: Production Design Under Constraints

### 1. Where Queries Go (And Why "Refund Rate Last Quarter" Stays Out of RAG)

When people ask questions, not all of them work well with Retrieval-Augmented Generation, or RAG. Some queries, like "What is the refund rate for last quarter?", need exact numbers from structured data. RAG works best with unstructured text, such as articles, manuals, or documents. If you try to use RAG for questions about specific metrics or recent stats, it may not give you a clear answer. That's why queries like "refund rate last quarter" usually stay out of RAG. They need a direct pull from databases or reports, not a summary from written content.

If you get asked "What was our refund rate in Maharashtra last quarter?" and you just hand the RAG pipeline the answer, you're in trouble. The vector search will pull back text chunks that *look* like an answer. If an analyst has been going around emailing out a Metabase PDF for the last two weeks, that PDF has a number in it that's static. It doesn't know about the 4,000 COD parcels that were rejected at the doorstep of Tier 3 towns and only made it to Tally/Zoho ten days later. The embedding model can't count the rows or do the sums across the 28 lakh orders we have per month.

So we split the traffic right at the front door using a lightweight **Intent & Metric Router**:

```text

[User on 4G / WhatsApp UI] (English, Hindi, Hinglish, Tamil, Malayalam, Bengali)

|

v

[Auth & Redis Entitlement Lookup] ---> Fetches user's active security_group_ids (5-min TTL)

|

v

[Intent & Metric Router]

|

1. Pure Number or KPI Question ("refund rate last quarter", "Bhiwandi COD count")

| Goes straight to the Governed SQL layer (ClickHouse). No vector search.

| Uses pre-approved SQL templates which have been signed off by Finance.

|

+---> 2. Policy / SOP / Qualitative Ticket Question ("replacement policy", "why is Guwahati delayed")

| Goes to Hybrid Search (BGE-M3 Dense + BM25 Sparse) with Roaring Bitmap ACL pre-filters.

|

+---> 3. "Why did a metric move?" (Multi-hop like Farah's war-room query)

Fires both in parallel: ClickHouse gets the real numbers, RAG pulls the Freshdesk/warehouse

tickets to explain what actually happened on the ground.

|

v

[Deterministic Code Verifier + Freshness Watermark Stamper] ---> Streams back over SSE / WhatsApp

```

**Ingestion & Indexing Path:**

We have two completely different speed lanes. The transactional data (SAP orders, Zoho Books refunds, Tally ledgers) flows via Debezium CDC into Kafka, gets run through a PII masker (removes customer phone numbers, PAN, UPI IDs as per RBI and DPDP rules) and is then into ClickHouse (ap-south-1 Mumbai) in a matter of minutes. The unstructured stuff (Freshdesk tickets, seller PDFs, Google Sheets, exported WhatsApp chats, emails) is processed by an extractor which grabs the source permissions, chunks the text (~400 tokens), embeds it with BGE-M3 (which is a single vector space that can handle English, Hindi, Tamil, Malayalam, Bengali and Romanised Hinglish) and writes to our sharded vector + BM25 index.

**Patchy 4G & Latency (p95 <= 3s first token, <= 20s full answer):**

Warehouse supervisors in Hosur or Guwahati aren't sitting on fibre internet. They're on very spotty 4G via a WhatsApp style interface. We do TLS termination at the Mumbai/Hyderabad edge POPs, stream plain text chunks straight out of the router + SQL/retrieval (typically ~1.6s to first token) and limit the warehouse responses to 120-150 words so that the slow connections don't drop in the middle of a response.

---

### 2. Back of the envelope sizing (10 crore embeddings, Rs 40L budget)

Let's put some real numbers down and be absolutely clear about every assumption.

**Assumptions:**

* 1 crore (10M) documents give about 10 crore (100M) chunks, with around 10 chunks for each document.

* Embedding model: BGE-M3 at D = 1024 dimensions. (If we truncate via Matryoshka to D = 768 then multiply the vector RAM numbers below by 0.75).

* Index type: HNSW with M=16 links per node. Graph links plus chunk metadata (doc_id, timestamps, ACL bitmap pointer) comes in at around 214 bytes per vector which is 21.4GB across 100M vectors.

We add a 20% RAM safety buffer on top for OS page cache, working memory during queries and segment merges.

**Memory & Disk Math across Precision Levels (for 100,000,000 vectors at 1024d):**

1. **Unquantized `float32` (4 bytes/dim):**

* Raw vectors: 100M * 1024 * 4 bytes = 409.6 GB

* Add HNSW graph and metadata (`21.4 GB`), total is `431.0 GB`

* With 20% headroom: ~517.2GB RAM per full index copy. (At 768d: ~394GB RAM).

* Disk required (vectors + raw compressed text + BM25 inverted index): ~530GB NVMe.

2. Half precision float16 (2 bytes/dim):

Raw vectors: 204.8GB + 21.4GB graph = 226.2GB

* With 20% headroom: ~271.4GB RAM.

3. Scalar Quantization `SQ8 / int8` (1 byte/dim) — what we actually run:

* Raw vectors in RAM: 100M * 1024 * 1 byte = 102.4 GB.

* Add HNSW graph and metadata (`21.4 GB`). Total is `123.8 GB`.

* With 20% headroom: ~148.6GB RAM per full copy (or ~118GB if 768d)

We keep the full float32 vectors on our local NVMe SSD (total ~630GB disk) so that we can search the int8 graph in RAM, get the top 100 candidates and rescore those 100 from SSD in under 2ms. You save 70% of your RAM bill and lose less than 0.4% recall.

4. Binary / Product Quantisation `PQ64` (128 bytes/vec):

Raw vectors: 12.8GB + 21.4GB graph -> ~41GB RAM with headroom. Cheap, but recall on noisy Hinglish and Tamil transliterated queries suffers quite a bit unless you oversample heavily. Not worth the bother at all when SQ8 fits in our budget so easily.

**Query Rate (QPS) from 1,000 Normal / 5,000 Sale-Day Concurrent Users:**

A human using an assistant won't be firing 10 requests a second. They'll type a question, read the answer and then ask a follow-up question roughly once every 60 seconds.

* Normal days (1,000 concurrent users): 1,000 / 60s = 16.7 QPS average, with morning war-room spikes at 2.5x to hit around 42 QPS.

* Festival sale days (5,000 concurrent users): 5,000/60s = 83.3 average QPS, with peak bursts of around 210 QPS.

* Total monthly query volume (on the basis of 25 normal days + 5 festive peak days, ~12 active hours/day): around 2.5 to 3.6 crore queries per month.

**Shards & Replicas:**

We split the 100M vectors into 4 shards (2.5cr vectors per shard, ~37GB RAM per shard). Each shard is running on an AWS Mumbai r6gd.2xlarge (64GB RAM, 8 vCPU, 474GB local NVMe).

* Normal days: 2 replicas per shard across two AZs = 8 nodes total (more than easily handles the ~160 QPS).

* Festival sale days: auto-scaling will spin up 2 extra read replicas per shard = 16 nodes total (handles >320 QPS).

**Where the Money Goes (Monthly Budget <= Rs 40 Lakh):**

| Line Item (All in AWS Mumbai `ap-south-1`) | What We Run | Monthly Cost (INR) |

| :--- | :--- | :--- |

| Vector + BM25 Cluster | 8x r6gd.2xlarge base nodes + 8x festival burst nodes for 7 days + S3 backups | Rs 5,45,000 |

| Self Hosted GPU Tier (vLLM + Embed + Rerank) | 4x g5.2xlarge (A10G GPUs) running BGE-M3, reranker and Llama-3.1-8B / Qwen-2.5-14B (serves 85% of the traffic plus daily doc ingestion) + 4 burst GPUs on sale days | Rs 7,80,000 |

| **Commercial Frontier LLM API (15% hard queries)** | About 3.6 million complex multi-hop queries sent to Gemini 2.5 Flash or Pro in the India region. | Rs 6,90,000 |

| **ClickHouse OLAP, Kafka CDC, Redis** | 3 `m6i.2xlarge` ClickHouse, 3 `t3.xlarge` Kafka/Debezium, Redis ACL cache | Rs 4,20,000 |

| **Storage, Networking & Telemetry** | 15 TB S3, load balancers, NAT gateway, WhatsApp Business API gateway, logs | Rs 3,90,000 |

| Total Monthly Burn | Leaves Rs 11.75L buffer under the Rs 40L ceiling | Rs 28,25,000 |

---

### 3. Permissions, hidden leak channels and the 15 minute rule

We saw in Problem 1 what happens with naive post-filtering: if a Guwahati warehouse supervisor is only permitted to see 0.1% of the company's documents then retrieving the global top 100 chunks and post-filtering will leave them with nothing. On the other hand, if you go the other way and call the SAP, Zoho and Google Drive APIs live for every candidate chunk as you go during the query then you're going to completely exceed our 3 second latency target and get rate limited on your IP.

We enforce access control via Pre-Filtering with In-Memory Roaring Bitmaps in both the vector index and the BM25 inverted index. Each chunk is tagged at ingestion with integer IDs indicating which security groups are allowed to access it (dept:ops, zone:guwahati, role:wh_supervisor). When a user comes to the bot to query, we pull their group list out of Redis and pass the combined bitmap mask straight into the index traversal. If the bit for a chunk is 0 then we don't even compute a distance.

**The 5 Leak Channels People Usually Forget (Beyond the Chunk):**

1. Semantic / Response Caches: If the HR VP asks about the Gurugram hub restructure and we cache that answer on the basis of query similarity then a supervisor asking about the Gurugram delay could end up hitting that cached response. *Fix:* Every cache key must contain HMAC(sorted(user_security_groups)).
