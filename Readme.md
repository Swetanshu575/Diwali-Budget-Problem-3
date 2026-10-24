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
2. Pre-computed summaries: if a background job is summarising all the "Gurugram Hub" docs into a single digest chunk it could inadvertently take a restricted HR lay-off doc and put it into an operational summary. *Fix:* the summary chunks must have the intersection (i.e. the strictest) of the permissions of their source documents, not the union.

3. Reranker & Debugging Logs: Plaintext chunks get sent to your cross-encoder rerankers or get dumped into your OpenTelemetry/Langfuse traces where engineers can then go and read the confidential payroll/HR data. *Fix:* Only log the doc_id and chunk_id in your traces, never log the raw text strings.

4. Conversation memory: imagine a manager is moved from Finance to Operations at 2.00 PM. If they're still talking in the same WhatsApp thread at 1.30 PM (i.e. the same conversation thread from yesterday), the Finance numbers from yesterday will still be in the chat history context window. *Fix:* re-check the doc_id permissions on all the cited history turns for every new message and strip out any turn whose source doc the user no longer has access to.

5. Embeddings of ultra-sensitive text: you can in fact leak information from M&A or HR termination lists even in the absence of the raw text via nearest neighbour probing. *Fix:* Keep the HR_CONFIDENTIAL and BOARD_ONLY documents in a physically separate index partition (ns_restricted) which is completely unmounted from the standard worker tokens.

**How Permission Changes Take Effect in <= 15 Minutes:**

We never re-embed chunks when permissions change. We split the "Who is in what group" (User -> Groups in Redis, updated via AD/HRMS webhooks every 60 seconds with a 5 minute TTL fallback) from "Which group can see which document" (Group -> Doc_ID Roaring Bitmaps in the vector shards, updated by a 5 minute delta listener on Drive/Freshdesk permissions). End to end propagation is 1-6 minutes, well within the 15 minute requirement.

---

### 4. Freshness & Delete Handling Across Messy Sources

Different systems at Bharat Bazaar move at completely different speeds:

* Orders & Refunds (<= 1 hour SLA): Debezium reads the Postgres/SAP binlog and streams into ClickHouse continuously (approx 2 minute lag).

* Freshdesk, Salesforce & Courier dashboards (<=1 hour SLA): Inbound webhooks for the instant updates, with a 15 minute sliding window polling job as a fall back if the webhook fails.

* Tally & Nightly Metabase exports (<= 24 hour SLA): go into an S3 bucket at midnight. A worker will SHA-256 hash each row/page and only put the delta in.

* Someone emailing a Google Sheet or a WhatsApp export (<= 24 hour SLA) goes into a monitored inbox (ingest@bharatbazaar), gets parsed, checked for sender RBAC and is tagged as authority = UNVERIFIED_SHEET so it can never override the SAP/ClickHouse numbers.

* When a file is deleted or a customer requests DPDP data erasure we immediately flip its bit to `0` in an in-memory Tombstone Bitmap (disappears from search results in <1 second) and then physically purge the vectors during our nightly HNSW segment compaction.

**Showing Freshness to the User:** Every single response stamps the actual sync watermark of the sources it used right at the bottom:

Data freshness: Orders/Refunds DB synced 6 mins ago (14:18 IST) | Policy docs synced 3 hrs ago. (Note: COD orders from the last 14 days may have pending courier return updates).*

---

### 5. Quality, Regressions, Monitoring, and Cost Cuts

* Before launch we build a 600 question evaluation set across all 6 languages (English, Hindi, Hinglish, Tamil, Malayalam, Bengali) against known SQL outputs and policy docs. It won't ship unless the unverified number hallucination rate is 0% and the cross role ACL leak rate is 0%.

*Catching regressions: every PR that touches a prompt, router rule, embedding model or index config has to go through an automated replay of 200 golden queries. If a Tamil query which used to go to DOC-POL-003 suddenly starts going to an old policy, or if the SQL query hash changes, the build will fail.

* In production we track four main alarms: Verifier Rejection Rate (tells us if an LLM update has started making up numbers), Source Watermark Lag (pages us if the ClickHouse CDC is more than 45 mins behind SAP), p95 first token latency on 4G and Canary ACL probes.

* Where we spend vs. where we live... Where We Cut:**

*Spend:* On ClickHouse reliability, our deterministic verifier and keeping the SQ8 vectors in RAM to keep the latency low.

* *Cut*: We don't send simple warehouse lookups off to a costly frontier model (our self hosted 8B/14B models do 85% of the work). We also skip the GPU cross-encoder reranker whenever the top-1 vector/BM25 hit is well ahead of the second hit (by >0.08) which halves our reranker GPU usage.

* What is unsafe to cache: Never cache answers across different ACL bitmaps, never cache queries which contain any personal customer data (phone, email, order_id etc) and never cache time sensitive SQL aggregations for longer than the 5 minute CDC window.

---

---

## Part B: Day 9 Crisis — Two Incidents Hit at Once

Nine days in, two bad reports land on our desk:

1. Incident 1: CFO Anand asked for Maharashtra's refund rate last quarter. He got 4.1% from a real Metabase PDF export and put that in the board pre-read. Finance says the actual figure is 6.3% as the late COD returns hadn't been posted at the time of that PDF export.

2. Incident 2: A warehouse supervisor in Hyderabad was asking about delays on a particular shipment and got a one liner which was the summary of a confidential HR restructure in Gurugram.

Then CEO Meenakshi texts: "Shall we switch it off? Do I need to let anyone outside the company know?"*

---

### 1. The First 60 Minutes (Step-by-Step + What We Tell Meenakshi)

**Do we switch the whole assistant off?**

No to a total blackout, **YES to an immediate kill-switch on Unstructured Document Search (RAG) and Caches.**

If we kill the bot during Diwali week then 5,000 warehouse and ops staff lose live SQL order/shipment tracking. Instead we can flip `ENABLE_UNSTRUCTURED_RAG = False` in under 2 minutes and flush the Redis caches. The bot remains up in **Safe SQL-Only Mode** (only answering to governed order/courier database lookups) which completely eliminates any HR document leaks and stops the bot from quoting the stale PDF exports.

**Chronological Runbook (Minutes 0 to 60):**

* Min 0–5: Flip the RAG kill switch (SQL-Only Safe Mode on) and flush all semantic/response caches.

* Min 5–15: Run a forensic trace on Incident 2's doc_id. Retrieve the actual Gurugram HR document text to determine whether it contains personal data of specific individuals (i.e. employee names, salaries, phone numbers, PAN) or merely high level facility headcount targets. Whilst doing this, go through all 9 days of the query logs to see if any other user has pulled chunks out of that HR folder.

* Min 15–30: Pull Anand's exact query trace for Incident 1, check the 4.1% Metabase PDF timestamp against the 6.3% ClickHouse ledger query and write a 3 line Board Errata for Anand. Brief Meenakshi (CISO) and Legal Counsel.

* Min 30–60: Blacklist all static BI/Metabase PDFs from answering metric questions, lock the HR_* document namespace at the gateway and rotate session tokens.

**What I Say Aloud to Meenakshi (Talking Points):**

Meenakshi first: within two minutes of the alert we'd locked the assistant into Safe SQL-Only Mode and wiped the response cache. Your warehouse teams can still look at the live order and courier tables so Diwali dispatch is absolutely not going to be impacted, but document retrieval is completely paused until we've finished our forensic check.*

>

> On Anand's 4.1% refund figure: the bot didn't make that number up. It simply read 4.1% from a Metabase PDF that had been exported prior to the late-arriving Maharashtra COD returns being posted in the ledger. That in turn pushed the final number to 6.3%. I've already sent Anand a clean 3-line reconciliation note for the board packet, and we've blocked static PDF exports from ever answering financial metric questions again.*

>

As to whether you need to tell anyone outside the company... that's down to two Indian regulations that I'm currently looking at with Legal and our CISO. One is CERT-In (6 hour reporting window for unauthorised access or a security incident). The other is India's DPDP Act 2023. If the Gurugram HR document in question contained any identifiable personal data of employees (i.e. names, salaries, a list of people being terminated etc) then we'd have a personal data breach and would be required to report that to the Data Protection Board and to the individuals concerned. If our forensic at minute 15 confirms it was just facility level restructuring notes with absolutely no personal data then DPDP external reporting isn't triggered. We'll have the exact file contents on your desk in ten minutes.*

---

### 2. Root Causes: Ranked Hypotheses, Fast Tests & Needed Logs

#### Incident 1 (4.1% Metabase PDF vs 6.3% True Finance Figure)

1. **Hypothesis 1 (Most Likely) — Router sent a KPI question to RAG instead of Governed SQL:**

* *Why:* The router saw "Maharashtra" and "last quarter" and did a hybrid document search where the titled PDF (*"Maharashtra Q3 Refund Report.pdf"*) came up as #1.

* *How to test in 5 minutes:* Check the query_router_logs for Anand's request_id to see what route was taken and what doc_ids were cited.

2. **Hypothesis 2 — COD Return-to-Origin (RTO) Settlement Lag:***How to test in 5 minutes:* Look in the retrieval_audit_log for that supervisor's request_id. Check the cache_hit = true/false and whether the leaked chunk_id was a synthetic summary chunk or a raw document chunk.
2. **Hypothesis 2 — Ingestion Connector Defaulted to Public/All-Staff on Missing ACL:**
   * Why: The Gurugram HR doc was pulled from an emailed file or a shared Drive link where the permission mapper couldn't resolve the HR group and defaulted to role:all_employees.
   * *How to test in 5 mins:* Query the vector DB metadata table for that doc_id and print out its acl_security_groups bitmap.
3. **Hypothesis 3 — BM25 Lexical Search Bypassed the Vector ACL Pre-Filter:**
   * *Why:* The dense vector search correctly applied the Roaring Bitmap filter. The keyword/BM25 leg however searched the entire index for "shipment delays" and fed an unfiltered HR chunk (which stated "shipment delays expected during Gurugram restructuring") into the reranker pool.
   * *Logs required:* retrieval_audit_log (user token groups, cache key, dense candidate IDs, BM25 candidate IDs and final reranked chunk IDs) and ingestion_acl_audit_log.

---

### 3. Containment (24 Hours), Real Fixes (6 Weeks), and What We Refuse to Ship

* **Shipped Within 24 Hours:**
  1. Hard block all PDFs, spreadsheets and email attachments from answering any numerical KPI questions (if the question is asking for a rate, a count or a rupee total then it must go to ClickHouse or else it must abstain).
  2. Enforce Fail-Closed ACLs on both the Dense and BM25 search legs. Any document with NULL permissions or tagged HR / BOARD / LEGAL will be DENY'd by default unless the user's JWT has the appropriate department claim.
  3. Turn off cross-user semantic caching and delete all pre-computed multi-document summary chunks.

* **Shipped Within 6 Weeks:**
  1. CFO signed off on the semantic metric layer: the top 50 company metrics (refund rate, GMV, COD RTO%, SLA breach%) are now locked into version controlled SQL views in ClickHouse.
  2. Automated COD cohort maturity warning: if anyone asks you about a date range where >5% of the COD orders are still in flight or unsettled the SQL layer will automatically return both the settled rate and the in flight pending COD rate so that no-one ever quotes an immature number to the board.
  3. Ingestion time PII & HR sensitivity scanner: a classifier which will quarantine any document that's just been ingested containing salary tables, headcount cuts or employee PII until the source ACL group has been verified.

* **What We REFUSE to Ship:**
  1. We are not going to "fix" the permissions by putting instructions in the LLM prompt (*"Please don't mention HR documents to warehouse supervisors"*). Prompts are not a mechanism for access control.
  2. We won't have an LLM work out percentages or averages from text documents.
  3. We won't ship "fail-open" permissions if Redis or Active Directory times out. If the auth check fails then the query fails closed.

---

### 4. Prevention: The Exact Test & Monitor That Would Have Caught Each Incident

* **To Catch Incident 1 Before Anand Did:**
  * Pre-launch CI test: 100 question "Financial KPI Routing Test" in which every question relating to refund rates, revenue or order counts must have route == GOVERNED_SQL and cited_unstructured_docs == 0.
  * *Production Guardrail* A rule within our Deterministic Verifier which will block any response containing a % or ₹ metric if the source is an unstructured PDF/Sheet in the event that a matching governed SQL metric exists.
* **To Catch Incident 2 Before the Supervisor Did:**
  * Pre-launch CI test: Role matrix penetration test which replays 50 warehouse queries using a Hyderabad_Supervisor JWT against an index seeded with Gurugram_HR files and asserts 0 HR chunks in both the vector and BM25 candidate lists.
  * *Production Monitor:* 5-Minute "Honey-Doc" Canary Probe — a background job using a low-privilege warehouse token to continually query the live index for the presence of keywords within the planted fake CANARY_HR_RESTRUCTURING documents. If any canary chunk ever returns a score > 0 it pages on-call and auto-trips the RAG kill-switch in seconds.

---

### 5. Turning Anand's "Mujhe Galat Number Nahi Chahiye" Into Honest, Measurable Promises

If you promise a CFO that the AI will "never give a wrong number" without defining what happens when the accounting data itself is late to settle, you're once again going to end up with a failure. Here is how we turn Anand's requirement into four measurable SLAs we can actually keep:

1. **0.0% Unverified Numbers (Enforced by Code Verifier):** The assistant will not output a number in a factual claim unless that number is matched by our Python verifier to either a recorded ClickHouse SQL result hash (SHA-256) or to a span in an active policy document.
2. 100% SQL-only routing for business metrics. All of our KPIs (refund rates, GMV, SLA %s etc) are answered directly from the SQL. We never pull them from PDFs, emails or spreadsheets.
3. Dual reporting on our unsettled COD cohorts. As half our orders are COD we automatically have two figures for any refund/revenue metric: Settled in Ledger (e.g. 4.1%) and Inclusive of in-flight / late COD returns (e.g. 6.3%) with the exact minute of the database sync.
4. 100% abstention when data is missing or stale. If ClickHouse CDC is more than 60 minutes behind or if the metric isn't defined in the Finance approved SQL catalog then the bot will return CANNOT_DETERMINE and will state what is missing rather than make a guess.

* Why: Half of Bharat Bazaar's orders are COD. An order shipped in late September takes 10–14 days to fail delivery, return to the hub and get booked as a refund. The Metabase PDF was exported on the 1st of October (`4.1%`), Finance's query to the ledger came after the COD returns had settled (`6.3%`).

* *How to test
