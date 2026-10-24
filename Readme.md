# Bharat Bazaar - Problem 3: Diwali Week Goes Wrong (System Design & Day-9 Post-Mortem)

This repo / doc covers the production rollout design for Bharat Bazaar (1 crore docs, 10 crore embeddings, Rs 40L/month cap) and our incident response when things break on Day 9. 

There is no application code required for Problem 3, though I included `generate_problem3_pdf.py` in case you want to compile this directly into a 4-page PDF report for submission.

---

## Part A: Production Design Under Constraints

### 1. Where Queries Go (And Why "Refund Rate Last Quarter" Stays Out of RAG)

If someone asks "What was our refund rate in Maharashtra last quarter?", letting a RAG pipeline answer that is asking for trouble. Vector search retrieves text chunks that *sound* like an answer. If an analyst exported a Metabase PDF two weeks ago and emailed it around, that PDF has a number frozen in time. It doesn't know about the 4,000 Cash-on-Delivery (COD) parcels that got rejected at customer doorsteps in Tier-3 towns and only hit Tally/Zoho ten days later. Plus, embedding models cannot count rows or divide sums across 28 lakh monthly orders.

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
   +---> 1. Pure Number / KPI Question ("refund rate last quarter", "Bhiwandi COD count")
   |        Goes straight to the Governed SQL Layer (ClickHouse). Zero vector search.
   |        Uses pre-approved SQL templates signed off by Finance.
   |
   +---> 2. Policy / SOP / Qualitative Ticket Question ("replacement policy", "why is Guwahati delayed")
   |        Goes to Hybrid Search (BGE-M3 Dense + BM25 Sparse) with Roaring Bitmap ACL pre-filters.
   |
   +---> 3. "Why did a metric move?" (Multi-hop like Farah's war-room query)
            Fires both in parallel: ClickHouse gets the real numbers, RAG pulls the Freshdesk/warehouse
            tickets to explain what happened on the ground.
        |
        v
[Deterministic Code Verifier + Freshness Watermark Stamper] ---> Streams back over SSE / WhatsApp
```

**Ingestion & Indexing Path:**
We have two totally different speed lanes. Transactional data (SAP orders, Zoho Books refunds, Tally ledgers) streams through Debezium CDC into Kafka, passes a PII masker (stripping customer phone numbers, PAN, UPI IDs per RBI and DPDP rules), and lands in ClickHouse (`ap-south-1` Mumbai) within a few minutes. Unstructured stuff—Freshdesk tickets, seller PDFs, Google Sheets, exported WhatsApp chats, emails—goes through an extraction worker that pulls out source permissions, chunks the text (~400 tokens), embeds it with `BGE-M3` (which handles English, Hindi, Tamil, Malayalam, Bengali, and Romanized Hinglish in one vector space), and writes to our sharded vector + BM25 index.

**Patchy 4G & Latency (p95 <= 3s first token, <= 20s full answer):**
Warehouse supervisors in Hosur or Guwahati aren't sitting on fiber internet. They're on spotty 4G using a WhatsApp-style interface. We keep TLS termination at Mumbai/Hyderabad edge POPs, stream plain text chunks immediately after the router + SQL/retrieval finishes (usually ~1.6s to first token), and cap warehouse answers at 120–150 words so slow connections don't drop mid-stream.

---

### 2. Back-of-the-Envelope Sizing (10 Crore Embeddings & Rs 40L Budget)

Let's put actual numbers down and state every assumption clearly.

**Assumptions:**
* 1 crore (10M) documents -> 10 crore (100M) chunks at ~10 chunks per doc.
* Embedding model: `BGE-M3` at **D = 1024** dimensions. (If we truncate via Matryoshka to D = 768, multiply the vector RAM numbers below by 0.75).
* Index type: HNSW with `M = 16` links per node. Graph links + chunk metadata (`doc_id`, timestamps, ACL bitmap pointer) add roughly `214 bytes` per vector, which is **21.4 GB** across 100M vectors.
* We add a **20% RAM safety buffer** on top for OS page cache, working memory during queries, and segment merges.

**Memory & Disk Math across Precision Levels (for 100,000,000 vectors at 1024d):**

1. **Unquantized `float32` (4 bytes/dim):**
   * Raw vectors: `100M * 1024 * 4 bytes` = `409.6 GB`
   * Add HNSW graph + metadata (`21.4 GB`) = `431.0 GB`
   * With 20% headroom: **~517.2 GB RAM** per full index copy. (At 768d: **~394 GB RAM**).
   * Disk needed (vectors + raw compressed text + BM25 inverted index): **~530 GB NVMe**.

2. **Half-precision `float16` (2 bytes/dim):**
   * Raw vectors: `204.8 GB` + `21.4 GB` graph = `226.2 GB`.
   * With 20% headroom: **~271.4 GB RAM**.

3. **Scalar Quantization `SQ8 / int8` (1 byte/dim) — What we actually run:**
   * Raw vectors in RAM: `100M * 1024 * 1 byte` = `102.4 GB`.
   * Add HNSW graph + metadata (`21.4 GB`) = `123.8 GB`.
   * With 20% headroom: **~148.6 GB RAM** per full copy (or **~118 GB** if 768d).
   * We keep the full `float32` vectors on local NVMe SSD (**~630 GB disk** total) so we can search the `int8` graph in RAM, grab the top 100 candidates, and rescore those 100 from SSD in under 2ms. You save 70% of your RAM bill and lose less than 0.4% recall.

4. **Binary / Product Quantization `PQ64` (128 bytes/vec):**
   * Raw vectors: `12.8 GB` + `21.4 GB` graph -> **~41 GB RAM** with headroom. Cheap, but recall on noisy Hinglish and Tamil transliterated queries drops noticeably unless you oversample heavily. Not worth the headache when `SQ8` easily fits our budget.

**Query Rate (QPS) from 1,000 Normal / 5,000 Sale-Day Concurrent Users:**
A human using an assistant doesn't fire 10 requests a second; they type a question, read the answer, and ask a follow-up roughly once every **60 seconds**.
* Normal days (1,000 concurrent users): `1,000 / 60s` = **16.7 QPS average**, with morning war-room bursts (2.5x) hitting **~42 QPS**.
* Festival sale days (5,000 concurrent users): `5,000 / 60s` = **83.3 QPS average**, with peak bursts hitting **~210 QPS**.
* Total monthly query volume (assuming 25 normal days + 5 festive peak days, ~12 active hours/day): roughly **2.5 to 3.6 crore queries/month**.

**Shards & Replicas:**
We split the 100M vectors into **4 shards** (2.5 crore vectors per shard, ~37 GB RAM per shard). Each shard runs on an AWS Mumbai `r6gd.2xlarge` (64 GB RAM, 8 vCPU, 474 GB local NVMe).
* Normal days: **2 replicas per shard** across two Availability Zones = **8 nodes total** (handles ~160 QPS easily).
* Festival sale days: Auto-scaling spins up **2 extra read replicas per shard** = **16 nodes total** (handles >320 QPS).

**Where the Money Goes (Monthly Budget <= Rs 40 Lakh):**

| Line Item (All in AWS Mumbai `ap-south-1`) | What We Run | Monthly Cost (INR) |
| :--- | :--- | :--- |
| **Vector + BM25 Cluster** | 8x `r6gd.2xlarge` base nodes + 8x festival burst nodes for 7 days + S3 backups | Rs 5,45,000 |
| **Self-Hosted GPU Tier (vLLM + Embed + Rerank)** | 4x `g5.2xlarge` (A10G GPUs) running `BGE-M3`, reranker, and `Llama-3.1-8B` / `Qwen-2.5-14B` (serves 85% of traffic + daily doc ingestion) + 4 burst GPUs on sale days | Rs 7,80,000 |
| **Commercial Frontier LLM API (15% hard queries)** | ~3.6M complex multi-hop queries escalated to Gemini 2.5 Flash/Pro (India region) | Rs 6,90,000 |
| **ClickHouse OLAP + Kafka CDC + Redis** | 3x `m6i.2xlarge` ClickHouse + 3x `t3.xlarge` Kafka/Debezium + Redis ACL cache | Rs 4,20,000 |
| **Storage, Networking & Telemetry** | 15 TB S3, load balancers, NAT gateway, WhatsApp Business API gateway, logs | Rs 3,90,000 |
| **Total Monthly Burn** | **Leaves Rs 11.75L buffer under the Rs 40L ceiling** | **Rs 28,25,000** |

---

### 3. Permissions, Hidden Leak Channels, and the 15-Minute Rule

We already saw in Problem 1 what happens when you do naive post-filtering: if a Guwahati warehouse supervisor is only allowed to see 0.1% of company documents, retrieving the global top-100 chunks and filtering afterward leaves them with zero results. On the flip side, calling SAP, Zoho, and Google Drive APIs live for every candidate chunk during a query will blow past our 3-second latency target and get our IP rate-limited.

So we enforce access control via **Pre-Filtering with In-Memory Roaring Bitmaps** inside both the vector index and the BM25 inverted index. Every chunk is tagged at ingestion with integer IDs for its allowed security groups (`dept:ops`, `zone:guwahati`, `role:wh_supervisor`). When a user queries the bot, we grab their group list from Redis and pass the combined bitmap mask directly into the index traversal. If a chunk's bit is 0, distance isn't even computed.

**The 5 Leak Channels People Usually Forget (Beyond the Chunk):**
1. **Semantic / Response Caches:** If the HR VP asks about Gurugram hub restructuring and we cache the answer by query similarity, a supervisor asking about Gurugram delays could hit that cached response. *Fix:* Every cache key must include `HMAC(sorted(user_security_groups))`.
2. **Pre-computed Summaries:** If a background job summarizes all "Gurugram Hub" docs into a digest chunk, it can accidentally blend a restricted HR layoff doc into an operational summary. *Fix:* Summary chunks must inherit the *intersection* (strictest) of their source documents' permissions, never the union.
3. **Reranker & Debugging Logs:** Plaintext chunks sent to cross-encoder rerankers or dumped into OpenTelemetry/Langfuse traces let engineers read confidential payroll/HR data. *Fix:* Log only `doc_id` and `chunk_id` in traces; never log raw text strings.
4. **Conversation Memory:** Suppose a manager gets moved from Finance to Operations at 2:00 PM. If they keep chatting in the same WhatsApp thread from 1:30 PM, yesterday's Finance numbers are still sitting in the chat history context window. *Fix:* Re-check the `doc_id` permissions of all cited history turns on every new message; strip any turn whose source doc the user can no longer access.
5. **Embeddings of Ultra-Sensitive Text:** Even without raw text, dense vectors of M&A or HR termination lists can leak info via nearest-neighbor probing. *Fix:* Keep `HR_CONFIDENTIAL` and `BOARD_ONLY` docs in a physically separate index partition (`ns_restricted`) that is completely unmounted for standard worker tokens.

**How Permission Changes Take Effect in <= 15 Minutes:**
We never re-embed chunks when permissions change. Instead, we separate **Who is in What Group** (`User -> Groups` in Redis, updated via Active Directory / HRMS webhooks within 60 seconds, with a hard 5-minute TTL fallback) from **Which Group Can See Which Doc** (`Group -> Doc_ID` Roaring Bitmaps in the vector shards, updated by a 5-minute delta listener on Drive/Freshdesk permissions). End-to-end propagation takes 1 to 6 minutes, well inside the 15-minute requirement.

---

### 4. Freshness & Delete Handling Across Messy Sources

Different systems at Bharat Bazaar move at completely different speeds:
* **Orders & Refunds (<= 1 hour SLA):** Debezium reads the Postgres/SAP binlog and streams into ClickHouse continuously (~2 min lag).
* **Freshdesk, Salesforce & Courier Dashboards (<= 1 hour SLA):** Inbound webhooks for instant updates, backed by a 15-minute sliding-window polling job in case a webhook drops.
* **Tally & Nightly Metabase Exports (<= 24 hour SLA):** Dropped into an S3 bucket at midnight. A worker hashes each row/page (`SHA-256`) and only embeds the delta.
* **Someone Emailing a Google Sheet or WhatsApp Export (<= 24 hour SLA):** Goes to a monitored inbox (`ingest@bharatbazaar`), gets parsed, checked for sender RBAC, and tagged as `authority = UNVERIFIED_SHEET` so it can never override SAP/ClickHouse numbers.

**Deletes:** When a file is deleted or a customer invokes DPDP data erasure, we flip its bit to `0` in an in-memory **Tombstone Bitmap** immediately (takes `<1 second` to disappear from search results), and physically purge the vectors during nightly HNSW segment compaction.

**Showing Freshness to the User:** Every single response stamps the actual sync watermark of the sources it used right at the bottom:
> *Data freshness: Orders/Refunds DB synced 6 mins ago (14:18 IST) | Policy docs synced 3 hrs ago. (Note: COD orders from the last 14 days may have pending courier return updates).*

---

### 5. Quality, Regressions, Monitoring, and Cost Cuts

* **Before Launch:** We build a 600-question evaluation set across all 6 languages (English, Hindi, Hinglish, Tamil, Malayalam, Bengali) tested against known SQL outputs and policy docs. It does not ship unless unverified number hallucinations = `0%` and cross-role ACL leaks = `0%`.
* **Catching Regressions:** Every PR that touches a prompt, router rule, embedding model, or index config has to pass an automated replay of 200 golden queries. If a Tamil query that used to hit `DOC-POL-003` suddenly drifts to an old policy, or if the SQL query hash changes, the build fails.
* **In Production:** We track four main alarms—Verifier Rejection Rate (tells us if an LLM update started making up numbers), Source Watermark Lag (pages us if ClickHouse CDC falls >45 mins behind SAP), p95 First-Token Latency on 4G, and Canary ACL Probes.
* **Where We Spend vs. Where We Cut:**
  * *Spend:* We spend on ClickHouse reliability, our deterministic verifier, and keeping `SQ8` vectors in RAM so latency stays low.
  * *Cut:* We don't send simple warehouse lookups to a pricey frontier model—our self-hosted 8B/14B models handle 85% of queries. We also skip the GPU Cross-Encoder Reranker whenever the top-1 vector/BM25 hit leads the second hit by a wide score margin (`>0.08`), which cuts reranker GPU usage in half.
  * *What is UNSAFE to cache:* Never cache answers across different ACL bitmaps, never cache queries containing personal customer data (phone/email/order_id), and never cache time-sensitive SQL aggregations longer than the 5-minute CDC window.

---
---

## Part B: Day 9 Crisis — Two Incidents Hit at Once

Nine days in, two bad reports land on our desk:
1. **Incident 1:** CFO Anand asked for Maharashtra's refund rate last quarter, got **4.1%** from a real Metabase PDF export, and put it in a board pre-read. Finance says the real number is **6.3%** because late COD returns hadn't been posted when that PDF was exported.
2. **Incident 2:** A Hyderabad warehouse supervisor asked about shipment delays and got a sentence summarizing a confidential Gurugram HR restructuring plan.

Then CEO Meenakshi texts: *"Do we switch it off? Do I need to tell anyone outside the company?"*

---

### 1. The First 60 Minutes (Step-by-Step + What We Tell Meenakshi)

**Do we switch the whole assistant off?**
No to a total blackout, **YES to an immediate kill-switch on Unstructured Document Search (RAG) and Caches.**
If we kill the whole bot during Diwali week, 5,000 warehouse and ops staff lose live SQL order/shipment tracking. Instead, within 2 minutes, we flip `ENABLE_UNSTRUCTURED_RAG = False` and flush Redis caches. The bot stays up in **Safe SQL-Only Mode** (answering governed order/courier database lookups only), which completely stops any HR document leaks and stops the bot from quoting stale PDF exports.

**Chronological Runbook (Minutes 0 to 60):**
* **Min 0–5:** Flip the RAG kill-switch (`SQL-Only Safe Mode` on) and flush all semantic/response caches.
* **Min 5–15:** Run a forensic trace on Incident 2's `doc_id`. Pull the exact Gurugram HR document text to see if it contains **personal data of specific individuals** (employee names, salaries, phone numbers, PAN) or just high-level facility headcount targets. Simultaneously scan all 9 days of query logs to see if any other unauthorized user pulled chunks from that HR folder.
* **Min 15–30:** Pull Anand's exact query trace for Incident 1, verify the 4.1% Metabase PDF timestamp against the 6.3% ClickHouse ledger query, and write a 3-line Board Errata explanation for Anand. Brief Meenakshi, CISO, and Legal Counsel.
* **Min 30–60:** Blacklist all static BI/Metabase PDFs from answering metric questions, lock the `HR_*` document namespace at the gateway, and rotate session tokens.

**What I Say Aloud to Meenakshi (Talking Points):**
> *"Meenakshi, first: within two minutes of getting the alert, I locked the assistant into Safe SQL-Only Mode and wiped the response cache. Your warehouse teams can still check live order and courier tables so Diwali dispatch keeps moving, but document retrieval is completely paused until we finish our forensic check.*
>
> *On Anand's 4.1% refund number: the bot didn't make the number up—it read 4.1% from a Metabase PDF that was exported before late-arriving Maharashtra COD returns were posted in the ledger, which pushed the final number to 6.3%. I've already sent Anand a clean 3-line reconciliation note for the board packet, and we've blocked static PDF exports from ever answering financial metric questions again.*
>
> *On whether you need to tell anyone outside the company: that comes down to two Indian regulations that I'm reviewing with Legal and our CISO right now. First is **CERT-In**, which has a 6-hour reporting window for unauthorized access or security breaches. Second is **India's DPDP Act, 2023**: if that Gurugram HR document contained identifiable personal data of employees—like names, salaries, or a termination list—then we have to report a personal data breach to the Data Protection Board and the affected individuals. If our forensic check at Minute 15 confirms it only had facility-level restructuring notes with zero personal data, then DPDP external reporting isn't triggered. We'll have the exact file contents on your desk in ten minutes."*

---

### 2. Root Causes: Ranked Hypotheses, Fast Tests & Needed Logs

#### Incident 1 (4.1% Metabase PDF vs 6.3% True Finance Figure)
1. **Hypothesis 1 (Most Likely) — Router sent a KPI question to RAG instead of Governed SQL:**
   * *Why:* The router saw "Maharashtra" and "last quarter" and ran hybrid doc search, where a titled PDF (*"Maharashtra Q3 Refund Report.pdf"*) ranked #1.
   * *How to test in 5 mins:* Check `query_router_logs` for Anand's `request_id` to see `route_taken` and `cited_doc_ids`.
2. **Hypothesis 2 — COD Return-to-Origin (RTO) Settlement Lag:**
   * *Why:* Half of Bharat Bazaar's orders are COD. An order shipped in late September takes 10–14 days to fail delivery, return to the hub, and get booked as a refund. The Metabase PDF was exported on Oct 1st (`4.1%`), whereas Finance queried the ledger after COD returns settled (`6.3%`).
   * *How to test in 5 mins:* Run the SQL query on Q3 Maharashtra orders filtering `refund_posted_date <= '2026-10-01'` vs `refund_posted_date <= '2026-10-20'` and check if the numbers match `4.1%` and `6.3%`.
3. **Hypothesis 3 — Metric Definition Split (Customer Post-Delivery Refunds vs Courier RTOs):**
   * *Why:* Ops dashboards often exclude undelivered courier RTOs from "refund rate", while Finance includes them.
   * *Logs needed:* `query_router_logs`, `tool_execution_traces` (exact SQL / doc IDs), and `document_ingestion_metadata` (file export timestamps).

#### Incident 2 (Hyderabad Supervisor Seeing Gurugram HR Plan)
1. **Hypothesis 1 (Most Likely) — Nightly Summary or Semantic Cache Bleed:**
   * *Why:* A background job generated a "Hub Delays & Staffing Summary" (or cached an earlier answer for an executive) that combined Gurugram courier delays with the Gurugram HR restructuring doc without intersecting their ACL permissions.
   * *How to test in 5 mins:* Inspect `retrieval_audit_log` for that supervisor's `request_id`—check `cache_hit = true/false` and whether the leaked `chunk_id` was a synthetic summary chunk or a raw document chunk.
2. **Hypothesis 2 — Ingestion Connector Defaulted to Public/All-Staff on Missing ACL:**
   * *Why:* The Gurugram HR doc was ingested from an emailed file or shared Drive link where the permission mapper failed to resolve the HR group and fell back to `role:all_employees`.
   * *How to test in 5 mins:* Query the vector DB metadata table for that `doc_id` and print its `acl_security_groups` bitmap.
3. **Hypothesis 3 — BM25 Lexical Search Bypassed the Vector ACL Pre-Filter:**
   * *Why:* The dense vector search applied the Roaring Bitmap filter properly, but the keyword/BM25 leg searched globally for `"shipment delays"` and merged an un-filtered HR chunk (which mentioned *"shipment delays expected during Gurugram restructuring"*) into the reranker pool.
   * *Logs needed:* `retrieval_audit_log` (recording user token groups, cache key, dense candidate IDs, BM25 candidate IDs, and final reranked chunk IDs) and `ingestion_acl_audit_log`.

---

### 3. Containment (24 Hours), Real Fixes (6 Weeks), and What We Refuse to Ship

* **Shipped Within 24 Hours:**
  1. Hard-block all PDFs, spreadsheets, and email attachments from answering numerical KPI questions—if a query asks for a rate, count, or rupee total, it must hit ClickHouse or abstain.
  2. Enforce **Fail-Closed ACLs** on both Dense and BM25 search legs: any document with `NULL` permissions or tagged `HR` / `BOARD` / `LEGAL` defaults to `DENY` unless the user's JWT explicitly carries that department claim.
  3. Turn off cross-user semantic caching and delete all pre-computed multi-document summary chunks.

* **Shipped Within 6 Weeks:**
  1. **CFO-Signed Semantic Metric Layer:** Lock the top 50 company metrics (refund rate, GMV, COD RTO%, SLA breach%) into version-controlled SQL views in ClickHouse.
  2. **Automated COD Cohort Maturity Warning:** Whenever someone queries a date range where `>5%` of COD orders are still in transit or unsettled, the SQL layer automatically returns *both* the Settled Rate and the In-Flight Pending COD Rate so nobody quotes an immature number to the board.
  3. **Ingestion-Time PII & HR Sensitivity Scanner:** A classifier that quarantines any newly ingested document containing salary tables, headcount cuts, or employee PII until its source ACL group is verified.

* **What We REFUSE to Ship:**
  1. We refuse to "fix" permissions by adding instructions to the LLM prompt (*"Please don't mention HR documents to warehouse supervisors"*). Prompts are not access control.
  2. We refuse to let an LLM calculate percentages or averages from text documents.
  3. We refuse to ship "fail-open" permissions when Redis or Active Directory times out. If the auth check fails, the query fails closed.

---

### 4. Prevention: The Exact Test & Monitor That Would Have Caught Each Incident

* **To Catch Incident 1 Before Anand Did:**
  * *Pre-launch CI Test:* A 100-question "Financial KPI Routing Test" where every query asking for refund rates, revenue, or order counts must assert `route == GOVERNED_SQL` and `cited_unstructured_docs == 0`.
  * *Production Guardrail:* A rule inside our Deterministic Verifier that blocks any response containing a `%` or `₹` metric if the source is an unstructured PDF/Sheet when a matching governed SQL metric exists.
* **To Catch Incident 2 Before the Supervisor Did:**
  * *Pre-launch CI Test:* Role-matrix penetration test that replays 50 warehouse queries using a `Hyderabad_Supervisor` JWT against an index seeded with `Gurugram_HR` files and asserts `0` HR chunks in both vector and BM25 candidate lists.
  * *Production Monitor:* **5-Minute "Honey-Doc" Canary Probe**—a background worker using a lowest-privilege warehouse token continuously queries the live index for keywords inside planted fake `CANARY_HR_RESTRUCTURING` documents. If a canary chunk ever returns a score > 0, it pages on-call and auto-trips the RAG kill-switch in seconds.

---

### 5. Turning Anand's "Mujhe Galat Number Nahi Chahiye" Into Honest, Measurable Promises

If you promise a CFO "the AI will never give a wrong number" without defining what happens when accounting data itself settles late, you're setting yourself up to fail again. Here is how we turn Anand's requirement into four measurable SLAs we can actually keep:

1. **0.0% Unverified Numbers (Enforced by Code Verifier):** The assistant cannot output a number in a factual claim unless our Python verifier matches that exact number to a recorded ClickHouse SQL result hash (`SHA-256`) or an active policy document span.
2. **100% SQL-Only Routing for Business Metrics:** Aggregate KPIs (refund rates, GMV, SLA percentages) are never answered from PDFs, emails, or spreadsheets.
3. **Dual Reporting on Unsettled COD Cohorts:** Because half our orders are Cash on Delivery, any refund or revenue metric automatically shows two numbers side-by-side: **Settled in Ledger (e.g., 4.1%)** and **Inclusive of In-Flight / Late COD Returns (e.g., 6.3%)**, stamped with the exact minute the database was synced.
4. **100% Abstention When Data is Missing or Stale:** If ClickHouse CDC is lagging by more than 60 minutes, or if a metric isn't defined in the Finance-approved SQL catalog, the bot returns `CANNOT_DETERMINE` and states what is missing instead of guessing.