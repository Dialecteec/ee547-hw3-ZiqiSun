# Problem 2

## Schema Design Decisions:
- Partitioning by category keeps those items co-located and makes range queries efficient; directly optimizes Query 1 and supports Query 4.
- Single-table design with GSIs:
  - `AuthorIndex` by author; serves Query 2: papers by a given author.
  - `PaperIdIndex` by arXiv ID; serves Query 3: get full paper by id as a direct key lookup.
  - `KeywordIndex` by keyword; serves Query 5: search by keyword
- Denormalizes papers into category/author/keyword items.
- Trade-offs: Updates to a paper require multi-item fan-out writes.

## Denormalization Analysis:
- Average number of DynamoDB items per paper: 80/5=16
- Storage multiplication factor: 16
- Which access patterns caused the most duplication?: keyword

## Query Limitations:
- What queries are NOT efficiently supported by your schema?
  - Global aggregates e.g. “Top authors across all categories” “Most cited papers globally”
  - Complex multi-dimensional filters e.g. “Papers with keywords A and B and C across all categories and within a custom date window”
- Why are these difficult in DynamoDB?:
  - DynamoDB does not support server-side joins or ad-hoc aggregations since operations outside the partition key require scans

## When to Use DynamoDB:
- Based on this exercise, when would you choose DynamoDB over PostgreSQL?:
 - Choose DynamoDB when:
  - Have high-throughput, low-latency key-value / partition queries, with well-known access patterns and the ability to pre-denormalize.
  - Need horizontal scalability with predictable performance and pay-per-request pricing.
  - Accept fan-out writes to keep denormalized views in sync.
 - Choose PostgreSQL when:
  - Need flexible ad-hoc queries, joins, and aggregations which didn’t model in advance.
  - Want strong transactional guarantees across multiple tables and complex constraints.
  - Data model evolves frequently and can’t afford to manage denormalization fan-out.
- What are the key trade-offs?:
 - DynamoDB trades write/storage complexity for O(1) reads on predefined patterns; PostgreSQL trades query flexibility (and simpler writes) for limited horizontal scaling unless you introduce sharding/federation.

## EC2 Deployment:
- EC2 instance public IP: 18.220.155.41
- IAM role ARN used: AmazonDynamoDBFullAccess




