# EcoGrid Energy — Member 1 Architecture Contribution

**Contributor:** Kiran Kapilavai

**Scope:** Report Sections 1.1, 1.2, 2.1 and 2.1.1.

**AI disclosure:** Extracted from the AI-assisted report revision prepared on 8 October 2026. The full report records the new AI analysis in Tables 1 and 2 and Appendix B1.

## 1.1 Executive Summary

EcoGrid Energy is building a peer-to-peer (P2P) renewable energy trading platform that allows households with rooftop solar panels (prosumers) to sell their surplus electricity directly to neighbouring consumers. The purpose of the platform is to create a local, transparent market for renewable energy, so that prosumers receive a fair return for the energy they export and consumers can buy locally generated clean energy.

The platform depends on three main capabilities, each with a very different workload. Marketplace manages energy offers and bids, matches buyers with sellers and records the resulting trades. Smart Meter Integration continuously ingests and validates high-frequency readings from household smart meters to confirm how much energy each household generates and consumes. Financial Settlement calculates the amounts owed for each completed trade and securely processes the payments.

The group proposes an Event-Driven Microservices Architecture. The three capabilities are mapped to independently deployable services aligned with the bounded contexts, with private data ownership [8]. An event-streaming platform such as Apache Kafka carries domain events, while synchronous APIs are retained for placing an offer or checking a trade [9]. A Saga coordinates settlement through local transactions and compensating actions, rather than providing one cross-service ACID transaction [6].

The group selected this architecture after comparing a monolith, microservices and an event-driven design (Section 3.3). Smart-meter ingestion produces a continuous, high-volume stream, whereas trading and settlement have different latency and consistency requirements. Separating ingestion lets that workload scale independently and reduces its impact on trading. The additional coordination and operating costs are addressed through idempotent consumers, controlled retries, dead-letter handling, reconciliation and monitoring [6], [9]. Decomposition is limited to three primary business services, with capacity and operational maturity validated for a four-person team.

## 1.2 Architectural Drivers

Architectural drivers are the quality attributes that most strongly influence EcoGrid’s structure. The table ranks their influence on the proposed design, links each to an EcoGrid problem and identifies the architectural response. Reliability and security remain essential acceptance conditions regardless of their position in this structural ranking. Numerical values are proposed targets to validate through testing.

| Priority | Driver | EcoGrid Requirement | Architectural Response |
| --- | --- | --- | --- |
| 1 | Scalability | A growing number of smart meters send readings continuously, and the volume increases with every household that joins the platform. | Separate Meter Integration; use partitioned Kafka topics and consumer groups to scale ingestion independently. Validate partitioning and load before setting capacity [9]. |
| 2 | Performance | Buyers and sellers expect fast responses when posting offers and bids or viewing trades, even during peaks in meter traffic. | Separate synchronous trading APIs from the asynchronous meter pipeline. Consume summarised availability updates. Proposed target: 95% of Marketplace requests under 300 ms. |
| 3 | Reliability | Meter readings and confirmed trades must not be lost or duplicated, because they decide what each seller is paid. | Durable event handling, controlled retries, idempotent consumers, dead-letter handling, outbox publication and reconciliation. Validate the zero acknowledged-event-loss target under defined failures [6], [7], [9]. |
| 4 | Security | The platform handles personal details, household energy-usage patterns and payments, and meter devices could be spoofed. | Authenticate users and devices, authorise actions, encrypt communication and isolate payment credentials. Propose security checks in CI/CD. |
| 5 | Maintainability | Trading rules, IoT handling and settlement logic change for different reasons and are owned by different developers in a small team. | Use cohesive context ownership and private stores. Propose build checks for forbidden code dependencies and integration checks for cross-context database access. |
| 6 | Evolvability | New meter types, pricing or matching models and payment methods are expected as EcoGrid grows. | Version event contracts and check schema compatibility. Independent deployment reduces change impact, subject to compatible contracts. |
| 7 | Observability | One trade passes asynchronously through three services, so failures are harder to locate than in a single application. | Use centralised logs and metrics, correlation identifiers, distributed tracing, consumer-lag monitoring and dead-letter alerts. |

Why this order. Scalability and performance drive the separation of ingestion from trading. Reliability and security remain essential financial safeguards. Maintainability and evolvability guide ownership and contracts, while observability supports diagnosis. The ranking expresses structural influence, not permission to weaken payment integrity. The four-person team must validate the operating cost of the distributed design.

Trade-offs between drivers. Asynchronous buffering introduces delayed data and coordination risks. Local financial updates remain atomic, while a Saga coordinates eventual consistency across services [6]. Marketplace needs quantity reservations and a freshness policy. An outbox records the trade and outgoing event together; consumers still handle duplicates [7]. Durability and security controls can add latency, so performance targets must be tested with those controls enabled [9].

## 2. Domain Decomposition

## 2.1 Bounded Contexts

Domain-Driven Design separates EcoGrid into three bounded contexts, each with a consistent language and model [8]. A bounded context defines a model boundary rather than automatically requiring a separate deployment. In the nominated architecture, the group maps these contexts to three primary services with private stores. Required information crosses boundaries through explicit event or API contracts.

| S.No | Bounded Context | Responsibilities | Data Owned | Outside the Boundary |
| --- | --- | --- | --- | --- |
| 1 | Marketplace | Manage energy offers and bids; match buyers and sellers; create trades; maintain trade status through its lifecycle; provide trading APIs to the customer app. | Offers, bids, trades, trade status history, agreed prices, matching rules. | Raw meter readings, device data, payment details and settlement calculations. |
| 2 | Smart Meter Integration | Register and authenticate meters; ingest, validate, de-duplicate and aggregate readings. Publish raw-reading events within the metering pipeline and summarised EnergyAvailabilityUpdated events for Marketplace. | Device registry, raw and aggregated readings, timestamps, energy measurements, validation results. | Offers, bids, prices, trades and any payment information. |
| 3 | Financial Settlement | Consume TradeMatched events; calculate amounts payable and fees; process payments through a payment provider; publish SettlementCompleted or SettlementFailed; reconcile trades against payments. | Settlement records, transactions, payment references, ledger entries, reconciliation results. | Matching rules, trade lifecycle and meter readings; it reports the payment outcome but does not change trade status itself. |

Energy has a different meaning in each context: a device measurement in Smart Meter Integration, a quantity offered for sale in Marketplace, and a financial obligation at an agreed price in Settlement. Separate models keep device formats, matching rules and payment-provider changes behind their respective contracts [8]. Shared trade, device and correlation identifiers support traceability without shared record ownership. The design prohibits direct access to another context’s database; the proposed maintainability checks would verify those boundaries during development.

## 2.1.1 Marketplace Domain Design

Marketplace is treated as EcoGrid’s core domain because its trading rules create the platform’s customer value. The model below covers offers, bids, matching, trades and lifecycle state [8]. Price-then-time matching, partial matching and cancellation policies are proposed business rules requiring stakeholder validation.

| S.No | Concept | Description | Key Attributes |
| --- | --- | --- | --- |
| 1 | Energy Offer | A seller’s surplus-energy listing for a delivery period. Validate its quantity against current, validated availability after subtracting existing reservations. Reserve capacity atomically; apply a freshness and event-version policy. | Offer ID, seller ID, quantity (kWh), minimum price per kWh, delivery window, status (Open, Partially Matched, Filled, Withdrawn, Expired). |
| 2 | Energy Bid | A buyer’s request to purchase energy for a delivery period at no more than a stated price. | Bid ID, buyer ID, quantity (kWh), maximum price per kWh, delivery window, status. |
| 3 | Buyer–Seller Matching | A domain service that pairs compatible delivery windows and prices. The proposed prototype uses price-then-time ordering and permits partial matching, with atomic quantity allocation. | Matching rule, matching interval, match result. |
| 4 | Trade | The aggregate created when an offer and a bid are matched; the record of an agreement between one seller and one buyer. | Trade ID, offer ID, bid ID, seller ID, buyer ID, quantity, agreed price, delivery window, correlation ID, status. |
| 5 | Trade Status | The lifecycle state of a trade. It is owned only by Marketplace and changes in response to Marketplace actions and Settlement events. | Matched, Settlement Pending, Settled, Settlement Failed, Cancelled. |

Trade status lifecycle. Matching creates a trade after a concurrency-safe quantity reservation. A local transaction stores the trade and its TradeMatched outbox record, and marks settlement as pending; a relay then publishes the event [7]. Marketplace applies SettlementCompleted or a definitive SettlementFailed outcome once per event identifier. Temporary outages or an unknown provider outcome leave the trade pending while Settlement retries or reconciles [6]. Cancellation releases reserved quantities only after a confirmed unrecoverable failure, or before payment processing starts. A stale meter snapshot is insufficient to guarantee actual future generation.

| Trade Status | Meaning | Triggered By |
| --- | --- | --- |
| Matched | An offer and a bid have been paired and a trade has been created. | Matching service |
| Settlement Pending | TradeMatched is queued for publication and the trade awaits the payment outcome. | Local transaction records the trade and outgoing TradeMatched event in the outbox |
| Settled | Payment is complete and the trade is final. | SettlementCompleted event |
| Settlement Failed | Settlement has confirmed an unrecoverable payment failure; temporary uncertainty remains pending. | Definitive SettlementFailed event |
| Cancelled | The reserved quantity is released for permitted re-matching. | Compensation after confirmed failure, or cancellation before payment processing starts |

What does not belong in Marketplace. Raw device readings, device validation and time-series storage belong to Smart Meter Integration. Marketplace subscribes only to summarised EnergyAvailabilityUpdated and settlement-outcome events; the shared bus in Section 2.2 does not imply subscribing to every meter event. Payment credentials, fee calculations and provider calls belong to Financial Settlement. These boundaries reduce the effect of meter-processing spikes and provider outages on trading APIs, while dependent trades may remain pending until fresh data or a confirmed settlement outcome is available.

## References

[1] Victorian Institute of Technology, “ICT711 Advanced Software Engineering,” course materials, 2026.

[2] E. Evans, Domain-Driven Design: Tackling Complexity in the Heart of Software. Boston, MA, USA: Addison-Wesley, 2003.

[3] S. Newman, Building Microservices: Designing Fine-Grained Systems, 2nd ed. Sebastopol, CA, USA: O’Reilly Media, 2021.

[4] C. Richardson, Microservices Patterns: With Examples in Java. Shelter Island, NY, USA: Manning Publications, 2018.

[5] M. Kleppmann, Designing Data-Intensive Applications. Sebastopol, CA, USA: O’Reilly Media, 2017.

[6] Microsoft, “Saga distributed transactions pattern,” Azure Architecture Center. [Online]. Available: https://learn.microsoft.com/en-us/azure/architecture/patterns/saga. Accessed: Oct. 8, 2026.

[7] C. Richardson, “Pattern: Transactional outbox,” Microservices.io. [Online]. Available: https://microservices.io/patterns/data/transactional-outbox.html. Accessed: Oct. 8, 2026.

[8] E. Evans, Domain-Driven Design Reference: Definitions and Pattern Summaries. Domain Language, 2015. [Online]. Available: https://www.domainlanguage.com/wp-content/uploads/2016/05/DDD_Reference_2015-03.pdf. Accessed: Oct. 8, 2026.

[9] Apache Software Foundation, “Design,” Apache Kafka 4.1 documentation. [Online]. Available: https://kafka.apache.org/41/design/design/. Accessed: Oct. 8, 2026.
