<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# Apache Solr Sandbox — Threat Model (v0 DRAFT)

## §1 Header

- **Project:** Apache Solr Sandbox (`apache/solr-sandbox`) — a repository that
  hosts **experimental Solr modules and features that are not part of core
  Solr**, so contributors can try ideas "without affecting the main project"
  *(documented — `README.md`)*. As of this draft it contains two functional
  module families — an **index encryption-at-rest** module (`encryption/`) and
  a **cross-data-center replication (CrossDC)** module
  (`crossdc-producer`, `crossdc-consumer`, `crossdc-commons`) — plus load-test
  tooling (`gatling-simulations`, `gatling-data-prep`). It is **not** the Solr
  search server; the search server is modelled separately in `apache/solr`'s
  `THREAT_MODEL.md`, cross-referenced throughout.
- **Written against:** `main` @ HEAD (2026-07).
- **Author:** ASF Security team, via the threat-model-producer rubric
  (Scovetta rubric), as a starting point for the Solr PMC.
- **Date:** 2026-07-14.
- **Status:** **v0 DRAFT** — produced by the ASF Security team *for the Solr
  PMC to review, correct, own, and ratify*. **Not yet ratified.** Almost every
  claim below is *(inferred)* and routed to a §14 open question. Do not treat
  any statement here as a maintainer position until the PMC has confirmed it.
- **Version binding:** the sandbox is **not versioned as an ASF release** (see
  the defining fact below). Once ratified, this model is triaged against the
  commit a user actually built from, not against a release tag.
- **Canonical role (proposed):** intended to be the sandbox's
  **scanner-facing** security model — the authoritative basis for triaging an
  automated finding against experimental sandbox code as in- or out-of-model.
  It deliberately does **not** restate Solr's own trust model; it covers only
  what is *different* about building and running unreleased sandbox modules.
  This role split is a §14 question for the PMC to confirm.
- **Reporting cross-reference:** §8-violating findings via the ASF security
  process (report to `security@solr.apache.org`, see
  <https://solr.apache.org/security.html>); §3/§9 findings closed citing this
  document. The sandbox has no separate disclosure channel from Solr — §14
  confirms.
- **Provenance legend:** *(documented)* = stated in the repo's module docs
  (`README.md`, `ENCRYPTION.md`, `CROSSDC.md`) or code (cited); *(maintainer)*
  = confirmed by a Solr PMC member in response to this draft; *(inferred)* =
  reasoned from code/doc structure or domain knowledge, not yet confirmed —
  each has a matching §14 open question.
- **Draft confidence:** ~16 documented / 1 maintainer / ~26 inferred. This is a
  v0: the *(documented)* claims are lifted from the module READMEs and code;
  everything about *intent, contract, and the scope line* is still hypothesis
  awaiting the PMC.

**What it is — the defining security fact.** Solr Sandbox is an **incubation
area for unreleased Solr code**. The Solr PMC's own framing is that
**"we don't release code from this [repo] directly, but anticipate people to
build and use it"** *(maintainer — Anshum Gupta, Solr PMC, 2026-06-04)*.
Two consequences drive this entire model:

1. **These are not released, signed ASF artifacts.** A user obtains the code by
   **building from source** off an experimental branch and dropping the
   resulting jars into a Solr install (encryption/CrossDC-producer plugins) or
   running a standalone app alongside Solr (CrossDC-consumer). The trust posture
   is *"you are running unreleased, less-reviewed, lower-assurance code that you
   chose to build."*
2. **Each module extends Solr's own trust model.** A producer/encryption plugin
   runs **inside the Solr JVM with Solr's privileges**, so the relevant security
   boundary is the *same* trusted-environment + auth/authz posture as the search
   server (see `apache/solr`'s model) — but at **lower assurance** because the
   code is experimental. The CrossDC Consumer is a separate JVM process with its
   own small HTTP surface.

## §2 Scope and intended use

The sandbox is **not one artifact**; it is a collection of independent modules,
each with a different deployment shape. The unifying facts are (1) unreleased /
built-from-source and (2) each extends or attaches to a Solr deployment. Roles
mirror Solr's (`apache/solr` §2): **untrusted client**, **authenticated user**,
and **operator/admin** (fully trusted — builds the code, installs the plugins,
owns all configuration, Kafka, ZooKeeper, and the KMS).

**Component families.**

| Family | Entry point | Privilege / exposure | In model? |
| --- | --- | --- | --- |
| **Encryption — index/crypto core** (`org.apache.solr.encryption`, `.crypto`) | `EncryptionDirectoryFactory`, AES-CTR encrypters | in-Solr; confidentiality of index files at rest | **Yes (primary — a security feature)** |
| **Encryption — request handler** | `/admin/encrypt` (`EncryptionRequestHandler`, `KmsEncryptionRequestHandler`) | in-Solr HTTP handler; triggers (re)encryption **and decryption to cleartext** (`no_key_id`) | **Yes (highest sensitivity)** |
| **Encryption — key supply / KMS** | `KeySupplier` (user-implemented), `KmsKeySupplier`/`KmsClient`, `KmsKeyCache` | in-Solr; holds key secrets in memory | **Yes (module side); user's `KeySupplier`/`KmsClient` impl = §3** |
| **CrossDC Producer plugins** | `MirroringUpdateProcessor`, `MirroringCollectionsHandler`, `MirroringConfigSetsHandler` | in-Solr plugins; intercept updates/admin/configset requests, push to Kafka | **Yes** |
| **CrossDC Consumer app** | standalone JVM; `/metrics`, `/threads` on port 8090; pulls Kafka → writes backup Solr | separate process; **own HTTP surface**; writes into backup Solr | **Yes** |
| **CrossDC config in ZooKeeper** | `crossdc.properties` in Solr ZK root znode | cluster-wide config store (trusted) | **Yes (trust ZK)** |
| **Kafka coupling** | Kafka topic(s) carrying mirrored Solr requests across DCs | the inter-DC transport | **Yes (trust Kafka infra)** |
| **The Solr search server the plugins run in** | Solr HTTP API | query/index/admin | **No — `apache/solr` model** (§3) |
| **Load-test tooling** | `gatling-simulations`, `gatling-data-prep`, `*Test.java`, `cluster*.sh`, `manual-test.sh` | dev/test only | **No — §3 (unsupported)** |

## §3 Out of scope (explicit non-goals)

- **The Apache Solr search server itself.** Everything about the Solr HTTP API
  — query/update/admin surface, SSRF via `shards`/streaming, Solr auth/authz
  plugin correctness, RCE via risky Solr features, the trusted-environment
  deployment contract — is modelled by `apache/solr`'s `THREAT_MODEL.md`. A
  sandbox plugin runs *inside* that model; this document covers only what the
  sandbox module *adds*. A finding that is really a Solr-core finding routes to
  the Solr model. *(documented — separate repo; Q-scope.)*
- **"It is unreleased / experimental / less-reviewed" as a finding in itself.**
  The entire repo is experimental **by design** *(maintainer — 2026-06-04)*.
  A blanket observation that the code is a sandbox, is not release-signed, or
  has had less review is **not an in-model finding**; a finding must identify a
  concrete defect in a specific module (see the scope line, §14 Q-line).
- **The user's own key-management code.** The encryption module *requires the
  user to implement* `KeySupplier.Factory` (and, for KMS, `KmsClient`)
  *(documented — `ENCRYPTION.md`, "some coding required")*. The correctness and
  security of that user-supplied implementation, and of the KMS it talks to, is
  the user's, not the module's. *(documented — `ENCRYPTION.md`; Q-keysupplier.)*
- **Kafka, ZooKeeper, and the KMS as infrastructure.** Securing/patching/ACLing
  the Kafka cluster, the ZooKeeper ensemble that holds `crossdc.properties`, and
  the external Key Management System is the operator's. *(inferred — Q-infra.)*
- **The JVM / OS / network / build toolchain** the operator builds and runs on.
- **Load-test and dev tooling** — `gatling-*`, `SolrAndKafkaIntegrationTest.java`,
  `cluster.sh`/`cluster-stop.sh`/`manual-test.sh`, `scripts/`. Not shipped to
  production; threat-model separately if ever promoted. *(inferred — Q-scope.)*

## §4 Trust boundaries and data flow

There is no single boundary; each module has its own.

```
── Encryption (in Solr JVM) ─────────────────────────────────────────────
client ─(Solr auth+authz)─► /admin/encrypt ─► EncryptionRequestHandler
    encryptionKeyId=<id>   → (re)encrypt index with that key
    encryptionKeyId=no_key_id → DECRYPT index to cleartext          (!)
EncryptionDirectoryFactory ─► AES-CTR encrypt/decrypt of Lucene files at rest
KeySupplier / KmsKeySupplier ─► key secret held in memory (KmsKeyCache, wiped)
    key id stored in commit metadata; key SECRET never persisted

── CrossDC (source DC ──► Kafka ──► backup DC) ──────────────────────────
source Solr ─(MirroringUpdateProcessor / *Handler plugins)─► Kafka topic
                                                              │ (mirrored Solr requests)
CrossDC Consumer ◄── Kafka ── (config from ZK crossdc.properties)
   ├─► writes mirrored updates/admin/configset requests into BACKUP Solr
   └─► exposes /metrics, /threads (thread dump) on port 8090
```

**Reachability precondition (triager's test).** A sandbox finding is in-model
only if it (a) lands in one of the §2 module families a user is anticipated to
build-and-use, **and** (b) is reachable by an in-scope adversary (§7) in a way
that breaks a §8 property — e.g. an authenticated-but-unauthorized Solr user who
can reach `/admin/encrypt` and decrypt the index, a crypto defect that lets
ciphertext be read as plaintext, or an unauthenticated reach of the Consumer's
`/threads` surface. A finding that is really a Solr-core issue (§3), that
requires trusting Kafka/ZK/KMS the model already trusts (§3), that lands in
load-test tooling (§3), or that amounts to "this is experimental code" (§3), is
out of model. *(inferred — Q-line/Q-reach.)*

## §5 Assumptions about the environment

- **The operator builds these modules themselves** from an experimental branch
  and installs them into a Solr install (plugins in `sharedLib`/`lib`; Consumer
  as a standalone distribution). There is no signed ASF release to verify
  against. *(maintainer — 2026-06-04; Q-line.)*
- **The surrounding Solr runs in its supported posture** — trusted environment,
  authentication + authorization enabled — per `apache/solr`'s model. The
  sandbox plugins inherit that posture and rely on it. *(inferred — Q-solr.)*
- **Kafka and ZooKeeper are trusted infrastructure** the operator secures.
  `crossdc.properties` in the Solr ZK root znode is a trusted config source.
  *(documented — `CROSSDC.md`; trust is Q-infra.)*
- **A KMS (if used) is trusted** and the user's `KmsClient` reaches it
  securely. *(documented — `ENCRYPTION.md`; Q-keysupplier.)*
- **What the modules do to their host:** encryption plugins run in the Solr JVM
  and touch the filesystem (index files) and heap (key secrets); the CrossDC
  Producer opens outbound connections to Kafka from within Solr; the CrossDC
  Consumer is a separate JVM that opens a **listening HTTP port (default 8090)**
  and outbound connections to Kafka, ZooKeeper, and the backup Solr. *(documented
  — `CROSSDC.md`; the full side-effect inventory is Q-sideeffects.)*

## §5a Configuration variants — the knobs that move the model

- **Encryption — `no_key_id` decrypt path.** `EncryptionRequestHandler` at
  `/admin/encrypt` decrypts the whole index to cleartext when called with
  `encryptionKeyId=no_key_id`. Whether this handler is restricted to admins is
  **Solr authz configuration**, not a module default. *(documented —
  `ENCRYPTION.md`; Q-encrypt-authz.)*
- **Encryption — encrypter choice.** Default `CipherAesCtrEncrypter`;
  `LightAesCtrEncrypter` (+10% perf) calls an internal
  `com.sun.crypto.provider.AESCrypt()` constructor requiring
  `--add-opens=java.base/com.sun.crypto.provider=ALL-UNNAMED` on JDK 17+.
  A non-default performance option that widens JVM module access. *(documented —
  `ENCRYPTION.md`; Q-lightaes.)*
- **CrossDC — `indexUnmirrorableDocs`** (default false): if true, oversized
  updates are still indexed locally though never mirrored — a
  silent-divergence knob. *(documented — `CROSSDC.md`.)*
- **CrossDC — `collapseUpdates` / `expandDbq`**: change ordering/semantics of
  replicated ops (documented correctness limitations, not security). *(documented.)*
- **CrossDC — `enableDataCompression`**: gzip/snappy/lz4/zstd on the Kafka
  wire. **Compression is not encryption**; it does not protect confidentiality
  on the wire. *(documented — `CROSSDC.md`; false friend, §9.)*
- **CrossDC — Consumer HTTP port** (`port`, default 8090) exposing `/metrics`
  and `/threads`. Whether this is bound to a private interface / firewalled is
  operator deployment config. *(documented — `CROSSDC.md`; Q-consumer-port.)*

**Wave-1 ruling needed (Q-encrypt-authz / Q-consumer-port):** for each item
above whose safe operation depends on operator configuration, confirm whether
the unhardened state is the *supported posture* (report `VALID`) or a
documented operator responsibility (report `OUT-OF-MODEL: non-default-config`).

## §6 Assumptions about inputs

| Boundary / source | Input | Attacker-controllable? | Enforced by / caller must |
| --- | --- | --- | --- |
| `/admin/encrypt` (Solr HTTP) | `encryptionKeyId` (incl. `no_key_id` = decrypt), `distrib`, KMS `tenantId`/`encryptionKeyBlob` | **yes** (a Solr client) | **Solr authz must restrict `/admin/encrypt` to admins** — else an authorized-but-unprivileged user can (re/de)crypt the index |
| encryption `KeySupplier` | key blob / key cookie from client (KMS mode) | **yes** | user's `KeySupplier`/`KmsClient` validates; KMS decrypts |
| Lucene index files on disk | encrypted file bytes + per-file IV/metadata | as trusted as the filesystem | module decrypts; Lucene checksums detect corruption (not tampering — §9) |
| CrossDC Producer (in Solr) | the Solr update/admin/configset request being mirrored | **yes** (whoever can write to source Solr) | source-Solr auth+authz gates who can index/admin |
| Kafka topic | mirrored Solr requests in transit | **yes if Kafka is reachable** | operator secures Kafka (TLS/SASL/ACLs) |
| CrossDC Consumer → backup Solr | requests replayed from Kafka | trusted **only** to the extent source DC + Kafka are trusted | Consumer authenticates to backup Solr; operator secures the path |
| CrossDC Consumer HTTP | `GET /metrics`, `GET /threads` on port 8090 | **yes if the port is reachable** | operator network-isolates the port (no auth described) |
| ZooKeeper | `crossdc.properties` (bootstrapServers, topicName, all crossdc props) | trusted store | operator ACLs ZooKeeper |

## §7 Adversary model

- **Authenticated-but-unauthorized Solr user** — has Solr credentials, tries to
  reach a sandbox surface beyond their permission. In scope: e.g. reaching
  `/admin/encrypt` to trigger decryption (`no_key_id`) of the index if authz
  does not stop them. The defence is Solr's authz plus correct handler wiring.
  *(inferred — Q-encrypt-authz.)*
- **Untrusted network actor who can reach an exposed module surface** — in
  scope *only* where a module exposes its own surface (the CrossDC Consumer's
  port 8090 `/metrics` / `/threads`) and it is reachable. Tries to read thread
  dumps / metrics for reconnaissance, or disrupt the Consumer. *(inferred —
  Q-consumer-port.)*
- **An adversary who can decrypt or read the encrypted index files** — the
  encryption module's raison d'être is to deny this actor cleartext given only
  the files at rest (not the running JVM / not the key). In scope for the
  encryption crypto core. *(documented — `ENCRYPTION.md`.)*
- **Out of scope:** an attacker who already controls the Solr JVM / heap (can
  read live keys — encryption defends files at rest, not a compromised
  process); the Kafka/ZooKeeper/KMS operator; the host/build toolchain; and an
  attacker whose only claim is "this is unreleased experimental code" (§3).

## §8 Security properties the modules provide

*(All conditional on the surrounding Solr running in its supported posture —
§5 — and on the operator's infra being secured — §3.)*

1. **Encryption — confidentiality of the index at rest.** With
   `EncryptionDirectoryFactory` configured, Lucene index files (and update logs)
   are AES-CTR encrypted; the key *secret* is never persisted (only the key id
   goes in commit metadata; KMS key cache is wiped after a short duration).
   *Violation:* cleartext index content recoverable from the encrypted files
   alone, or the key secret written to disk/logs. *Severity:* **critical**.
   *(documented — `ENCRYPTION.md`; that the implementation actually achieves it
   is Q-crypto.)*
2. **Encryption — keystream uniqueness (nonce-misuse resistance).** A fresh
   random IV per file (5-byte CTR counter + 11-byte random nonce) avoids AES-CTR
   keystream reuse across Lucene's write-once segment files. *Violation:* IV/
   nonce reuse under the same key → keystream reuse → plaintext recovery.
   *Severity:* **critical**. *(documented — `ENCRYPTION.md` rationale;
   Q-crypto.)*
3. **Encryption — encrypted backup/replication fidelity.** `EncryptionBackupRepository`
   copies files in encrypted form and verifies Lucene checksums before copy.
   *Violation:* cleartext leak through the backup/replication path, or a
   corrupted copy passing as valid. *Severity:* high (confidentiality) /
   correctness. *(documented — `ENCRYPTION.md`; Q-crypto.)*
4. **Encryption — authorization of the (de)encryption trigger.** `/admin/encrypt`
   (including the `no_key_id` decrypt) is a Solr request handler and is only as
   protected as Solr authz makes it. The property "only an admin can trigger
   (de)cryption" holds **iff** the operator restricts the path. *Violation:* a
   non-admin decrypts or re-encrypts the index. *Severity:* **critical**.
   *(inferred — Q-encrypt-authz.)*
5. **CrossDC — mirrored-request integrity within the trusted path.** Given a
   trusted Kafka + ZooKeeper, requests mirrored from the source DC are delivered
   to and replayed against the backup Solr (with configurable retry / dead-letter
   handling). *Violation:* silent divergence not attributable to a documented
   knob (`indexUnmirrorableDocs`, `collapseUpdates`, `expandDbq`). *Severity:*
   correctness (security only if it crosses a data-authorization boundary).
   *(documented — `CROSSDC.md`; Q-crossdc-integrity.)*

## §9 Security properties the modules do *not* provide

- **Encryption does NOT provide integrity/authenticity of the index (false
  friend).** AES-CTR gives **confidentiality only**. Lucene checksums detect
  *accidental* corruption; they are **not a MAC** and do not defend against an
  adversary who can *modify* the encrypted files. "The index is encrypted, so it
  can't be tampered with" is false. *(documented — `ENCRYPTION.md` states
  integrity is via Lucene checksums; the security implication is Q-integrity.)*
- **Encryption does NOT protect a running/compromised process.** Keys live in
  JVM memory while in use; an attacker with heap/JVM access, or with the key,
  gets cleartext. It defends *files at rest*, not a live server. *(documented —
  `ENCRYPTION.md` framing; Q-crypto.)*
- **Encryption does NOT manage your keys.** Key lifecycle, rotation policy,
  storage, and the KMS itself are the user's `KeySupplier`/`KmsClient` (§3).
  *(documented — `ENCRYPTION.md`.)*
- **CrossDC does NOT secure the Kafka wire.** Mirrored Solr requests (your
  document data) traverse Kafka; confidentiality/integrity/auth on that wire is
  Kafka config (TLS/SASL/ACLs), the operator's. `enableDataCompression` is
  **not** encryption (false friend). *(documented — `CROSSDC.md`; Q-infra.)*
- **The CrossDC Consumer's HTTP endpoints are not described as authenticated.**
  `/threads` returns a full JVM thread dump and `/metrics` returns processing
  metrics on port 8090; protecting that port is operator network config.
  *(documented — `CROSSDC.md`; Q-consumer-port.)*
- **No property is claimed to a higher assurance than "experimental."** Being
  unreleased, these modules make no availability/robustness guarantee beyond
  what the code and docs state. *(maintainer — 2026-06-04.)*
- **Well-known classes left to the operator:** confidentiality-on-the-wire for
  cross-DC replication (Kafka TLS); information exposure via the Consumer's
  diagnostic endpoints; authorization of Solr-facing handlers (`/admin/encrypt`);
  and every search-server class already owned by `apache/solr`'s model (SSRF,
  RCE-via-risky-features, exposure-without-auth) — the sandbox plugins do not
  change those, they inherit them.

## §10 Downstream responsibilities (operator / builder)

- **Accept and manage the experimental-code risk.** You are running code the ASF
  does **not** release or sign; build from a commit you have reviewed/pinned and
  track upstream changes yourself. *(maintainer — 2026-06-04.)*
- **Run the surrounding Solr in its supported posture** (auth + authz on,
  trusted environment) per `apache/solr`'s model — the plugins depend on it.
- **Encryption:** implement a secure `KeySupplier`/`KmsClient`; own key
  lifecycle/rotation and secure the KMS; **restrict `/admin/encrypt` (and the
  `no_key_id` decrypt) to admins via Solr authz**; understand that encryption
  gives confidentiality-at-rest, **not** tamper-resistance.
- **CrossDC:** secure Kafka (TLS/SASL/ACLs) — the mirrored data is your index
  content; **network-isolate the Consumer's HTTP port (8090)** — `/threads`
  leaks JVM internals; secure the ZooKeeper holding `crossdc.properties`; treat
  the Consumer's writes to the backup Solr as trusted only to the extent the
  source DC + Kafka are trusted.
- **Do not deploy sandbox modules as if they were hardened, released Solr
  features.**

## §11 Known misuse patterns

- **Treating AES-CTR encryption-at-rest as tamper-proof / integrity-protected.**
  It is confidentiality-only; Lucene checksums are not a MAC (§9).
- **Leaving `/admin/encrypt` reachable by non-admins**, letting an authenticated
  user decrypt the index (`no_key_id`) or force costly re-encryption.
- **Exposing the CrossDC Consumer's `/threads` / `/metrics` port** to an
  untrusted network (reconnaissance / info exposure).
- **Running the cross-DC Kafka queue without TLS/auth**, putting index data on
  the wire in cleartext — and mistaking `enableDataCompression` for encryption.
- **Deploying these unreleased modules as production-hardened features**, or
  building from an arbitrary/unpinned experimental commit.
- **Unsecured ZooKeeper** holding `crossdc.properties` (a lever over the
  replication topology and Kafka endpoints).

## §11a Known non-findings (recurring false positives)

- **"This code is unreleased / experimental / less reviewed."** By design
  (§3); not a finding on its own — a report must name a concrete in-module
  defect (see the scope line, §14 Q-line). *(maintainer — 2026-06-04.)*
- **Findings in `gatling-*`, `*Test.java`, `cluster*.sh`, `manual-test.sh`,
  `scripts/`** — dev/test tooling, `OUT-OF-MODEL: unsupported-component` (§3).
- **"A sandbox plugin runs inside Solr with Solr's privileges / loads custom
  code."** That is what a Solr plugin *is*; `VALID` only if it lets an actor
  cross an authz boundary that should have stopped them (§8), else by design.
- **"`/admin/encrypt` can decrypt the index (`no_key_id`)"** reached by an
  authorized admin — the feature, not a bug; `VALID` only if reachable across an
  authz boundary (§8.4) — a §14 ruling pending.
- **"Kafka wire is cleartext" / "Consumer port 8090 is unauthenticated"** where
  the operator has not applied the documented network/TLS controls — operator
  responsibility (§9/§10); `OUT-OF-MODEL: non-default-config` pending the §14
  ruling on whether unhardened is the supported posture.
- **"AES-CTR provides no integrity"** — by design (§9); integrity-at-rest is not
  a claimed property.
- **Weaknesses in the user-supplied `KeySupplier`/`KmsClient`** — user code,
  `OUT-OF-MODEL` (§3).
- **Third-party dependency CVEs** (Kafka client, etc.) — case-by-case: in model
  only when there is a reasonable sandbox-side mitigation; otherwise routed to
  the dependency's project.

## §12 Conditions that would change this model

- A sandbox module being **promoted into a Solr release** (it would then fall
  under `apache/solr`'s model at full assurance) — the single most important
  trigger.
- A new module landing in the repo, or a module gaining a new externally
  reachable surface (new handler, new listening port, new protocol).
- A change to the encryption crypto construction (cipher, IV/nonce scheme, key
  handling) or to how `/admin/encrypt` authorizes.
- A change to what the CrossDC Consumer exposes on its HTTP port.
- A report that cannot be cleanly routed to a §13 disposition → revise §8/§9.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | A §8 property breaks in an anticipated-to-be-used module, via an in-scope adversary and input. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 break, but a §11 misuse is too easy and the module elects to harden it. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of infra the model trusts (Kafka/ZK/KMS) or of operator config. | §6/§3 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Needs JVM/heap, host, or infra compromise. | §7 |
| `OUT-OF-MODEL: non-default-config` | Needs an unhardened operator config (unrestricted `/admin/encrypt`, exposed 8090, unsecured Kafka) — pending the §14 ruling. | §5a |
| `OUT-OF-MODEL: unsupported-component` | Lands in `gatling-*`, tests, scripts, or user-supplied `KeySupplier`/`KmsClient`. | §3 |
| `OUT-OF-MODEL: solr-core` | Really an `apache/solr` search-server finding. | §3 |
| `BY-DESIGN: property-disclaimed` | AES-CTR non-integrity, keys-in-memory, Kafka-wire-is-operator's, "it's experimental." | §9 |
| `KNOWN-NON-FINDING` | Matches §11a. | §11a |
| `MODEL-GAP` | Unroutable — including any finding that turns on the unresolved scope line (Q-line). | triggers §12 |

## §14 Open questions for the maintainers

**Wave 1 — load-bearing (the scope line).**

- **Q-line.** *(Proposed answer to confirm/correct.)* Where is the line between
  "experimental, use at your own risk = out of model" and "a real vuln in a
  module you intend people to build and use = in model"? Proposed: a concrete,
  exploitable defect in `encryption/` or `crossdc-*` that breaks a §8 property,
  reachable by an in-scope adversary (§7), **is** `VALID` — because the PMC
  anticipates people building and using these modules; the "experimental" label
  lowers *assurance/expectation* but does **not** blanket-exempt real vulns.
  Purely meta observations ("unreleased", "less-reviewed", "unsigned") are
  out of model (§3). Confirm, and name any module you consider *fully* at-own-
  risk (fully out of model). (§1/§3/§13.)
- **Q-encrypt-authz.** Is "only an admin can trigger (de)cryption via
  `/admin/encrypt`" a property the module expects the operator to enforce via
  Solr authz (so an unrestricted handler is `OUT-OF-MODEL: non-default-config`),
  or should the module itself guard the `no_key_id` decrypt (so an unrestricted
  decrypt is `VALID`)? (§5a/§8.4/§11a.)
- **Q-consumer-port.** Are the CrossDC Consumer's `/metrics` and especially
  `/threads` (thread dump) endpoints expected to be unauthenticated and
  operator-isolated (so exposure is `OUT-OF-MODEL: non-default-config`), or
  should the Consumer restrict them? (§5a/§9.)

**Wave 2 — the security-critical crypto core.**

- **Q-crypto.** Confirm the encryption module's claimed properties (§8.1–8.3):
  per-file random IV/nonce uniqueness, key-secret-never-persisted, KMS cache
  wiping, and that AES-CTR is applied correctly to Lucene's write-once files.
  Which of these do you want treated as a claimed `VALID`-eligible property vs a
  best-effort experimental behaviour? (§8/§9.)
- **Q-integrity.** Confirm AES-CTR is confidentiality-only and index integrity/
  tamper-resistance is explicitly **not** claimed (Lucene checksums ≠ MAC), so
  such reports close `BY-DESIGN`. (§9.)
- **Q-keysupplier.** Confirm the user-implemented `KeySupplier`/`KmsClient` and
  the KMS are out of model (user code / infra). (§3.)

**Wave 3 — CrossDC surface, infra, and coexistence.**

- **Q-infra.** Confirm Kafka and the ZooKeeper holding `crossdc.properties` are
  trusted infrastructure the operator secures (TLS/SASL/ACLs), so wire-level
  findings are `OUT-OF-MODEL: trusted-input`. (§3/§9/§10.)
- **Q-crossdc-integrity.** Is any cross-DC replication guarantee (delivery,
  ordering, dead-letter) a *security* property, or purely correctness with the
  documented `collapseUpdates`/`expandDbq`/`indexUnmirrorableDocs` caveats?
  (§8.5.)
- **Q-solr / Q-scope.** Confirm the sandbox plugins assume and inherit
  `apache/solr`'s supported posture, and that `gatling-*`/tests/scripts are out
  of scope. (§2/§3/§5.)
- **Q-doc.** Confirm the disclosure channel (`security@solr.apache.org`) and
  this document's proposed **canonical, scanner-facing** role for the sandbox,
  cross-referencing `apache/solr`'s model rather than duplicating it. Where
  should `THREAT_MODEL.md` / `SECURITY.md` / an `AGENTS.md` `## Security` section
  live in this repo? (§1.)

## §15 Appendix — document roles and cross-reference

This `THREAT_MODEL.md` is proposed as the sandbox's **scanner-facing** security
model. It deliberately does **not** restate the Solr search-server trust model —
that lives in `apache/solr`'s `THREAT_MODEL.md` and is authoritative for
everything the sandbox plugins run *inside*. This document covers only what is
*different* about building and running **unreleased, experimental** sandbox
modules: lower assurance, build-from-source provenance, and the module-specific
surfaces (index encryption crypto + `/admin/encrypt`; CrossDC Producer plugins,
the Consumer's HTTP port, and the Kafka/ZK coupling).

Cross-references:

- **Solr search-server threat model** — `apache/solr` `THREAT_MODEL.md`
  (query/update/admin surface, auth/authz, SSRF, risky-feature/RCE, the
  trusted-environment deployment contract).
- **Sibling companion-repo models** (same round) — `apache/solr-operator`
  `THREAT_MODEL.md`, `apache/solr-mcp` `THREAT_MODEL.md`.
- **Module documentation** (the `(documented)` sources here) —
  `README.md`, `ENCRYPTION.md`, `CROSSDC.md`.
- **Securing Solr / advisories** —
  <https://solr.apache.org/guide/solr/latest/deployment-guide/securing-solr.html>,
  <https://solr.apache.org/security.html>.

**Provenance note.** The only *(maintainer)* claim in this v0 is the Solr PMC's
2026-06-04 statement that the sandbox is not released directly but is anticipated
to be built and used. Every other non-*(documented)* claim is *(inferred)* and
routed to a §14 question for the Solr PMC to confirm, correct, and own.
