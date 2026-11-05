# Provenance and evidence boundaries

Vedant Thaker's portfolio framing uses a decision-first narrative: explain the topic, locate a concrete failure, define a framework, run an experiment, and make the limits visible. The code, fixture, tests, and reports in this repository were created with Codex implementation assistance and reviewed through executable local checks. They are new portfolio work, not historical employer deliverables.

No customer, employer, applicant, financial, or private resume data is included. The numbers in `data/workflow.json` are invented for a deterministic protocol fixture. No paid service, GPU, or model API was used. Budget units are synthetic accounting caps, not observed provider spend.

Conceptual inspiration: [ECC by affaan-m and contributors](https://github.com/affaan-m/ECC), MIT license, pinned at `b38be6a7f6350964d59328b52f0624e1d1d552d1`.

- [continuous-agent-loop](https://github.com/affaan-m/ECC/blob/b38be6a7f6350964d59328b52f0624e1d1d552d1/skills/continuous-agent-loop/SKILL.md): bounded execution, persisted sessions, explicit stopping conditions.
- [eval-harness](https://github.com/affaan-m/ECC/blob/b38be6a7f6350964d59328b52f0624e1d1d552d1/skills/eval-harness/SKILL.md): declare acceptance criteria and separate capability checks from regression checks.
- [verification-loop](https://github.com/affaan-m/ECC/blob/b38be6a7f6350964d59328b52f0624e1d1d552d1/skills/verification-loop/SKILL.md): verify behavior through observable checks.
- [cost-aware-llm-pipeline](https://github.com/affaan-m/ECC/blob/b38be6a7f6350964d59328b52f0624e1d1d552d1/skills/cost-aware-llm-pipeline/SKILL.md): routing and bounded cost accounting as design concerns.

ECC was used as a reference for problems to investigate. Its agents, skills, installers, prompts, and code were not copied or executed. The SQLite task protocol, adapters, failure fixtures, and reports here are an original implementation. The project does not claim affiliation with ECC or that the experiments evaluate ECC itself.

Primary technical references: [SQLite transactions](https://www.sqlite.org/lang_transaction.html), [SQLite isolation](https://www.sqlite.org/isolation.html), and [Python sqlite3](https://docs.python.org/3/library/sqlite3.html).

The checked-in JSON records source SHA-256 digests and full logical transition traces. All reported numbers come from local deterministic experiments. The output does not establish production reliability, business lift, actual billing savings, model quality, or any result stated in a previous resume.
