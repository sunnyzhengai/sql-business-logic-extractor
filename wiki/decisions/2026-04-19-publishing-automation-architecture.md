---
date: 2026-04-19
status: proposed
---

## Context

Under the TIP strategy (`2026-04-19-thinking-in-public-strategy.md`), the content site needs a weekly Saturday publishing cadence with sustainable throughput. Yang wants automation for: (a) draft generation from the wiki knowledge base, (b) publishing to social media platforms, (c) capturing reader responses into a knowledge graph (JSON files) that feeds back into the wiki.

Full automation is neither feasible nor desirable. Some platforms (LinkedIn) don't expose reliable APIs for personal-profile posting or comment extraction. The final publish click should remain human to preserve voice and satisfy the TIP off-hours protocol. Response ingestion from LinkedIn is genuinely hard without TOS-violating scraping. The architecture therefore targets **semi-automation with a preserved human edit loop.**

## Decision

A three-tier pipeline:

1. **Automated:** draft generation (Claude pulls from wiki concepts + last week's feedback ingestion), static site build/deploy (git push triggers), poll-response capture (site endpoint → JSON in wiki/raw/), email reply routing (mail filter → wiki/raw/).
2. **Semi-automated:** Yang edits drafts in-place (wiki/drafts/); Claude iterates with examples; final polish pass is collaborative.
3. **Manual:** final publish click to personal site; LinkedIn post paste; Medium cross-post (if desired); LinkedIn comment ingestion (weekly screenshot → paste into wiki/raw/).

## Stack (proposed)

- **Domain:** personal-brand domain (NOT AIVIA); TBD, Yang to choose.
- **Site generator:** Astro (current sweet spot; markdown-first; good defaults).
- **Hosting:** Cloudflare Pages (free tier sufficient; CDN included; integrates with Cloudflare Workers for the poll endpoint).
- **Poll backend:** Cloudflare Worker with Durable Objects or simple KV store; responses POST to endpoint → on cron, write to wiki/raw/feedback/ as JSON via GitHub API.
- **Email list:** Buttondown (recommended over Substack for independence from a hosted platform; own the list data; clean export).
- **Source of truth:** git repo covering wiki/, drafts/, published/, and site source. Everything versioned.

## Weekly cadence

```
Mon    Claude generates draft from wiki + last week's feedback
       → wiki/drafts/YYYY-MM-DD-slug.md

Tue–Thu Yang edits in place; adds examples; sharpens voice
        Claude iterates on feedback

Fri    Final polish pass (collaborative)

Sat AM Yang publishes:
       - git merge draft → published/, push, site auto-deploys
       - LinkedIn post: Yang pastes manually, links to full piece + poll
       - (optional) Medium cross-post via API

Sun–Sat+6 Reader responses arrive continuously:
          - site poll → Worker endpoint → wiki/raw/feedback/polls/
          - email replies → filter → wiki/raw/feedback/email/
          - LinkedIn comments → weekly manual paste → wiki/raw/feedback/linkedin/

Next Mon  Claude runs ingestion pass (/ingest):
          - Processes wiki/raw/feedback/ → updates concept pages
          - Surfaces patterns: "N readers pushed back on X"
          - Feeds topic selection for the following week
```

## Alternatives considered

- **Fully automated publishing (LinkedIn included).** Rejected: LinkedIn API doesn't support reliable personal-profile auto-posting; third-party tools violate TOS or require workarounds that are fragile. TIP's off-hours manual posting protocol is also a *feature* (looks like genuine professional activity, not a bot).
- **Substack as primary venue.** Rejected: platform lock-in; list portability concerns; poll widgets are limited; can't co-locate polls with concept pages. Better as a downstream republication.
- **Medium as primary venue.** Rejected similarly: limited control; curation risk; not a durable list-building surface.
- **Hosted blog platform (Ghost, WordPress).** Rejected: operational overhead exceeds value for a weekly-cadence solo effort. Astro + Cloudflare Pages is nearly zero-ops.
- **Scraping LinkedIn comments for automated ingestion.** Rejected: TOS violation, fragile against LinkedIn UI changes, risk-vs-reward is terrible. Weekly manual paste is fine.

## Consequences

- **Zero AIVIA touchpoints in the public pipeline.** Personal-brand domain, personal email, personal byline. AIVIA doesn't appear anywhere in the public-facing stack. Reinforces TIP posture.
- **Git as the single source of truth.** Wiki, drafts, published articles, reader feedback — everything lives in one repo. Single backup target; complete history; no lock-in to any hosted service.
- **Feedback loop closes in 7 days.** Monday's ingestion reflects last week's readership. Topic selection for the next article is informed by what actually resonated, not just what Yang has queued.
- **Manual LinkedIn comment ingestion is the weakest link.** Weekly paste is acceptable at low volume; if the list grows past a few dozen active comments per week, reassess. Possible future workaround: ask respondents to also reply to an email address for a "guaranteed thoughtful response," which captures them in the ingestible channel.
- **Email list is the durable asset.** If LinkedIn or any other venue goes cold, the email list is Yang's portable audience. Pick a provider (Buttondown recommended) that allows clean CSV export without payment lock-in.

## Open questions

- Which domain? Yang to choose. Should avoid anything AIVIA-adjacent or employer-identifiable. Candidates: topical (sqlmeaning.com, datagovfork.com), personal (yang-zheng.com), or neutral-abstract. Check availability + defensiveness against domain-squatting.
- Does the poll widget need sophisticated anti-abuse protection, or is basic rate-limiting + naive duplicate detection enough for year-one traffic levels?
- What's the right trigger for Claude's draft generation — manual prompt, calendar-cron, or GitHub Action? Probably manual prompt at first (keeps the human in the loop for topic selection); automate later if cadence holds.
- How much of the first 8–12 weeks of articles should Claude pre-draft up front (buffer) vs. generate live from accumulated feedback (fresh)? Buffer of 2–3 articles ahead protects against missed Saturdays; beyond that, freshness is worth more.
