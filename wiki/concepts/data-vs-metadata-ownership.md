---
name: Data vs. metadata ownership
aliases: [who owns the data vs. who owns the meaning, two-layer ownership]
see_also: [governance-vs-compliance-layers, govern-authored-meaning, patient-a01-scene, governance-forks-catalog]
sources: []
---

## Definition

Owning **data** (records exist, are accurate, are retained, are complete) and owning **metadata** (the definitions that give those records meaning in a given workflow) are two different jobs with two different owners. Programs that silently assign one owner to both usually fail at one — typically at metadata, because metadata ownership isn't even recognized as a distinct role.

## Why it matters here

This is the foundational fork of the content site — the premise Yang flagged as "one of the earliest discussions." It sits above every other fork: until a reader accepts that data-ownership and metadata-ownership are distinct, every downstream governance question collapses into the wrong debate.

In healthcare concretely: HIM can legitimately own the Patient *data* (accuracy of demographics, record retention, duplicate resolution). But the *definition* of who counts as an "outpatient patient" for this report, in this workflow, under this measure — that authorship lives with the operational team that wrote the query. Forcing HIM to own both means the metadata is owned by no one in practice.

The SQL logic extractor operates entirely at the metadata layer. It cannot tell you who owns the data. It can show you who authored the definitions, because those live in code.

## Open questions

- What's the minimal RACI that holds data-owner and metadata-owner separately accountable but coordinated at the seams?
- In a catalog UI, how do we display two owners per asset without confusing users who've been trained there's one?
- Are there classes of assets (reference data, master data) where the two layers legitimately collapse into a single owner? Or is that an illusion that breaks under closer look?
