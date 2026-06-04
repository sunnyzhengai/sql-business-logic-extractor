"""Corpus search -- one HTML, unified entity + text search.

Implements Phases 1 + 2 of the design mockup at
`docs/mocks/corpus_search_mockup.html`:

  - Phase 1 (entity-first table search): typing a table name returns
    every view/proc that touches it.
  - Phase 2 (multi-field text search): typing any term hits across
    view name, view description, column names, filter expressions
    (raw SQL + English translation), inline comments, and ZC-lookup
    names (e.g., typing "Active" matches a view whose filter is
    `STATUS_C = 1` because zc_values.csv resolved 1 -> "Active").

Phase 3 (CLARITY_EDG code-to-meaning resolution beyond ZC) and
Phase 4 (Caboodle/Registry naming-pattern badges) are deferred to
follow-up commits.

Unified search per Sunny's call: one input box, the engine routes
by best match.  Results are GROUPED BY where the match was found
so a developer sees not just what hit but why -- "in view name" vs
"in inline comment" vs "in ZC-lookup resolution" answers different
questions.

Implementation: build a JSON index from the corpus once, embed it in
the HTML as a `<script type="application/json">` block, and have a
small vanilla-JS filter run live on every keystroke.  No backend, no
build step, no dependencies -- works on a downloaded file as long as
the corpus has been indexed at write time.

Public entry points
-------------------
- build_search_index(views)           -> dict ready for json.dumps
- write_corpus_search(views, output_path, *, view_links=...)

The output HTML is self-contained and can be opened standalone if
view_links resolves to relative paths (default).
"""

from __future__ import annotations

import html
import json
from collections import defaultdict
from pathlib import Path

from tools.p50_present.community_matrix import _is_real_table_name


# ===========================================================================
# Index building
# ===========================================================================

def _bare_table(qualified: str) -> str:
    """Strip schema/brackets; reject SQL-fragment leaks. Same guard as
    view_shape / corpus_map / community_overview."""
    if not qualified:
        return ""
    bare = qualified.split(".")[-1].strip().strip("[]").strip()
    if not _is_real_table_name(qualified):
        return ""
    if not _is_real_table_name(bare):
        return ""
    return bare


def build_search_index(views: list[dict]) -> dict:
    """Build a JSON-ready search index from corpus views.

    The result is structured for the search UI's grouped results:

    {
      "tables": [
        {"name": "ARPB", "used_by_views": ["V_X", "V_Y"], "n_views": 2},
        ...
      ],
      "views": [
        {
          "name": "V_AR_AGING",
          "description": "...",
          "tables": ["ARPB", "HSP_ACCOUNT"],
          "column_names": ["pat_id", "aging_bucket", ...],
          "filter_text": ["AGING_BUCKET_ID IS NOT NULL", "Aging bucket exists"],
          "comments": ["/* only open AR */"],
          "zc_lookups": ["Active", "Closed"]
        },
        ...
      ]
    }

    `views` may include both views and procs (they share the same
    ViewV1 shape after the parsing_rules CREATE-PROC strip). The UI
    doesn't distinguish them at the data level; it could badge them
    differently if a `kind` field is added in a later phase.
    """
    table_to_views: dict[str, set[str]] = defaultdict(set)
    indexed_views: list[dict] = []

    for v in views:
        name = v.get("view_name") or ""
        if not name:
            continue

        report = v.get("report") or {}
        # description: prefer the LLM/English business description;
        # fall back to the technical bullet-list.
        desc = (
            (report.get("business_description") or "").strip()
            or (report.get("technical_description") or "").strip()
        )

        tables: set[str] = set()
        column_names: set[str] = set()
        filter_text: list[str] = []
        comments: list[str] = []
        zc_lookup_names: set[str] = set()

        for scope in v.get("scopes") or []:
            for t in (scope.get("reads_from_tables") or []):
                bare = _bare_table(t)
                if bare:
                    tables.add(bare)
            for j in (scope.get("joins") or []):
                rt = _bare_table(j.get("right_table") or "")
                if rt:
                    tables.add(rt)
            for c in (scope.get("columns") or []):
                cn = (c.get("column_name") or "").strip()
                if cn:
                    column_names.add(cn)
            for f in (scope.get("filters") or []):
                expr = (f.get("expression") or "").strip()
                english = (f.get("english") or "").strip()
                if expr:
                    filter_text.append(expr)
                if english and english != expr:
                    filter_text.append(english)
                for comment in (f.get("inline_comments") or []):
                    if comment.strip():
                        comments.append(comment.strip())
                for zc in (f.get("zc_lookups") or []):
                    if zc.get("name"):
                        zc_lookup_names.add(zc["name"])

        for t in tables:
            table_to_views[t].add(name)

        indexed_views.append({
            "name": name,
            "description": desc,
            "tables": sorted(tables),
            "column_names": sorted(column_names),
            "filter_text": filter_text,
            "comments": comments,
            "zc_lookups": sorted(zc_lookup_names),
        })

    tables_list = [
        {
            "name": tbl,
            "used_by_views": sorted(used),
            "n_views": len(used),
        }
        for tbl, used in sorted(table_to_views.items())
    ]

    return {
        "tables": tables_list,
        "views": sorted(indexed_views, key=lambda d: d["name"]),
    }


# ===========================================================================
# HTML rendering
# ===========================================================================

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>{title}</title>
<style>
  body {{ font-family: sans-serif; margin: 24px; color: #333; background: #fafafa;
          max-width: 1100px; }}
  h1 {{ font-size: 22px; margin: 0 0 4px; }}
  p.meta {{ color: #666; font-size: 13px; margin: 0 0 18px; }}
  h2 {{ font-size: 15px; margin: 22px 0 8px; color: #555;
        border-bottom: 1px solid #e0e0e0; padding-bottom: 4px; }}
  .searchbox {{ background: #fff; border: 2px solid #2c7fb8; border-radius: 6px;
                 padding: 12px 16px; font-size: 16px; width: 100%;
                 box-sizing: border-box; margin-bottom: 12px; }}
  .help {{ background: #f1f5f9; border-left: 3px solid #94a3b8; padding: 8px 12px;
            font-size: 13px; color: #555; margin: 0 0 14px; }}
  .help strong {{ color: #2c7fb8; }}
  .result-group {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px;
                    padding: 12px 16px; margin-bottom: 10px; }}
  .group-header {{ font-weight: bold; color: #1a1a1a; margin-bottom: 8px;
                    font-size: 13px; display: flex; gap: 8px; align-items: baseline; }}
  .group-header .count {{ color: #888; font-weight: normal; }}
  .hit {{ padding: 6px 10px 6px 14px; margin: 4px 0; background: #fafafa;
          border-radius: 4px; border-left: 3px solid #2c7fb8; font-size: 13px; }}
  .hit .name {{ font-weight: bold; color: #1a1a1a; }}
  .hit .why {{ display: block; color: #666; font-size: 12px; margin-top: 3px;
                padding-left: 8px; border-left: 2px solid #ccc; }}
  .hit a.open {{ color: #2c7fb8; text-decoration: none; font-size: 12px;
                  margin-left: 8px; }}
  .hit a.open:hover {{ text-decoration: underline; }}
  mark {{ background: #fef3c7; color: #92400e; padding: 0 2px; }}
  code {{ background: #f1f5f9; padding: 1px 5px; border-radius: 3px;
          font-family: 'Monaco', 'Menlo', monospace; font-size: 12px; }}
  .empty-state {{ color: #888; font-style: italic; padding: 18px; text-align: center; }}
  .nav {{ margin-bottom: 14px; font-size: 13px; }}
  .nav a {{ color: #2c7fb8; text-decoration: none; margin-right: 14px; }}
  .nav a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="meta">{meta}</p>
<div class="nav">
  <a href="corpus_map.html">&laquo; Corpus landscape map</a>
</div>

<input type="text" id="q" class="searchbox" autofocus
       placeholder="Type any table, view, term, code description, or comment..." />
<div class="help">
  <strong>Unified search.</strong> Type anything -- table names, view names,
  description terms, column names, filter text, or English translations of
  ZC codes.  Results are grouped by <strong>where</strong> the match was
  found (table name, view name, description, column, filter, comment,
  ZC-resolution) so you can see why each hit is relevant.
</div>

<div id="results">
  <div class="empty-state">Type to search across {n_tables} tables and
    {n_views} views/procs...</div>
</div>

<script id="search-index" type="application/json">{index_json}</script>
<script id="view-links" type="application/json">{view_links_json}</script>
<script>
(function() {{
  var INDEX = JSON.parse(document.getElementById('search-index').textContent);
  var LINKS = JSON.parse(document.getElementById('view-links').textContent);
  var q = document.getElementById('q');
  var results = document.getElementById('results');

  function escapeHtml(s) {{
    if (s == null) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }}

  // Wrap every case-insensitive occurrence of `term` in `text` with <mark>
  // for visual highlighting, after html-escaping. `term` and `text` come in
  // raw (user input / corpus text). Returns the marked HTML string.
  function markHits(text, term) {{
    if (!text) return '';
    if (!term) return escapeHtml(text);
    // Escape regex special chars in the user's query.
    var esc = term.replace(/[.*+?^${{}}()|[\\]\\\\]/g, '\\\\$&');
    var safeText = escapeHtml(text);
    var re = new RegExp(esc, 'gi');
    return safeText.replace(re, function(m) {{ return '<mark>' + m + '</mark>'; }});
  }}

  function openLink(viewName) {{
    var url = LINKS[viewName];
    if (!url) return '';
    return '<a class="open" href="' + escapeHtml(url) +
           '" target="_blank">Open shape &raquo;</a>';
  }}

  function search(term) {{
    if (!term) {{
      results.innerHTML = '<div class="empty-state">Type to search across ' +
        INDEX.tables.length + ' tables and ' + INDEX.views.length +
        ' views/procs...</div>';
      return;
    }}
    var t = term.toLowerCase();

    // ----- Group 1: exact and partial table-name matches -----
    var tableHits = INDEX.tables.filter(function(tbl) {{
      return tbl.name.toLowerCase().indexOf(t) !== -1;
    }});

    // ----- Per-view match categorization. Each view can appear in
    // multiple groups if it matched in multiple ways. We track the
    // groups per-view so a developer can see every way their query
    // touched it. -----
    var byViewName = [];
    var byDescription = [];
    var byColumn = [];
    var byFilter = [];
    var byComment = [];
    var byZcLookup = [];

    INDEX.views.forEach(function(v) {{
      var nameHit = v.name.toLowerCase().indexOf(t) !== -1;
      if (nameHit) byViewName.push(v);

      var descHit = v.description &&
        v.description.toLowerCase().indexOf(t) !== -1;
      if (descHit) byDescription.push(v);

      var colMatches = v.column_names.filter(function(c) {{
        return c.toLowerCase().indexOf(t) !== -1;
      }});
      if (colMatches.length) byColumn.push({{view: v, cols: colMatches}});

      var filterMatches = v.filter_text.filter(function(ft) {{
        return ft.toLowerCase().indexOf(t) !== -1;
      }});
      if (filterMatches.length) byFilter.push({{view: v, hits: filterMatches}});

      var commentMatches = v.comments.filter(function(c) {{
        return c.toLowerCase().indexOf(t) !== -1;
      }});
      if (commentMatches.length) byComment.push({{view: v, hits: commentMatches}});

      var zcMatches = v.zc_lookups.filter(function(z) {{
        return z.toLowerCase().indexOf(t) !== -1;
      }});
      if (zcMatches.length) byZcLookup.push({{view: v, hits: zcMatches}});
    }});

    var totalHits = (tableHits.length + byViewName.length + byDescription.length +
        byColumn.length + byFilter.length + byComment.length + byZcLookup.length);
    if (!totalHits) {{
      results.innerHTML = '<div class="empty-state">No hits for "' +
        escapeHtml(term) + '".</div>';
      return;
    }}

    var html = '';

    // Group: Tables matching
    if (tableHits.length) {{
      html += '<div class="result-group">';
      html += '<div class="group-header">Tables matching <span class="count">' +
        tableHits.length + '</span></div>';
      tableHits.forEach(function(tbl) {{
        html += '<div class="hit">';
        html += '<span class="name">' + markHits(tbl.name, term) + '</span>';
        html += '<span class="why">Used by ' + tbl.n_views +
          ' view(s)/proc(s):</span>';
        html += '<span class="why">' + tbl.used_by_views.map(function(vn) {{
          return escapeHtml(vn);
        }}).join(', ') + '</span>';
        html += '</div>';
      }});
      html += '</div>';
    }}

    // Group: View name match
    if (byViewName.length) {{
      html += renderViewGroup('Match in view name', byViewName, term, function(v) {{
        return '<span class="why">View name contains "' + escapeHtml(term) + '"</span>';
      }});
    }}
    if (byDescription.length) {{
      html += renderViewGroup('Match in description', byDescription, term, function(v) {{
        return '<span class="why">' + markHits(v.description, term) + '</span>';
      }});
    }}
    if (byColumn.length) {{
      html += renderViewGroupHits('Match in column name', byColumn, term, 'col');
    }}
    if (byFilter.length) {{
      html += renderViewGroupHits('Match in filter expression / English', byFilter, term, 'filter');
    }}
    if (byComment.length) {{
      html += renderViewGroupHits('Match in inline comment', byComment, term, 'comment');
    }}
    if (byZcLookup.length) {{
      html += renderViewGroupHits('Match via ZC code-to-name resolution',
        byZcLookup, term, 'zc');
    }}

    results.innerHTML = html;
  }}

  function renderViewGroup(title, views, term, whyFn) {{
    var h = '<div class="result-group">';
    h += '<div class="group-header">' + title + ' <span class="count">' +
      views.length + '</span></div>';
    views.forEach(function(v) {{
      h += '<div class="hit"><span class="name">' + markHits(v.name, term) +
        '</span>' + openLink(v.name) + whyFn(v) + '</div>';
    }});
    h += '</div>';
    return h;
  }}

  function renderViewGroupHits(title, items, term, kind) {{
    var h = '<div class="result-group">';
    h += '<div class="group-header">' + title + ' <span class="count">' +
      items.length + '</span></div>';
    items.forEach(function(item) {{
      h += '<div class="hit"><span class="name">' + markHits(item.view.name, term) +
        '</span>' + openLink(item.view.name);
      var hits = item.cols || item.hits;
      hits.slice(0, 3).forEach(function(snip) {{
        if (kind === 'col') {{
          h += '<span class="why">Column <code>' + markHits(snip, term) + '</code></span>';
        }} else if (kind === 'filter') {{
          h += '<span class="why">Filter: <code>' + markHits(snip, term) + '</code></span>';
        }} else if (kind === 'comment') {{
          h += '<span class="why">Comment: <code>' + markHits(snip, term) + '</code></span>';
        }} else if (kind === 'zc') {{
          h += '<span class="why">ZC-resolved value: <code>' + markHits(snip, term) +
            '</code></span>';
        }}
      }});
      if (hits.length > 3) {{
        h += '<span class="why">...and ' + (hits.length - 3) + ' more</span>';
      }}
      h += '</div>';
    }});
    h += '</div>';
    return h;
  }}

  // Debounce so typing fast doesn't run the filter on every keystroke.
  var debounceTimer = null;
  q.addEventListener('input', function() {{
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(function() {{ search(q.value.trim()); }}, 80);
  }});
}})();
</script>
</body>
</html>
"""


def write_corpus_search(
    views: list[dict],
    output_path: str | Path,
    *,
    title: str = "Corpus search",
    view_links: dict[str, str] | None = None,
) -> Path:
    """Write the corpus_search.html artifact.

    Parameters
    ----------
    views : list of ViewV1 dicts (the full corpus).
    output_path : where to write the HTML.
    view_links : optional dict view_name -> relative URL to that
        view's shape HTML (typically
        "community_shapes/community_NN_<top>_shapes.html#view-<name>").
        Each search result with a matching view_name becomes a
        clickable "Open shape >>" link.
    """
    output_path = Path(output_path)
    index = build_search_index(views)
    index_json = json.dumps(index, separators=(",", ":"))
    view_links_json = json.dumps(view_links or {}, separators=(",", ":"))

    meta = (
        f"{len(index['tables'])} tables &middot; "
        f"{len(index['views'])} views/procs indexed."
    )
    html_body = _HTML_TEMPLATE.format(
        title=html.escape(title),
        meta=meta,
        n_tables=len(index["tables"]),
        n_views=len(index["views"]),
        index_json=index_json,
        view_links_json=view_links_json,
    )
    output_path.write_text(html_body, encoding="utf-8")
    return output_path
