# The rule pack

Every file in this directory is one convention. This directory is the only
place a convention is written down. Three things read it:

```
rules/*.yaml ──┬─→ .cursor/rules/*.mdc   the Cursor agent's context   (generated)
               ├─→ docs/RULES.md         what a human reads           (generated)
               └─→ tools/lint_conventions.py  the linter        (reads YAML directly)
```

`python tools/gen_rules.py --check` fails if the generated targets have drifted
from the YAML. That check is what turns "one source of truth" from a claim into
a property — without it, someone tightens the linter, nobody regenerates, and
the agent starts confidently teaching last quarter's convention. That failure
mode is silent, which is what makes it the dangerous one.

## Adding a rule

Copy the nearest existing file, change the fields, run `python tools/gen_rules.py`.
No code change is needed unless you need a detector kind that does not exist yet.

Then add a fixture to `tools/test_lint_conventions.py`. This is not optional:
`test_every_enforceable_rule_is_covered` fails if a rule carries a detector and
has no fixture proving it fires. A detector nobody has watched fire is a rule
nobody is enforcing.

## Schema

| Field | Required | Notes |
| --- | --- | --- |
| `id` | yes | `PACK-NNN`. Must be unique across the directory; a duplicate is a hard error. |
| `pack` | yes | Groups rules into one generated `.mdc`. New packs need no code change. |
| `title` | yes | One line, imperative. |
| `severity` | yes | `error` (exit 1) or `advisory` (reported, does not fail the run). |
| `owner` | yes | A person. A rule without an owner is a rule nobody will retire. |
| `applies_to` | yes | Glob list. Also becomes the `.mdc`'s auto-attach `globs`. |
| `detector` | yes | A mapping with a `kind`, or `null` for advisory-only. |
| `message` | yes | What the engineer sees when it fires. |
| `rationale` | no | Why the rule exists. The opening goes to the agent; all of it goes to the docs. |
| `evidence` | no | How we know. See below. |
| `replacement` | no | The corrected code. |
| `references` | no | Where to read more. |

`applies_to` globs support `**`. They are matched against repo-relative paths by
a proper translator, not `fnmatch` — `fnmatch` treats `**` as `*`, which would
make `providers/**/*.py` silently skip every nested file.

## Detector kinds

Nine, all AST- or path-based, all offline.

| Kind | Config | Fires when |
| --- | --- | --- |
| `forbidden_call` | `attr` | `<any>.attr(...)` appears |
| `forbidden_kwarg` | `attr`, `kwarg` | `<any>.attr(..., kwarg=...)` appears |
| `import_location` | `symbols` | a symbol is imported from a module other than its real exporter |
| `require_import` | `module`, `name` | the module never imports `name` from `module` |
| `require_header` | `contains` | the first 2 KB, whitespace-normalised, lacks the string |
| `require_class_attr` | `base_suffix`, `attr` | a class whose base ends in `base_suffix` omits `attr` |
| `paired_call` | `requires`, `pair` | `requires` appears in the module and `pair` never does |
| `path_mirror` | `src_root`, `test_root`, `prefix` | no mirrored test file exists |
| `registry_sync` | `registry`, `src_root` | the module's dotted path is absent from the registry file |

**An unknown kind is a hard error, exit 2.** A typo that silently disabled a
check would produce a linter reporting clean while checking nothing, which is
strictly worse than no linter.

Some of these are deliberately coarse. `paired_call` proves the author thought
about teardown, not that teardown runs on the failure path — that would need
control-flow analysis. `forbidden_call` matches on the attribute name without
resolving the receiver's type. In both cases the cheap version catches the real
defect in the real codebase, and the expensive version does not exist. Coarse
and shipped beats precise and unwritten; the limitation is written down here
rather than discovered later.

## Not every rule has a detector

`KAFKA-004` is advisory with `detector: null`, on purpose. A pack that pretends
every convention is mechanically checkable is lying about its own coverage.
`lint_conventions.py --summary` prints advisory rules separately from enforced
ones, and prints rules it could not evaluate in this checkout at all, so the
coverage number means something.

## Evidence discipline

`evidence:` records how we know a rule is true, and the distinction it draws is
load-bearing: **reproduced against a live broker** is not the same claim as
**read in a changelog**.

`KAFKA-001` is the case that makes the point. `list_groups()` is deprecated, and
that deprecation is invisible to every check a normal team already runs — the
docstring does not mention it, the upstream tests are mock-based so they stay
green forever, and against a real broker the call returns real data and nothing
fails. It is observable only as a runtime `DeprecationWarning` on a live call at
the declared dependency floor. So the evidence for that rule is a transcript
from the INT VM, including the negative result
(`list_groups.__doc__ mentions "deprecat*": False`), not a citation.

`KAFKA-003` came from the opposite direction: a bug in *our own* tooling, where
`list_topics(topic=X)` created the topic it was checking for. That is the
intended lifecycle — a defect found once becomes a detector so it cannot be
found twice.

## Ownership and decay

Every rule names an owner and cites its evidence, so a rule can be audited and
retired rather than accumulating forever. The failure mode for a convention pack
is not being wrong on day one; it is being right on day one and unmaintained on
day two hundred. `--check` in CI, per-rule owners, and a fixture per detector are
the three things holding that off.
