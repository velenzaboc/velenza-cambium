# Agent-first design principle

This tool exists for **agent use**, not human CLI use. Every design choice gets evaluated against "can an agent use this effectively?" first, "is this convenient for a human shell user?" second.

## What that means concretely

1. **Every operation an agent might need is an MCP tool**, including `ingest`. Agents reviewing a PR ingest the branch themselves — they don't ask a human to run `code-graph ingest <path>`.
2. **Long-running operations stream progress** via MCP `notifications/progress`. An agent waiting 10 minutes on a silent tool call is a broken UX; an agent watching a `embedding 1200/3609` stream is fine.
3. **Discovery tools exist** so agents don't guess. `code_graph_list_commits` answers "what's already ingested?" before an agent runs `diff` or `ingest`.
4. **Error messages explain the next action**, not just the failure. "commit X not ingested — call code_graph_ingest first" is correct; "no rows" is not.
5. **CLI subcommands mirror MCP tools 1:1** so a human can reproduce anything an agent does.

## The shape of an agent session using this tool

```
agent: code_graph_list_commits(repo="velenza-cambium")
       → ["8887ad24...", "c3a404e9..."]

agent: code_graph_diff(repo="velenza-cambium", before="c3a404e9...", after="8887ad24...")
       → {added: [...], removed: [...], modified: [...50 entries...]}

agent: for each modified FQN:
         code_graph_neighbors(fqn=X, depth=2)
         → {inbound impact list}

agent: code_graph_get(fqn=X)
       → full source

agent: now writes the PR review
```

The agent never invokes `git`, never reads files directly, never types in a shell.
