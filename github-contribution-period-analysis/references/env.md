# Environment and Auth

## Token Resolution Order
The script resolves authentication in this order:
1. `--token`
2. `GITHUB_TOKEN`
3. `GH_TOKEN`
4. `gh auth token`
5. Unauthenticated mode (public data only)

## Private Activity Coverage
- To include private repository activity, use a token with `repo` scope.
- If `repo` is missing, the report marks private coverage as unknown or limited.

## Recommended Setup

```bash
# Option A: env token
export GITHUB_TOKEN=<token-with-repo-scope>

# Option B: GitHub CLI login
gh auth login
```

## Notes
- SSH keys are useful for `git clone/fetch/push` but do not replace API token auth for this report workflow.
- Unauthenticated mode is supported for smoke tests but cannot cover private contributions.
