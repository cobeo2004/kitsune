# Commit Rules

Use semantic commit messages for every commit.

Format:

```text
<type>(<scope>): <subject>
```

`<scope>` is optional. Keep the title short and concise, written in present tense.

Example:

```text
feat: add hat wobble
^--^  ^------------^
|     |
|     +-> Summary in present tense.
|
+-------> Type: chore, docs, feat, fix, refactor, style, or test.
```

## Types

- `feat`: a user-facing feature.
- `fix`: a user-facing bug fix.
- `docs`: documentation-only changes.
- `style`: formatting-only changes with no production behavior change.
- `refactor`: production-code restructuring without behavior change.
- `test`: adding or refactoring tests with no production behavior change.
- `chore`: maintenance tasks with no production behavior change.

## Description

Keep the description short and focused on achievements and metrics. Mention what changed, why it matters, and the verification result when useful.

## Boundaries

- Do not use `Co-authored-by` in commits or pull requests.
- Keep commits focused and reviewable.
- Ideal commit size is 1 to 5 files.
- More files are acceptable when the change needs it, but the current maximum threshold is 7 files.

## References

- https://www.conventionalcommits.org/
- https://seesparkbox.com/foundry/semantic_commit_messages
- http://karma-runner.github.io/1.0/dev/git-commit-msg.html