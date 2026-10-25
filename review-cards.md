VERDICT: changes-requested

FINDINGS:

- [BLOCKER] internal/storage/wal.go:101 | Election Safety; durable directory creation | `MkdirAll` creates `wal`, but subsequent `syncDir(w.dir)` syncs its contents, not the parent entry naming `wal`. `snapshot.go:41` has the same gap for `snap`; newly created ancestor directories also lack durability barriers. | Concrete interleaving: A and B vote for A in term 1; A becomes leader. B loses power, and its unsynced parent directory entry for `wal` disappears despite the vote record’s file fsync. B restarts with empty HardState and grants C a term-1 vote; C plus B elect C while A remains term-1 leader. Add first-start power-loss tests with durable parent directories and independently crashable child-directory creation.

- [MAJOR] internal/storage/disk.go:325 | Snapshot recovery; vote eligibility | Applying a mismatching snapshot discards the suffix only in memory; reopening at `disk.go:101` resurrects it. T3.11’s vote regression is real by code inspection. | Five-node sequence: A/B/C commit `(5,2)`; D retains an uncommitted term-1 log through 10, E through 12. A installs snapshot `(5,2)` on D. Before restart, D rejects E’s higher-term RequestVote advertising `(12,1)`; after restart, D recovers `(5,2), (6..10,1)` and grants it because `isUpToDate` compares against `(10,1)`. This establishes an invalid local vote regression. It does **not** establish that E can obtain a majority: A/B/C still reject E. Add this reachable sequence without claiming a proven Leader Completeness violation from that vote alone.

- [MAJOR] docs/tasks/T3.11.md:63 | Correct negative-test oracle | The expected `ConflictIndex == 11` is wrong for `prev=(8,2)` against the untouched recovered log. `log.go:119-121` applies only when `prevIdx > lastIndex`. At index 8, the stored term is 1, so `conflictHint` returns `(6,1)`. | Use separate fixtures: untouched stale tail gives `(6,1)`; after replacing 6..10 with entries 6..7 at term 2, `prev=(8,2)` gives `(8,0)`. Requesting beyond untouched index 10 gives `(11,0)`.

- [MAJOR] docs/tasks/T3.11.md:65 | Commit-test isolation | If test 3b first appends and commits 6..7, the following heartbeat at `(5,2)` cannot reduce commitIndex. Consequently, `commitIndex <= 5` can fail on correct code. Also, a higher-term rejected vote clears an existing vote through `becomeFollower`; “VotedFor unchanged” requires an initial `None` vote or comparison after the term transition. | Give each heartbeat, append, rejection, and vote case a fresh recovered fixture with explicit HardState and leader commit values.

- [MAJOR] docs/tasks/T3.12.md:40 | Regression test must become green | Test 1 requires segment 1 to contain unsynced bytes before crashing. The proposed fix makes those bytes durable, so that assertion necessarily fails after the fix. | Keep `Unsynced > 0` as a baseline demonstration only. The permanent regression test must run the same workload and crash request against both versions, allowing no unsynced bytes after the fix.

- [MAJOR] docs/tasks/T3.10.md:36 | Isolation of compaction durability | The helper never explicitly makes its initial HardState durable under `SyncNever`. Before T3.12, rolling does not do so. Thus the proposed failure can reflect ordinary permitted unsynced-data loss rather than destruction of a durable HardState copy. Earlier segments may also tear before compaction begins. | Explicitly sync setup data, verify durable `{5,2}`, then inject crashes only during compaction. Test the newer `{6,3}` replacement separately. Test 5 under `SyncNever` is red before this fix because the targeted sync does not exist; T3.9 alone cannot make it green.

- [MAJOR] docs/tasks/T3.8.md:23 | Faithful crash model | The model omits directory-inode creation durability, durable truncation/recovery semantics, and handle/lock invalidation on crash. Its torn-tail formula assumes append growth and does not define behavior when `live` is shorter than `durable`. | Add model tests for parent-directory loss, truncate then crash, truncate/fsync then crash, and crash during repair followed by a second crash. Explicitly invalidate old file handles and release simulated process locks.

- [MAJOR] internal/storage/wal.go:350 | Acknowledged WAL records must remain replayable | Entry writes have no size check corresponding to `readRecord`’s 64 MiB limit. `AppendEntries` can fsync and return success for an oversized record that reopening rejects. Snapshot writes already check their size at `snapshot.go:97`. | Append an entry whose encoded body exceeds `maxRecordSize`, then reopen. Require rejection before writing, with existing WAL bytes unchanged. None of these cards covers this failure.

- [MINOR] internal/storage/disk.go:269 | Error-path consistency | T3.9’s assertion that DiskStorage “never updates memory on failure” is too broad. Both snapshot methods update `s.entries` before `TruncateBefore`; compaction or purge can then fail. | Inject failure at each Remove, directory sync, and purge operation; assert the resulting memory/disk relationship and required node-stop behavior. Distinguish successful publication followed by cleanup failure from failure before publication.

- [MINOR] internal/storage/disk.go:275 | Snapshot retention and concurrent access | `DiskConfig.SnapshotsKept` is validated but both snapshot methods purge using `DefaultSnapshotsKept`. Separately, `Snapshot()` releases its read lock before `LoadAt`, allowing concurrent compactions to purge the selected snapshot. | Test configured retention values and pause `Snapshot()` between selecting metadata and loading the file while enough compactions run to purge it.

- [MINOR] docs/tasks/T3.6.md:18 | Accurate references and complete filesystem inventory | Incorrect or incomplete claims: WAL `Stat` is at **280**, not 281; omitted closes are at **282, 301, 307**; omitted snapshot directory sync is at **66**. Replacing `filepath.Glob` with `ReadDir` also changes error behavior: Glob ignores directory-reading errors, whereas ReadDir returns them. | Include these paths in the recording test and explicitly decide how the behavior-preserving requirement handles scanning errors.

- [MINOR] docs/tasks/T3.11.md:35 | Accurate references and conditional claims | `lastNewIdx` is calculated at `log.go:87-88`, not 82-84; successful reply MatchIndex is at `replication.go:130-135`, not 127-133; the “only lowers next” guard is at **193-195**, not 178-181. The rejection-hint description omits its `prevIdx > lastIndex` condition. | Correct the references and conditional wording.

- [MINOR] docs/tasks/T3.9.md:14 | Accurate failure description | `wal.go:396-400` returns only when `err != nil`; it does not independently detect `n < len(buf)`. T3.7’s `deployed_test.go:20` reference points to the regex, while the checking test starts at **24**. T3.7’s claim that no other Recorder implementations exist is false: `batchRecorder` and `applyRecorder` implement it in node tests. | Define `ShortWrite(k)` to return a non-nil error, and update both fake Recorders. Remaining explicit code line references checked in the cards identify the stated code or enclosing range.

- [MINOR] docs/tasks/T3.6.md:54 | Cross-platform verification | In `GOOS=linux GOARCH=amd64 go build ./... && go vet ...`, the environment assignments apply only to `go build`. The following vet runs with the host environment. | Repeat the GOOS/GOARCH assignments for each vet command; the Darwin command has the same issue.

TEST REVIEW:

Static assessment only; the proposed tests are not implemented in these cards.

| Card | Would the tests fail before the fix? |
|---|---|
| T3.6 | Filesystem AST guard: yes. Contract tests are new-interface tests, not behavioral regressions. The golden is useful only if captured independently before refactoring. |
| T3.7 | Bad CRC and oversize-tail cases: yes. Byte mutations producing `ErrTornRecord` already pass. CRC-valid undecodable records already refuse, so test 7 is green despite its red classification. Repair/reopen test 8 is already green when using an ordinary torn tail. Reporting tests require new API and initially fail to compile. |
| T3.8 | Intentionally characterization tests. Test 10 can fail on unsynced rolls, now assigned to T3.12 rather than T3.10. Restrict its layout or update dependencies. The shared checker must permit replaced uncommitted entries and snapshot-covered entries rather than demanding every historical storage append remain present. |
| T3.9 | Poison assertions should fail before implementation. Ordinary node-stop-on-proposal-storage-failure already works; the automatic-snapshot failure path supplies the meaningful red node test. Test 5 is explicitly green, contradicting “each fails.” A crash simulator that already rejects every later operation cannot prove WAL poisoning. Use recoverable one-shot I/O faults for that assertion. |
| T3.10 | Tests 1–2 need the durable setup described above. Controls should remain green. Test 5 is red under `SyncNever` before the new sync exists. |
| T3.11 | Tests 1–2 characterize current behavior. Vote `(12,1)` is granted; `(7,1)` and `(4,1)` are rejected. Controls `(5,2)` and `(6,2)` are granted with fresh eligible-voter fixtures. Match/commit tests need corrected oracles. Tests 4–5 can pass today. Test 6 must assert that a mismatching snapshot was actually installed and restart occurs before suffix repair. |
| T3.12 | Tests 1, 2 and 4 can expose the missing old-segment sync, but test 1 cannot turn green as written. Sync-failure test 5 is also red before implementation, contrary to the commit-plan classification. Tests 3 and 6 are controls. |

Additional missing or incorrect test expectations:

- **T3.11 matchIndex:** On fresh stale-tail fixtures, heartbeat `prev=(5,2)` returns **5** and caps commit advancement at 5. Appending term-2 entries 6..7 returns **7** and replaces the stale suffix. Rejection processing cannot increase `next`, because of `backoff`’s explicit comparison. These paths do not inflate matchIndex from the stored tail.
- **T3.7 zeroing:** Zeroing an already-zero byte may leave a record unchanged. Ensure each mutation changes bytes; size the final record so zeroing 4096 bytes does not unintentionally destroy the asserted preserved prefix.
- **T3.7 framing:** Add a damaged length that produces `ErrTornRecord` while a valid later record exists. That specifically catches the current unconditional torn-tail truncation.
- **T3.7 fuzzing:** Arbitrary trailing bytes can encode valid records. Requiring recovery to contain only a prefix of the original entries is then incorrect. Constrain the generator or include valid trailing records in the model.
- **Repair durability:** Ordinary reopen tests do not prove the truncate was fsynced. Add power loss after each repair operation, then reopen and crash again.
- **Explicit Sync:** Under current `SyncNever`, `WAL.Sync()` syncs only the active segment; older rolled segments remain unsynced. Add a roll/Sync/power-loss test alongside T3.12.
- **Nonmonotonic names:** T3.11’s proposed layout is useful, but “extra segments only” remains a hypothesis until its model tests execute.

NOT VERIFIED:

- Tests could not execute. `go test ./internal/storage ./internal/raft` failed before running tests: `go: creating work dir: mkdir …: operation not permitted`.
- No race, fuzz, cross-platform, chaos, guard, or mutation checks ran.
- No `RUN_REPORT.md` was found; no runner output or implemented fix diff was supplied. Red-before-fix results and commit ordering remain unverified.
- Physical filesystem crash behavior and the proposed faultfs implementation remain unverified. The stale-tail vote sequence above proves a local vote regression by inspection, not a majority election or State Machine Safety violation.