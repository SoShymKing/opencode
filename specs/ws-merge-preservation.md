# ws 병합 기능 보존 수용 기록

## 범위와 판정 원칙

- 현재 기준은 사용자가 commit한 merge `HEAD 50aca9ad1a93ca4d8214658a541a391800be7185`다. 부모는 `5573129a75`와 `eeec76bb62`다. 초기 271개 충돌 및 마지막 42개(A 26/B 12/C 2/D 2)의 해결·stage 기록은 merge 전 이력이다. 아래 2026-10-16 증거와 당시 index 상태는 현재 작업 트리 상태의 증명이 아니다.
- 사용자 방향: `2.x` runtime과 schema/정규화 part storage를 유지하며 ws 동작을 native 소유자에 통합한다. Schema → Core/Protocol → Server 의존 방향을 지키고 Client runtime은 Core/Server를 참조하지 않는다. 파생 상태를 durable event에 중복 저장하지 않는다.
- 아래 donor commit은 인계에서 확인된 출처다. 이 문서에서 commit을 독립 검증하지 않았으며, commit 존재나 충돌 표식 제거만으로 동작 보존을 인정하지 않는다. **충돌 해결과 기능 검증의 범위를 구분한 기록이며 전체 검증 승인 또는 과거 성능 동등성 판정이 아니다.**

## 필수 동작과 수용 증거

대상 소유자는 `2.x` 고유 계층의 책임 위치다. 실제 구현과 실행 증거를 확인한 뒤 상태를 갱신한다. 필요한 증거는 테스트 명령, 결과, 대상 참조, 재현 조건을 포함해야 한다.

| 필수 ws 동작 | 인계에서 확인된 donor commit | 대상 고유 소유자 (`target native owner`) | 필수 수용 증거 (`required acceptance proof`) | 현재 상태 |
| --- | --- | --- | --- | --- |
| part storage 및 성능 보존 | `8504130e23`, `0a8c0a157b` | Core DB/storage | 격리 DB 저장·조회·재시작 내용 일치; 동일 입력의 읽기·쓰기 비용 비교 | 부분 검증: 저장/fixture 통과, 성능 비교 차단 |
| 안전한 DB export/import | `9e603e4e0b` | Core DB/storage | 격리 왕복·무결성, 미지원 입력의 변경 전 거부 | 지원 형식의 자동 변환 구현·검증 완료. 아래 2026-10-17 지원 범위와 안전 조건 적용 |
| history 증분 처리 | `31a85fe3e2` | Core storage/runner | 항목 추가 후 누락·중복 없는 증분 반영 | native history/Runner/cache 기능 검사 통과; 매 Step DB reload 유지 |
| 불필요한 clone 방지 | `fc9f2c7384` | Core runner | 큰 history 반복 step의 할당·실행 비용 비교 | native drain cache/선택적 hook 복사 검증; 전체 비용 동등성 미측정 |
| tool cache 및 shell progress | `a13ab66cd0`, `0d696e377e` | Core runner, AI, App/TUI/session-ui | cache 정합성 및 UI 진행 이벤트·종료 정리 | native converter cache와 ShellID 진행 검증; V1 tuple/metadata API 호환 주장 없음 |
| 무응답 복구, main 제한, 일반 retry | `ee4e063720`, `da395f2eeb`, `2a3abb3d69`, `bfe2f86d68` | Core runner, AI | 재시도 횟수·종료 조건·중복 출력 방지 | native post-tool deadline MAIN 10/CHILD 1, 일반 timeout 3 및 중단 검사 통과 |
| raw frames, 최종 counts, activity | `8f0767727f`, `e7ed2c1aa9`, `e14da51e90`, `e738ea8832` | AI, Schema/Protocol/Server/Client Solid, App/TUI/session-ui | 실제 stream의 경계 간 raw frame/count/activity | AI/TUI/browser 및 response 이전 SSE 구독 검사 통과; 기존 HOLD 해결 |
| 취소 및 restart 복구 | `74c7fccd0c`, `2e7f644e00` | Core runner, App/TUI/session-ui | 반복 취소·유실·중복 방지와 busy 해제 | native 반복 취소/서버 lifecycle 검증; 공유 서버 종료나 V1 worker/header 재현 없음 |
| archive 복원 및 세션 목록 기간 유지 | `4ad6accf33`, `24454b6973` | Core storage, Schema/Protocol/Server/Client Solid, App/TUI/session-ui | API/UI 복원 및 `session_list_request_window_days` 기본 `30`/`all` 영속성 | native API/SDK/TUI 및 includeArchived 혼합 pagination 검증; 기존 HOLD 해결 |
| reconnect history, snapshot, pending, owner | `8286136adc`, `3edce9c4c6`, `34c757bff6`, `b3ca021989`, `6925341864` | Core runner, Schema/Protocol/Server/Client Solid, App/TUI/session-ui | 재연결 history/snapshot, pending/owner 보존 및 중복 방지 | Client/실제 WS/browser, native 실행 소유권·취소 검사 통과 |
| attachment controls | `827dc14934`, `c9869929a7`, `37c2b2bbe7` | Schema/Protocol/Server/Client Solid, App/TUI/session-ui | 첨부 조작·제출 내용 및 거부 참조 격리 | 부분 검증: native controls/quarantine 통과, opaque 출처 한계 유지 |
| thinking 현지화 | `54f3951c3e` | App/TUI/session-ui | 해당 언어의 실제 표시·상태 전환 | 부분 검증: 한국어 browser/TUI, 기존 번역 768개 checksum 보존 |

## 현재 수용 범위와 미확인 조건

- **지원 범위의 일반 ws 자동 변환은 구현·검증되었다.** 아래 지원 표와 보존 조건을 적용한다. 미지원 입력, fingerprint 불일치, ID 충돌 및 설명되지 않는 history prefix는 변경 전에 거부한다. 원본을 파괴하는 migration이나 임의 형식 지원을 승인한 것은 아니다.
- 병합 전 UI benchmark 시도는 historical ws `HEAD` build에서 `@opencode-ai/protocol/groups/session`을 찾지 못해 실패했다. 유효한 historical baseline은 없다. 현재 production build 성공이나 단일 진단 표본으로 과거 성능 보존을 주장하지 않는다.
- 초기 환경 인계는 `Effect 4.0.1`의 `ignore-scripts` 설치와 실행 Bun `1.4.1`/고정 `1.4.2` 차이를 기록했다. 이를 아래 실제 실행 결과의 실패 원인으로 단정하지 않는다.

## 증거 갱신 규칙

주 통합 담당자의 직접 확인과 작업자 보고를 구분한다. 아래 묶음은 서로 겹치므로 합산하지 않는다. 실패 또는 미실행 범위를 수용하지 않으며 부분 검증만으로 전체 기능 검증이 통과했다고 판정하지 않는다. 2026-10-17 결과는 이번 통합 작업의 증거이며, 후속 수정 전후를 구분한다. 최종 읽기 전용 검토의 두 차단 사항은 아래 수정·재검증으로 닫았다.

## 2026-10-17 자동 변환 수용 기록

### 지원 입력과 보존 계약

| 입력 계열 | 구현·검증 범위 |
| --- | --- |
| compact ws | v01 inline 및 v02 parts |
| expanded ws | 38개 계열, metadata alias, split ledger 및 historical part 포함 |
| previous native | 전체 57개 migration 이력과 구조가 일치하는 이전 native 형식 |
| current native / V1 | 회귀 검사 포함. 활성 native migration은 timestamp ID 52개이며 `20261017041333_ws_native_imports` 포함 |

- 원본의 모든 table/DDL, 알 수 없는 cell, raw JSON/BLOB, ledger/event는 변경 불가 INTERNAL archive로 남긴다. source ID·순서·시각·archive·fork·합계와 WS1/P2 root를 보존한다. 공개 raw-event SSE나 전역 persist 기본값을 새로 만들지 않는다. native tail은 import별 ancestry를 따라 유지한다.
- 정확한 frozen baseline은 현재 값과 구분한다. fork는 캡처된 부모 cursor를 사용하며, 지연된 prompt promotion timestamp도 유효하다. event log에만 있다고 가정하지 않고 projection에만 있던 source system notice도 복원한다.
- shell 기록의 알 수 없는 결과는 그대로 남기며 가짜 job/exit를 만들지 않는다. 빈 tool, raw hosted/provider map, 사용할 수 없는 URI 참조도 사실대로 유지한다. local worktree/folder는 `ws-local` native adapter와 명시적 Workspace ID를 사용한다. remote를 local로 추측하지 않는다. `WsNativeJson`은 호환되는 JSON 속성의 원래 순서와 raw token을 유지한다.

### 활성화 안전성과 검증 경계

- stage, backup, preimage, WAL 및 증거를 확보하고 exclusive 절차를 거친 뒤 lifetime lease를 유지한다. 기존 V1 marker도 별도 stage에서 승격한다. source-safety의 빈 WAL 오판은 일반 파일의 0-byte 동등성에만 한정하여 수정했다. 실패 artifact는 보존한다. crash 또는 활성화 실패 뒤 참조되지 않는 이전 stage가 남을 수 있는 한계는 유지한다.
- 실제 native WAL `BUSY` 오류 5는 migration 전에 WAL을 처리하고, 최대 5초의 `LockTimeout`만 제한적으로 재시도하도록 수정했다. Model retry는 변경하지 않았다. profile과 독립적인 V1 완료 경로이며 legacy gate에 의존하지 않는다. source lease는 공유하고 fingerprint/충돌은 실패 처리한다. 전역 event 삭제는 없다.
- hybrid `verifyClone`은 내부 DISPOSABLE 직렬화 clone에서만 실행한다. 현재 authority와 supplement 입력을 요구하고 scoped reset 및 native rowid 순서를 사용한다. 전체 log나 전역 rebuild를 검증했다는 뜻이 아니다. 설명되지 않는 prefix는 실패한다.
- 실제 UI/TUI는 정적 기록을 표시한다. 알 수 없는 결과를 확정하지 않고 live fetch를 하지 않는다. 새 fallback 문구만 영어다. 실제 사용자 DB/profile/model, 서비스 재시작, 설치는 사용하지 않았다. 실제 Server 검증도 격리된 합성 fixture에 한정한다.

### 이번 통합 작업의 실행 증거

| 확인 주체·범위 | 결과 | 해석 및 시점 |
| --- | --- | --- |
| 주 담당자 Core 11파일 묶음 | 175 통과, 1725 assertions | 최신 소규모 수정 전 결과 |
| 주 담당자 Core replacement/lease/fork/fold/rebuild 5파일 | 52 통과, 217 assertions | 이후 재검증 |
| 주 담당자 기존 Runner/첨부/transport 8파일 gate | 367 통과, 1816 assertions | 이번 DB 통합 후 재실행. 이전 결과를 단순 인용한 것이 아님 |
| 주 담당자 Server 실제 Fetch | 2 통과, 111 assertions | 실제 Runner + TestLLM, 요청 2회의 순서 및 reopen 후 중복 없음 |
| 주 담당자 source conversion 7파일 | 74 통과, 1031 assertions | 지원 입력 변환 |
| 주 담당자 message driver | PASS | native wire, ordinal, hosted raw, shell unknown |
| 주 담당자 이전 차단 4건 | 4 통과, 214 assertions | 아래 과거 BLOCKED 판정 대체 |
| 작업자 전체 migration 반복 | 29개 안정 통과, 5회 반복에서 150 통과 보고 | WAL 수정 후 보고값. 다른 묶음과 합산 금지 |
| 작업자 Schema / Protocol | 각각 11 통과·55 assertions / 5 통과·18 assertions | 작업자 실행 증거 |
| 작업자 web | unit 96 통과, browser 30 통과 | Edge, 폭 390/1280, LTR/RTL, live-shell 읽기 0. 증거는 `%LOCALAPPDATA%/Temp/opencode/recorded-history-proof/recorded-history-*.png`로 이동. 격리 Storybook/Edge 프로세스 종료 |
| 작업자 TUI | 101 통과, 기존 Windows skip 1; 반복 120회 | 실제 OpenTUI 폭 44/100. artifact는 `%TEMP%/opencode` 아래 |
| 주 담당자 화면 확인 | Edge에서 폭 390 RTL, 키보드 disclosure와 가로 overflow 없음, live API 요청 0 확인 | screenshot 및 OpenTUI terminal frame도 직접 검사. Chrome 부재로 설치된 Edge 사용, 추가 설치 없음 |
| 주 담당자 타입·빌드·migration | Core/Server/Client/TUI/Session UI typecheck PASS, Core build PASS, 공식 migration `--check` PASS | 루트 전체 검사 통과와 별개 |
| 주 담당자 최종 P 회귀 묶음 | 75 통과, 1223 assertions | P native/WS native/profile/message 4파일. 과거 step-start에 없는 `started`를 요구하지 않는 전용 codec 검증 |
| 주 담당자 최종 TUI 회귀 묶음 | 146 통과, 기존 Windows skip 1 | 기록된 assistant tool도 native dispatch·job 추적·output reader보다 먼저 정적 처리. 실제 폭 44/100 renderer 증거는 `%LOCALAPPDATA%/Temp/opencode/recorded-tools-{session,mini}-{44,100}.txt` |
| Client 공식 생성 재실행 | 재생성 전후 Git content hash 일치: Promise `b95ab7b8390a…`, Effect API `44babc4304f1…` | 생성 결정성 확인. `check:generated`는 index와 비교하여 의도한 미commit 생성 차이로 실패하므로 PASS로 기록하지 않음. Client typecheck 및 contract/import 경계 44건·170 assertions 통과 |

최종 Oracle 최초 검토는 P step-start 필드 차이와 TUI의 기록 tool live 취급 두 건을 차단 사항으로 지적했다. 실제 과거 P 계약의 전용 decoder와 TUI/mini/subagent/grouping의 정적 기록 분기를 추가하고 red-first 회귀 검사로 확인했다. 같은 검토자의 범위 제한 재검토는 **두 지적 모두 해소, 추가 구체적 지적 없음(PASS)**이었다. 검토자는 코드를 읽었으며 테스트를 재실행하지 않았다. 이는 루트 전체 검사나 임의 형식·실사용 DB의 보증은 아니다.

이번 변경에 `git add`/commit/push는 하지 않았다. 유효한 과거 성능 baseline이 없으므로 종단 성능 동등성은 판정하지 않는다.

## 2026-10-16 후속 통합 검증

다음은 2026-10-16 당시 주 통합 작업에서 직접 실행한 과거 결과다. 명령은 표시한 패키지 디렉터리에서 실행했다. 당시 Client `check:generated` 성공과 DB 실패는 현재 판정이 아니며, 위 2026-10-17 결과가 해당 범위를 대체한다.

| 패키지 | 명령 또는 확인 | 실제 결과 | 판정 범위 |
| --- | --- | --- | --- |
| Client | `bun run generate`, `bun run check:generated`, `bun typecheck` | 성공 | Git 환경을 적용한 공식 생성/일치 확인. 생성 코드를 수동 수정하지 않음 |
| Client | `bun test --timeout 5000 test/import-boundaries.test.ts test/contract-identity.test.ts` | 4 통과, 31 assertions | 생성 계약과 runtime 의존 경계 |
| Client | `bun test test/solid-refresh.test.ts test/solid-data.test.ts test/solid-inbox-race.test.ts test/solid-pending-status.test.ts` | 94 통과, 349 assertions | 실제 deferred REST/SSE의 asked/created·중복 fact, replied/cancelled/HTTP settlement tombstone, reconnect/evict/dispose, global form location 격리. metadata/content/live pre-read history 수정 포함 |
| Schema | `bun test test/event-manifest.test.ts test/event.test.ts test/session-error.test.ts test/session-terminal.test.ts` | 28 통과, 163 assertions | 이벤트 계약, 안전한 첨부 참조, terminal 직렬화 |
| Protocol | `bun test test/session-active.test.ts test/session-cursor.test.ts test/session-archive.test.ts` | 최신 5 통과, 31 assertions | active/cursor/archive 및 혼합 목록 계약 |
| Server | `bun test test/event-feed.test.ts test/sse-failure.test.ts test/session-archive.test.ts test/session-active.test.ts test/session-diff.test.ts`, `bun typecheck` | 최신 18 통과, 148 assertions; 타입 성공 | 이전 13/70 대체. bounded 4096/scoped fanout 및 실제 response 이전 구독/웹 body 검사. 기존 session-log `sse-failure`만으로 이를 주장하지 않음 |
| Client | `bun test test/promise.test.ts test/import-boundaries.test.ts test/contract-identity.test.ts` | 최신 44 통과, 170 assertions | 생성 전 includeArchived wire 누락 실패 확인 후 공식 generate/check:generated 통과 |
| SDK | `bun test test/session-archive.test.ts` | 1 통과, 7 assertions | 현재 SDK 보관/복원 연결 |
| AI | `bun test --timeout 30000 test/stream-observation.test.ts test/provider-error.test.ts`, `bun typecheck` | 50 통과, 100 assertions; 타입 성공 | raw frame 관측 및 provider 오류 분류 |
| Core | `bun run script/test.ts test/database-lineage.test.ts test/database-native-parts.test.ts test/database-artifact.test.ts` | 13 통과, 34 assertions | lineage 거부, native part 변환, 백업 artifact |
| Core | `bun run script/test.ts test/database-legacy-v01.test.ts test/database-legacy-v01-failure.test.ts` | 20 통과, 75 assertions | 지원하는 구 ws 형식의 격리 export/import. native 자동 이전 성공은 아님 |
| Core | `bun run script/test.ts test/database-replacement.test.ts --test-name-pattern "Standalone ws replacement"` | 6 통과, 9 filtered out, 30 assertions | 독립 replacement 복구/실패 보존만 검증 |
| Core | `bun run script/test.ts test/database-migration.test.ts` | 당시 25 통과, 4 실패, 129 assertions | 과거 자동 변환 실패. 현재는 위 재검증으로 대체 |
| Core | `bun run script/test.ts test/session-execution.test.ts test/session-status-coalescer.test.ts test/session-runner-retry.test.ts test/session-runner-tool-events.test.ts` | 96 통과, 355 assertions | 실행 상태, retry, final count 및 publisher settlement |
| Core | `bun run script/test.ts test/session-runner.test.ts test/session-attachment-quarantine.test.ts test/session-attachment-transport.test.ts test/session-model-transport.test.ts` | 292 통과, 0 실패, 1484 assertions | 전체 Runner 244 포함. canonical safe refs, 원본 불변/reload/followup media `[1,0]`, 실제 loopback WS fresh replay 및 continuation/fallback 보존 |
| Core | `bun run script/test.ts test/session-runner-message.test.ts test/session-model-request-hooks.test.ts test/session-attachment-quarantine.test.ts test/session-runner.test.ts test/session-runner-retry.test.ts test/session-step.test.ts test/session-attachment-transport.test.ts test/session-model-transport.test.ts` | 최신 직접 367 통과, 1816 assertions | cache/hook 격리, retry/Step 전달 및 첨부/transport 통합 8파일 gate |
| Core | `bun run script/test.ts test/session-runner-retry.test.ts test/session-step.test.ts` | 직접 30 통과, 202 assertions | post-tool deadline 출처와 일반 timeout/사용자 중단 구분 |
| TUI | `bun test --timeout 30000 test/session-interrupt-recovery.test.tsx test/mini/stream-v2.transport.test.ts` | 직접 71 통과, 200 assertions | 첫 HTTP 정지 중 두 번째 두-Escape 취소, 지연 ACK 및 lifecycle 정합성 |
| Core/AI | native 대응별 직접 검사 | 실행/coordinator/store 70/287; instruction/plan/status 29/122; message/tool/output/history 80/201; AI provider 41/81; 선택 shell/progress/errors 5/26, 115 filtered | 각 값은 통과/assertions. 원본을 바꾸지 않는 sanitized SessionError와 file-backed Shell jobs 포함; 겹치는 묶음 합산 금지 |
| Core | `bun run script/test.ts test/session-projector-performance-fixture.test.ts test/session-storage-performance-fixture.test.ts test/session-message-storage.test.ts test/session-store.test.ts` | 38 통과, 0 실패, 135 assertions | 같은 4파일 gate 재실행. 아래 fixture 교정 근거 참조. 성능 향상 측정이 아님 |
| Core | `bun run script/test.ts test/session-projector.test.ts test/session-projector-content.test.ts test/session-projector-content-failure.test.ts test/session-message-updater.test.ts` | 29 통과, 98 assertions | 조각별 쓰기, ordinal, replay, fork 및 메타데이터 갱신 |
| Core | `bun run script/test.ts test/session-history.test.ts test/session-history-normalized.test.ts test/session-store.test.ts test/session-transfer.test.ts test/session-revert.test.ts` | 17 통과, 63 assertions | 정규화 history, cursor, transfer 및 revert |
| Core | `bun run script/test.ts test/session-prompt.test.ts test/session-runner-recorded.test.ts test/session-tool-progress.test.ts` | 46 통과, 160 assertions | 보관 시각 0의 변환 수정 후 prompt/보관/도구 진행 검사 |
| Core | `bun run script/test.ts test/filesystem/search.test.ts test/location-filesystem.test.ts` | 22 통과, 53 assertions | 실제 Location을 제공하는 fixture로 검색 및 포함된 링크의 목록 동작 검증 |
| Core | `bun typecheck`, `bun run build`, `bun run script/migration.ts --check` | 성공 | 이 단계에서의 타입, 배포 빌드 및 공식 migration 생성 일치 |
| TUI | `bun test --timeout 30000 test/cli/tui/dialog-session-list.test.tsx` | 앞선 직접 실행: 7 통과, 62 assertions | 실제 renderer 키 입력, 보관/복원/삭제, 30일 또는 all 저장 및 opaque pagination |
| TUI | `bun test --timeout 30000 test/cli/cmd/tui/sync-live-hydration.test.tsx test/cli/tui/thinking-stream.test.tsx test/component/spinner.test.tsx test/mini/footer.statusline.test.tsx`, `bun typecheck` | 33 통과, 121 assertions; 타입 성공 | 기존 32 통과/1 실패를 대체. REST 본문 복원과 live final count 유지 |
| Session UI | `bun test src/timeline/projection.test.ts src/timeline/rows-current.test.ts src/message/current-tool-state.test.ts src/message/current-message.test.ts` | 90 통과, 323 assertions | 현재 projection/도구 상태/메시지, agent/model 상속 및 원본 객체 보존 |
| App | `bun test --conditions=solid --isolate --preload ./happydom.ts src/composer/submit.test.ts src/session/session-ownership.test.ts` | 20 통과, 109 assertions | Client revision 소유 rollback 및 세션 소유권 |
| App | `bun test --conditions=solid --isolate --preload ./happydom.ts src/runtime/i18n/preservation.test.ts src/runtime/i18n/language.test.ts` | 3 통과, 5 assertions | 기존 번역 768개 값의 고정 checksum 및 언어 동작 |
| App | `bun test --conditions=solid --isolate --preload ./happydom.ts src/shell/layout/helpers.test.ts src/workspaces/paths.test.ts src/runtime/server/persistence.test.ts src/runtime/persistence/storage.test.ts src/runtime/persistence/persist.test.ts src/home/sessions/index.test.ts src/home/sessions/records.test.ts src/session/requests/session-request-tree.test.ts` | 109 통과, 238 assertions | 별도 순수 Bun driver로 동일 서버의 `C:/`/`c:/` 프로젝트 별칭 선택 해제/닫기, 원래 닫기 경로, workspace 저장 키 분리 확인. `projectPathKey`만 drive letter 대문자화; 기존 `pathKey`/storage identity, Linux/UNC 대소문자, first-wins 중복 제거와 원래 표기 유지 |
| App | `bun test --conditions=solid --isolate --preload ./happydom.ts src/home/sessions/archive.test.ts src/session/list.test.ts src/home/sessions/records.test.ts src/home/sessions/index.test.ts src/runtime/i18n/preservation.test.ts src/runtime/i18n/language.test.ts` | 21 통과, 45 assertions | native archive/list 및 세션 기록 동작. 구 V1 복원 없음. 위 번역 checksum 증거 유지 |
| App | `set VITE_OPENCODE_TEST_FIXTURES=1 && bun run build` | 성공 | 주 담당자가 production fixture 재빌드. 전체 타입 검사 성공은 아님 |
| App | 기존 keeper production Playwright 17개 | 17 통과 10.0초; 최신 17 통과 9.2초, 재시도 0 | new-session 2개, pending/slash, multi-server captured create/optimistic seed/project promotion/autoaccept, reasoning/count/stale snapshot 10개. 겹치는 실행을 합산하지 않음 |
| App | 기존 `model-activity.spec.ts`, `counter-resync.spec.ts` production Playwright | 최신 2 통과, 6.0초 | 한국어 mobile/pending/tool/SSE overflow 포함. 격리 API/SSE fixture, Playwright가 임시 정적 서버 시작/종료 |

Browser gate는 실제 실행한 keeper와 검증 범위를 표시했다. Bundled Chromium 부재로 설치된 Edge를 사용했고 설치하지 않았다. 증거 artifact: `%TEMP%/opencode/parent-native-final-confirmed-20261016`, `%TEMP%/opencode/parent-activity-final-20261016`. 임시 설정은 `submit-proof.config.cjs`와 앞선 repair-acceptance 설정이다.

### 첨부 격리 및 storage fixture 판정

- 같은 Oracle 세션 `ses_eb9cc9a5dffebkhW2bkQU70M4Z`의 읽기 전용 재검토에서 원래 P2 세 항목 모두 PASS: media wrapper 귀속, 실제 활성 제외 항목만 reset, 소유자 HTTP fallback/실패 횟수 보존. 이는 전체 병합 승인이 아니다. hook/opaque payload의 출처를 알 수 없는 범위는 미검증이다. 추측한 참조 생성이나 전체 media 격리는 하지 않는다.
- storage/perf-reader gate는 새 `rejectedAttachments: []`가 expected runner-context에 없어 먼저 37 통과/1 실패했다. 실제 key는 `initial/entries/rejectedAttachments`였고 201개 항목과 순서는 모두 같았다. 최소 expected shape만 갱신하고 deep strict equality를 유지했으며 예상하지 않은 비어 있지 않은 refs를 거부하는 negative assertion을 추가했다. 동일 gate의 38 통과는 harness 정확성 증거이지 benchmark가 아니다.
- DB lineage/nativeparts/artifact/legacy-v01/legacy-v01-failure 5파일의 위 직접 결과는 33 통과/109 assertions다. standalone replacement 6 통과/30 assertions와 9 filtered는 좁은 범위이며 전체 DB 또는 CLI/TUI 진입점 이전 수용이 아니다.

당시 DB migration에서 실패한 테스트 이름은 다음과 같다. 현재는 네 건 모두 주 담당자 재검증에서 통과했으며 `BLOCKED` 이름도 제거되었다. 아래는 실패 원인을 보존한 과거 기록이다.

- `BLOCKED automatic ws conversion: compact baseline`
- `BLOCKED automatic ws conversion: complete union with metadata alias`
- `BLOCKED automatic ws conversion: old part marker`
- `BLOCKED preserves previous V2 state through the current migration lineage`

당시 네 건은 모두 `WsUpgradeRequiredError`로 변경 전에 중단되었다. 현재 지원 입력의 실제 이전 성공 증거는 위 2026-10-17 기록을 따른다.

## 전체 검사 차단 상태

| 검사 | 최신 상태와 범위 |
| --- | --- |
| 루트 `bun run check` | **최신 실패: lint 1 error / 1478 warnings**, 이후 단계 미실행. 유일한 오류는 비충돌 구 fixture `packages/opencode/test/session/prompt-history-conversion.fixtures.ts:230:26`의 `no-restricted-globals`/`Reflect.set`. 이전 5개 중 나머지는 소스/테스트 폐기로 해소. 이 orphan fixture와 비충돌 소비자는 수정하지 않음. 미병합 오류가 아님 |
| App 전체 typecheck | **최신 재실행도 같은 기존 오류 1건**: `../session-ui/src/components/markdown-cache.tsx:23`, DOMPurify `WindowLike`와 중복 `@types/trusted-types` private brand. 이 파일과 의존성은 수정하지 않음. Session UI typecheck 및 App production build는 통과 |
| App E2E typecheck | **과거 4 errors, 최신 재실행 없음**: `performance/terminals/probe.ts:61`, `performance/timeline/session-timeline-stream-probe.ts:148`, `regression/timeline-stream.spec.ts:1822`, `:1825`. 당시 3파일 모두 incoming과 같았음. 별도의 generic timeline forwarding 오류는 `export const event = makeEvent`로 수정했으며 기존 오류로 분류하지 않음 |
| Core DB migration | 과거 4건 차단 해소. 주 담당자 4건 PASS 및 작업자 전체 migration 반복 결과는 위 참조 |
| staged whitespace | merge 전 focused 검사는 통과했으나 당시 전체 staged 검사는 incoming 공백으로 실패. 현재 index 판정이 아니며 이번 작업에서 stage하지 않음 |

## 마지막 42개 충돌 해결과 native 대체 증거

앞선 구 App bootstrap 6개, context/timeline 16개, facade 6개, helper 3개, submit 2개 및 HTTP `groups/session.ts`, `handlers/session.ts`, `public.ts`, `test/server/httpapi-session.test.ts` 폐기 근거는 유지한다. HTTP 네 경로는 native archive/restore와 normalized store/pagination 16 tests에 대응한다. 이후 마지막 42개도 아래 대체 구현과 검사로 모두 해결·stage했다. 단순한 native import 부재를 삭제 근거로 쓰지 않았으며 V1 private API 호환성은 주장하지 않는다.

| 해결 묶음 | 구현과 확인 범위 |
| --- | --- |
| 구 HTTP HOLD 3개 | `handlers/event.ts`, `handlers/global.ts`, `test/server/httpapi-event.test.ts` 삭제/stage. native handler가 response **이전**에 구독을 확보한다. `EventFeed.subscribe(initial?)`은 저장된 queue failure를 connected frame보다 먼저 검사한다. 실제 HttpApi pre-response hook의 수정 전 encode 0/기대 1 실패와 overflow의 오류 전 connected/data 노출 실패를 확인한 뒤 capture/live/cancel/completion cleanup/encoding failure heartbeat 종료 검사 통과 |
| 구 목록 HOLD 1개 | `test/server/global-session-list.test.ts` 삭제/stage. CoreStore/Protocol URL에 additive `includeArchived` 추가. true는 active+archived, `archived=true`가 우선하여 archive-only, 기본/false는 active-only 유지. opaque cursor에 혼합 flag/filter/order 보존. 실제 SQL ID tie 및 native HTTP asc/desc next/prev 혼합 페이지 누락 없음 |
| CLI/plugin/Effect/RPC 12개 | 구 worker/private API 대신 native process-global 실행 소유권과 반복 취소로 대응해 삭제/stage. 첫 HTTP가 정지해도 두 번째 취소 가능. mini HTTP ACK는 idle을 표시하지 않으며 서버 lifecycle/wait settlement가 기준이다. 늦은 ACK는 후속 실행을 덮어쓰지 않는다. typed error commit으로 실패 처리; 빈 catch나 공유 서버 종료 없음 |
| Retry/processor 4개, prompt 2개 | native deadline 출처/예산과 drain-local converter cache로 보완 후 삭제/stage. 아래 상세 및 367/1816 직접 gate 참조 |
| Provider/tool 5개, instruction/reminders/status 4개 | native provider 오류, ShellID/file-backed 출력, instruction state/plan/status coalescer 직접 검사로 대응 후 삭제/stage. V1 literal metadata/tuple API를 복원하지 않음 |
| 나머지 11개 | message/tool registry/output/history 및 raw counts/tool settlement/compaction/quarantine/per-kind ordinal 대응 증거로 해결. 위 instruction/reminders/status 4개와 함께 나머지 15개 안전 폐기 범위를 구성하며 중복 합산하지 않음 |

- Retry: private `PostToolDeadline` 출처는 100000ms이며 Step이 `noResponse`/`parentID`를 전달한다. 실제 post-tool deadline 예산은 MAIN 10/CHILD 1, 일반 timeout은 3이다. hook 우회와 사용자 중단 retry는 없다. 안전한 구조화 machine log `model.no_response.retrying_continuation`/`post_tool_first_event_timeout`에 provider/model/attempt/source/phase/timestamp/part count를 기록한다. 모르는 출처는 unknown으로 남기며 가짜 watchdog, raw payload, 새 durable field를 추가하지 않는다.
- Cache: 기존 `to-llm-message.ts`의 `createConverter`를 Runner drain당 하나 사용한다. 매 Step DB history reload와 auxiliary cold 경로는 유지한다. zero/multi output·origin·거부 flag를 포함한 안정 prefix block과 immutable encoded positional fingerprint가 edit/insert/delete/reorder/nested mutation/model/ref/metadataKey/resolved identity/quarantine 변경 및 완료 compaction frontier를 반영한다. tail은 새로 변환하며 실제 batch `[100,1,2]`와 cold 결과 동등성을 검사했다.
- Hook: mutable primary context hook 전에 Schema encode → encoded data의 `structuredClone` → decode로 복사하고 즉시 origin을 재연결한다. 이 제어된 복사는 canonical 참조 귀속을 유지한다. hook이 media를 복제하거나 wrapper를 바꾸면 출처를 unknown으로 취급하며 request media replacement는 cache하지 않는다. context hook이 없으면 전체 history 복사를 하지 않지만 기본 hook이 흔하므로 encoded 복사도 흔할 수 있다. 전체 DB hydration/JSON snapshot/hook 복사는 남아 있어 종단 성능 개선으로 해석하지 않는다.

읽기 전용 Oracle 동시성 검토 `ses_eb95eedc1ffe4rPV46LrTn2Ey1`은 심각한 지적 없음, 최종 cache 검토 `ses_eb9560b13ffe7rLCTiphON43k4`는 실제 source/test 변경 확인 후 PASS/지적 없음이었다. 검토자는 테스트를 재실행하지 않았고 전체 병합 승인이 아니다. Core/Server/Client/TUI/CLI 최신 typecheck와 Core build는 직접 PASS다. 별도 비충돌 구 DB palette test는 변경 전 미지원 lineage 거부 검사에 대응해 폐기했지만 SQLite CLI import 흐름 동등성을 주장하지 않는다. `createWorkerFetch`, 3000ms abort timeout, `x-opencode-abort-retried-after-worker-restart` 같은 V1 내부 장치를 native에 재생성하지 않았다.

## 인계 경계

이전 42 U/HOLD/매핑 대기 기록은 위 해결 결과로 대체한다. merge 전 주 담당자의 `unmerged:''`, `unstaged:''`, `untracked:''` 확인과 보호 index 6967개 항목의 SHA256 `2fc6af76a4a347affcc41d04b9f2e0a3352138844f33aea246904a91229af465` 불변 기록은 **과거 증거**이며 현재 index나 작업 트리의 보증이 아니다. 현재 merge commit은 사용자가 완료했다. 이번 문서 편집은 이 파일에만 한정하며 테스트 실행, stage/commit/push, 실사용 DB/profile/model 접근·변경, 서비스 재시작이나 설치를 하지 않는다.

historical ws HEAD build는 `@opencode-ai/protocol/groups/session` 해석 실패로 유효한 baseline을 만들지 못했다. UI 진단 표본과 converter 작업량 검사는 과거 성능 동등성이나 종단 성능 측정이 아니다. **지원 형식의 자동 변환 및 이전 DB 차단 4건은 통과했다. 루트 검사는 기존 `Reflect.set` 오류 1건/1478 warnings로 실패했고 이후 검사는 중단되었다. App typecheck의 같은 기존 오류 1건은 재확인했고 production build는 통과했다. E2E 4건은 과거 기록이며 재실행하지 않았다. Client 생성 hash 결정성은 확인했으며 최종 검토의 두 차단 사항은 수정·재검토 PASS로 닫았다. merge 완료와 전체 검증 통과를 혼동하지 않는다.**
