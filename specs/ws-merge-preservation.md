# ws 병합 기능 보존 수용 기록

## 범위와 판정 원칙

- 기준은 그대로 `HEAD 5573129a75492773c5049a0b0909bbfb90d26a9d`, `MERGE_HEAD eeec76bb620f9bee8cab89c5a3a88e315539645b`다. **초기 271개 충돌 모두 해결, 현재 U 0**이다. 읽기 전용 `set "GIT_MASTER=1" && git ls-files -u` 결과도 비어 있다. 마지막 42개(A 26/B 12/C 2/D 2)를 포함한 native 이식과 구 경로 해결은 주 담당자가 모두 stage했다. 이는 index 충돌 해결이며 merge commit 완료나 전체 검사 통과가 아니다.
- 사용자 방향: `2.x` runtime과 schema/정규화 part storage를 유지하며 ws 동작을 native 소유자에 통합한다. Schema → Core/Protocol → Server 의존 방향을 지키고 Client runtime은 Core/Server를 참조하지 않는다. 파생 상태를 durable event에 중복 저장하지 않는다.
- 아래 donor commit은 인계에서 확인된 출처다. 이 문서에서 commit을 독립 검증하지 않았으며, commit 존재나 충돌 표식 제거만으로 동작 보존을 인정하지 않는다. **충돌 해결과 기능 검증의 범위를 구분한 기록이며 전체 검증 승인 또는 과거 성능 동등성 판정이 아니다.**

## 필수 동작과 수용 증거

대상 소유자는 `2.x` 고유 계층의 책임 위치다. 실제 구현과 실행 증거를 확인한 뒤 상태를 갱신한다. 필요한 증거는 테스트 명령, 결과, 대상 참조, 재현 조건을 포함해야 한다.

| 필수 ws 동작 | 인계에서 확인된 donor commit | 대상 고유 소유자 (`target native owner`) | 필수 수용 증거 (`required acceptance proof`) | 현재 상태 |
| --- | --- | --- | --- | --- |
| part storage 및 성능 보존 | `8504130e23`, `0a8c0a157b` | Core DB/storage | 격리 DB 저장·조회·재시작 내용 일치; 동일 입력의 읽기·쓰기 비용 비교 | 부분 검증: 저장/fixture 통과, 성능 비교 차단 |
| 안전한 DB export/import | `9e603e4e0b` | Core DB/storage | 격리 왕복·무결성, 미지원 입력의 변경 전 거부 | 부분 검증: standalone만 통과, 일반 ws 변환 BLOCKED |
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

## 현재 차단 사항과 미확인 조건

- **BLOCKED: 데이터가 있는 일반 ws DB의 자동 변환.** `message/event/instruction/permission/workspace/pending` 매핑의 보존을 실제 테스트로 입증하기 전에는 허용하지 않는다. 파괴적인 과거 데이터 migration은 금지한다. 지원 여부를 확인할 수 없으면 mutation 전에 fail-closed로 중단해야 한다. export/import donor만으로 자동 변환을 승인하지 않는다.
- 병합 전 UI benchmark 시도는 historical ws `HEAD` build에서 `@opencode-ai/protocol/groups/session`을 찾지 못해 실패했다. 유효한 historical baseline은 없다. 현재 production build 성공이나 단일 진단 표본으로 과거 성능 보존을 주장하지 않는다.
- 초기 환경 인계는 `Effect 4.0.1`의 `ignore-scripts` 설치와 실행 Bun `1.4.1`/고정 `1.4.2` 차이를 기록했다. 이를 아래 실제 실행 결과의 실패 원인으로 단정하지 않는다.

## 증거 갱신 규칙

주 통합 담당자가 직접 확인한 결과를 기록한다. 아래 묶음은 서로 겹치므로 합산하지 않는다. 실패 또는 미실행 범위를 수용하지 않으며 부분 검증만으로 전체 기능 검증이 통과했다고 판정하지 않는다. Git 충돌 해결 완료 여부는 별도로 확인한다. 이번 문서 갱신에서는 테스트를 다시 실행하지 않았다.

## 2026-10-16 후속 통합 검증

다음은 주 통합 작업에서 직접 실행한 결과다. 작업자 보고만 회수한 항목, 진행 중인 수정, 성능 비교는 포함하지 않는다. 명령은 표시한 패키지 디렉터리에서 실행했다.

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
| Core | `bun run script/test.ts test/database-migration.test.ts` | 25 통과, 4 실패, 129 assertions | 독립 프로세스 초기화는 통과. 아래 자동 변환 4건은 차단 상태 |
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

DB migration의 다음 실패는 테스트를 삭제하거나 skip하지 않고 유지한다.

- `BLOCKED automatic ws conversion: compact baseline`
- `BLOCKED automatic ws conversion: complete union with metadata alias`
- `BLOCKED automatic ws conversion: old part marker`
- `BLOCKED preserves previous V2 state through the current migration lineage`

네 건 모두 `WsUpgradeRequiredError`로 변경 전에 중단된다. 안전한 거부와 기존 DB의 실제 이전 성공은 별개다.

## 전체 검사 차단 상태

| 검사 | 최신 상태와 범위 |
| --- | --- |
| 루트 `bun run check` | **최신 실패: lint 1 error / 1478 warnings**, 이후 단계 미실행. 유일한 오류는 비충돌 구 fixture `packages/opencode/test/session/prompt-history-conversion.fixtures.ts:230:26`의 `no-restricted-globals`/`Reflect.set`. 이전 5개 중 나머지는 소스/테스트 폐기로 해소. 이 orphan fixture와 비충돌 소비자는 수정하지 않음. 미병합 오류가 아님 |
| App 전체 typecheck | **1 error**: `../session-ui/src/components/markdown-cache.tsx:23`, DOMPurify `WindowLike`와 중복 `@types/trusted-types` private brand. 주 담당자의 `git diff --exit-code MERGE_HEAD --` 비교에서 소스는 incoming과 동일. 의존성 불일치는 수정하지 않음 |
| App E2E typecheck | **4 errors**: `performance/terminals/probe.ts:61`, `performance/timeline/session-timeline-stream-probe.ts:148`, `regression/timeline-stream.spec.ts:1822`, `:1825`. 3파일 모두 MERGE_HEAD 비교 통과. 이번 generic timeline forwarding 오류는 `export const event = makeEvent`로 수정됨; cast/억제 없음. 이를 기존 오류로 분류하지 않음 |
| Core DB migration | **25 통과 / 4 실패 / 129 assertions**, 위 BLOCKED 이름 유지. populated ws 자동 변환 미지원 |
| staged whitespace | 편집한 native Core/Protocol/Server/TUI/Client 및 앞선 지정 App 경로의 focused `git diff --cached --check` 통과. 전체 staged 검사는 보존한 incoming license/font/patch/test262/documentation 공백 때문에 실패. 전체 clean 주장이나 일괄 공백 수정 없음 |

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

이전 42 U/HOLD/매핑 대기 기록은 위 해결 결과로 대체한다. 주 담당자 직접 확인에서 이번 문서 편집 전 `unmerged:''`, `unstaged:''`, `untracked:''`였다. 의도한 대상 밖 보호 index 6967개 항목의 SHA256 `2fc6af76a4a347affcc41d04b9f2e0a3352138844f33aea246904a91229af465`는 변하지 않았다. 앞선 충돌 표식 검사는 0이었고 최종 재검사는 주 담당자가 별도 수행한다. 보고서를 포함한 최종 stage와 index 확인은 주 담당자가 수행한다. commit/push, 실사용 DB 접근·변경, 기존 서비스 재시작은 하지 않았다.

historical ws HEAD build는 `@opencode-ai/protocol/groups/session` 해석 실패로 유효한 baseline을 만들지 못했다. 현재 UI 단일 진단 표본과 converter 작업량 검사는 과거 성능 동등성이나 종단 성능 측정이 아니다. **Git 충돌은 0개이며 모두 stage되었다. 별도로 루트 1/App 1/E2E 4/DB 4 실패와 전체 staged 공백 검사 실패가 남아 있다. 전체 검증 통과나 merge commit 완료를 주장하지 않는다.**
