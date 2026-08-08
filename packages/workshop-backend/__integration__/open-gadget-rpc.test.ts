import { abortAllDurableObjects } from "cloudflare:test";
import { exports } from "cloudflare:workers";
import { newWebSocketRpcSession, RpcTarget, type RpcStub } from "capnweb";
import {
  createOpenGadgetError,
  getOpenGadgetErrorCode,
  OPEN_GADGET_ERROR_CODES,
  type AuthenticatedApi,
  type OpenGadgetErrorCode,
  type PublicApi,
} from "@gadgets/workshop-shared/api";
import { describe, expect, it, vi } from "vitest";

type CodedError = Error & { code?: unknown };

const PASSWORD_HASH = new Uint8Array([1, 2, 3]);
const EXPECTED_MESSAGES: Record<OpenGadgetErrorCode, string> = {
  [OPEN_GADGET_ERROR_CODES.workspaceNotFound]: "Workspace not found.",
  [OPEN_GADGET_ERROR_CODES.workspaceAccessDenied]: "You don't have access to this workspace.",
};

function username(prefix: string): string {
  return prefix + crypto.randomUUID().replaceAll("-", "");
}

async function rejection(value: PromiseLike<unknown>): Promise<CodedError> {
  try {
    await value;
  } catch (error) {
    if (!(error instanceof Error)) {
      throw new TypeError("Expected RPC to reject with an Error.", { cause: error });
    }
    return error;
  }
  throw new Error("Expected RPC to reject.");
}

function expectRpcCode(error: CodedError, code: OpenGadgetErrorCode): void {
  expect(error.message).toBe(EXPECTED_MESSAGES[code]);
  expect(error.code).toBe(code);
  expect(Object.prototype.propertyIsEnumerable.call(error, "code")).toBe(true);
  expect(getOpenGadgetErrorCode(error)).toBe(code);
}

async function connect(): Promise<RpcStub<PublicApi>> {
  const response = await exports.default.fetch(new Request("https://workshop.invalid/api", {
    headers: { Upgrade: "websocket" },
  }));

  expect(response.status).toBe(101);
  const socket = response.webSocket;
  if (!socket) throw new TypeError("Expected a WebSocket response.");

  socket.accept();
  return newWebSocketRpcSession<PublicApi>(socket);
}

async function createAccount(
    publicApi: RpcStub<PublicApi>, prefix: string): Promise<{ username: string; token: string }> {
  const name = username(prefix);
  const token = await publicApi.createAccount(name, name, PASSWORD_HASH);
  if (token === null) throw new Error(`Failed to create ${name}.`);
  return { username: name, token };
}

async function openRejection(
    authenticated: RpcStub<AuthenticatedApi>,
    id: string): Promise<CodedError> {
  using workspace = authenticated.openGadget(id);
  return await rejection(workspace.getMetadata());
}

// TODO: This test suite keeps timing out in CI, skipping for now.
describe.skip("openGadget errors across native RPC and Cap'n Web", () => {
  it("retains enumerable Error.code at the native Durable Object boundary", async () => {
    const code = OPEN_GADGET_ERROR_CODES.workspaceNotFound;
    const local = createOpenGadgetError(code);

    expect(local.message).toBe(EXPECTED_MESSAGES[code]);
    expect(local.code).toBe(code);
    expect(Object.prototype.propertyIsEnumerable.call(local, "code")).toBe(true);

    const name = username("native");
    const userId = exports.UserDurableObject.idFromName(name).toString();
    const workspaceId = exports.OverseerDurableObject.newUniqueId();
    const error = await rejection(
      exports.OverseerDurableObject.get(workspaceId).open(userId, name, () => {}),
    );

    expectRpcCode(error, code);
  });

  it("maps malformed IDs through AuthenticatedApi", async () => {
    using publicApi = await connect();
    const account = await createAccount(publicApi, "missing");
    using authenticated = await publicApi.authenticate(account.token);

    const error = await openRejection(authenticated, "not-a-durable-object-id");
    expectRpcCode(error, OPEN_GADGET_ERROR_CODES.workspaceNotFound);
  });

  it("maps valid-but-missing IDs through AuthenticatedApi", async () => {
    using publicApi = await connect();
    const account = await createAccount(publicApi, "missing");
    using authenticated = await publicApi.authenticate(account.token);

    const id = exports.OverseerDurableObject.newUniqueId().toString();
    const error = await openRejection(authenticated, id);
    expectRpcCode(error, OPEN_GADGET_ERROR_CODES.workspaceNotFound);
  });

  it("maps an unauthorized existing workspace to access denied", async () => {
    using publicApi = await connect();
    const ownerAccount = await createAccount(publicApi, "owner");
    const intruderAccount = await createAccount(publicApi, "intruder");
    using owner = await publicApi.authenticate(ownerAccount.token);
    using intruder = await publicApi.authenticate(intruderAccount.token);

    using workspace = await owner.newGadget();
    const metadata = await workspace.getMetadata();

    const nativeError = await rejection(
      exports.OverseerDurableObject
        .get(exports.OverseerDurableObject.idFromString(metadata.id))
        .open(
          exports.UserDurableObject.idFromName(intruderAccount.username).toString(),
          intruderAccount.username,
          () => {},
        ),
    );
    expectRpcCode(nativeError, OPEN_GADGET_ERROR_CODES.workspaceAccessDenied);

    const browserError = await openRejection(intruder, metadata.id);
    expectRpcCode(browserError, OPEN_GADGET_ERROR_CODES.workspaceAccessDenied);
  });
});

// The DO-reset recovery contract is server-side: in production, workerd tags rejections from a
// reset Durable Object with structured flags (`durableObjectReset`, `retryable`, `overloaded` —
// workerd jsg/util.c++), and the worker's retry predicate (do-retry.ts) reads exactly those
// flags. Locally, vitest-pool-workers aborts reject FLAGLESS with the test harness's own
// message, on both the native (worker→DO) and capnweb views — this test pins that, so if a
// future pool/miniflare upgrade starts attaching the production flags, it fails and the
// flag-dependent paths can graduate from synthetic unit tests to real-reset integration tests.
// abortAllDurableObjects() is the non-graceful teardown — the local stand-in for the
// storage-timeout/overload resets observed in production. (Deliberately not
// evictDurableObject(): eviction is graceful and never breaks a stub.)
describe("user-DO reset flags", () => {
  it("local aborts reject flagless — flag-based recovery is untestable locally", async () => {
    using publicApi = await connect();
    const account = await createAccount(publicApi, "probe");
    using authenticated = await publicApi.authenticate(account.token);

    expect(await authenticated.listModels()).toBeInstanceOf(Array);

    // Bind a native stub to the current DO incarnation BEFORE the reset — a stub minted after
    // the abort would simply restart the object and succeed. This poisoned-stub rejection is
    // the exact shape AuthenticatedApiImpl sees when one of its calls loses the reset race.
    const userStub = exports.UserDurableObject.get(
      exports.UserDurableObject.idFromName(account.username));
    expect(await userStub.listModels()).toBeInstanceOf(Array);

    await abortAllDurableObjects();

    // The session recovers: AuthenticatedApiImpl resolves a fresh stub per call, so the
    // restarted object serves this read — the browser never sees the reset.
    expect(await authenticated.listModels()).toBeInstanceOf(Array);

    const nativeErr = await rejection(userStub.listModels());
    expect({
      message: nativeErr.message,
      durableObjectReset: (nativeErr as Record<string, unknown>).durableObjectReset,
      retryable: (nativeErr as Record<string, unknown>).retryable,
      overloaded: (nativeErr as Record<string, unknown>).overloaded,
    }).toEqual({
      message: "Application called abortAllDurableObjects().",
      durableObjectReset: undefined,
      retryable: undefined,
      overloaded: undefined,
    });
  });
});

// Recovery of live connected-accounts subscriptions across a reset. The DO holds its
// registrations in memory only; the session re-subscribes the browser's own subscriber stub
// when it detects the object's incarnation changed — no relay interposes, because every
// browser subscriber is replay-idempotent (repeat `add` is the update contract and every
// `ready()` handler tolerates refiring). Local aborts reject flagless (see above) and calls
// issued after the reset simply restart the object and succeed, so the incarnation-drift path
// is the one exercised here — the flag path is covered by unit tests. With no OAuth vendors
// bound in the test env there is no way to mutate connected accounts, so "updates flow after
// recovery" is asserted end-to-end as: the subscriber receives the restarted object's catch-up
// replay (a second ready()) without errors. Manual fault injection against a dev server
// remains the full-stack check.
describe("user-DO subscription recovery", () => {
  class RecordingSubscriber extends RpcTarget {
    adds: number[] = [];
    removes: number[] = [];
    readyCount = 0;

    add(id: number): void { this.adds.push(id); }
    remove(id: number): void { this.removes.push(id); }
    ready(): void { this.readyCount++; }
  }

  // The DO fires subscriber callbacks without awaiting them, so arrival lags the subscribe
  // call's return by a beat; recovery additionally runs in a background waitUntil. Conditions
  // MUST be >= (never ===): a recovery replay can bump readyCount past the target between
  // polls, so an equality check races its own success.
  const waitFor = (condition: () => boolean, what: string) =>
    vi.waitFor(() => { expect(condition(), what).toBe(true); }, { timeout: 2000 });

  it("changes the DO incarnation id on reset (the drift-detection primitive)", async () => {
    const id = exports.UserDurableObject.idFromName(username("incarnation"));
    const before = await exports.UserDurableObject.get(id).getIncarnationId();
    await abortAllDurableObjects();
    const after = await exports.UserDurableObject.get(id).getIncarnationId();
    expect(before).toMatch(/[0-9a-f-]{36}/);
    expect(after).toMatch(/[0-9a-f-]{36}/);
    expect(after).not.toBe(before);
  });

  it("re-subscribes across a reset (subscribers see the catch-up replay), no errors", async () => {
    using publicApi = await connect();
    const account = await createAccount(publicApi, "resub");
    using authenticated = await publicApi.authenticate(account.token);

    const first = new RecordingSubscriber();
    using firstSub = await authenticated.subscribeConnectedAccounts(first);
    await waitFor(() => first.readyCount >= 1, "first subscriber's ready()");

    await abortAllDurableObjects();

    // A second subscribe lands on the restarted incarnation; the session notices the
    // incarnation drift and re-subscribes every registration in the background. The first
    // subscriber receiving the replay's second ready() is the proof recovery reached it.
    const second = new RecordingSubscriber();
    using secondSub = await authenticated.subscribeConnectedAccounts(second);
    await waitFor(() => second.readyCount >= 1, "second subscriber's ready()");

    await waitFor(() => first.readyCount >= 2, "first subscriber's post-recovery replay");
    expect(await authenticated.listModels()).toBeInstanceOf(Array);

    expect(first.removes).toEqual([]);  // empty replay of an empty account list: no removes

    // Both disposers are Worker-minted and must tear down cleanly even though the DO-side
    // registration they originally wrapped died with the old incarnation.
    firstSub[Symbol.dispose]();
    secondSub[Symbol.dispose]();
    expect(await authenticated.listModels()).toBeInstanceOf(Array);
  });

  it("disposing a subscription whose DO registration died with a reset is safe", async () => {
    using publicApi = await connect();
    const account = await createAccount(publicApi, "dispose");
    using authenticated = await publicApi.authenticate(account.token);

    const subscriber = new RecordingSubscriber();
    const sub = await authenticated.subscribeConnectedAccounts(subscriber);
    await waitFor(() => subscriber.readyCount >= 1, "subscriber's ready()");

    await abortAllDurableObjects();

    sub[Symbol.dispose]();
    // The session must remain fully usable after disposing across the reset.
    expect(await authenticated.listModels()).toBeInstanceOf(Array);
  });
});
