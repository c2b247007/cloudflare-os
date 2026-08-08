import { RpcStub, RpcTarget, newWorkersRpcResponse } from "capnweb";
import { validateRpc } from "capnweb-validate";
import type { JWTPayload } from "jose";
import { PublicApi, AuthenticatedApi, Overseer, GadgetMetadataWithTimestamps, AiChatAuthorInfo, AiModelConfig, AiGatewayInfo, AiModelProvider, ConnectedAccountsSubscriber, ConnectedAccountsFilter, GatekeeperVendorFilter, ObserverConfigCallback, BlueprintLibrarySummary, BlueprintPublicInfo, BlueprintUserSummary, BlueprintBindingAssignment, AgentSpawnerConfig, WorkpieceId, BLUEPRINT_SCREENSHOT_PATH_PREFIX, BLUEPRINT_SCREENSHOT_R2_PREFIX, blueprintScreenshotUrl, ServerConfig, CloudflareUsageInfo, CloudflareAccountOption, LoginAttempt, GatekeeperAppInfo, AdminApi, GatekeeperVendorInfo, OutputFormatOffer, ListOutputsResult, createOpenGadgetError, getOpenGadgetErrorCode, OPEN_GADGET_ERROR_CODES, AUTH_ERROR_CODES, createAuthError } from '@gadgets/workshop-shared/api';
import type { UiFeatureFlags } from "@gadgets/workshop-shared/feature-flags";
import { getServerConfig } from "./deployment-config.js";
import { isPasswordAuthEnabled, getAuthGatekeeperAllowlist } from "./auth/config.js";
import { getAuthVendorBinding } from "./auth/auth-vendors.js";
import { getUsageInfo } from "./ai-gateway-billing/limits/usage-checker.js";
import { listConnectedAccounts, selectAccount } from "./ai-gateway-billing/cloudflare/connection-service.js";
import { PendingLogin, LoginConnectCallbackImpl } from "./auth/login-flow.js";
import { deploymentOutputForBlueprint, listFormatOffers, readAdminConfig } from "./admin-config.js";

// Re-export the optional-feature Durable Objects + entrypoints so they can be bound in wrangler.
export { PendingLogin, LoginConnectCallbackImpl };
import { GatekeeperUiFrame } from "@gadgets/workshop-shared/gatekeeper";
import { LanguageModelGatekeeper } from "./ai-models";
import { getAiGatewayConfig } from "./ai-gateway.js";
import { AdminSettings, AdminApiImpl } from "./admin-settings.js";
import { BlueprintKvRecord, buildBlueprintArchiveStream, sanitizeBlueprintOutput, listFeaturedBlueprintsFromKv, parseBlueprintArchive, randomBlueprintId, readBlueprintContent, readBlueprintKvRecord } from "./blueprint-archive.js";
import { GatekeeperConnectCallbackImpl, normalizeUsername, UserDurableObject, CLOUDFLARE_VENDOR_ID } from "./user";
import { OverseerDurableObject, GatekeeperLoopback, CodeModeTailLoopback, AgentSpawnerGatekeeper, GatekeeperHookLoopback, GadgetTailLoopback, AgentSelfLoopback, TransientStubLoopback } from "./overseer";
import { ExternalMessageGateway } from "./external-message-gateway";
import { RpcStub as NativeRpcStub } from "cloudflare:workers";
import { recordAnalytics } from "./analytics";
import { handleClientErrorRequest } from "./client-errors.js";
import { verifyCfAccessJwt } from "./access.js";
import { resolveUiFeatureFlags } from "./feature-flags";
import { serveSiteLogo, SITE_LOGO_PATH } from "./site-logo.js";
import { createWorkshopLogger } from "./observability";
import { isDoResetError, retryOnDoReset } from "./do-retry";

const logger = createWorkshopLogger("workshop.server");

// Set once we've asked the AdminSettings DO to install the bundled format blueprints (see the
// fetch handler), so later requests skip the call. The DO holds the real answer.
let formatBlueprintInstallStarted = false;

function publicBlueprintInfo(id: string, metadata: BlueprintPublicInfo['metadata']): BlueprintPublicInfo {
  return {
    id,
    metadata,
    screenshotUrl: blueprintScreenshotUrl(id, metadata),
  };
}

// Re-export entrypoint types from ai-models.ts.
export { LanguageModelGatekeeper };

// Re-export entrypoint types from admin-settings.ts.
export { AdminSettings };

// Re-export entrypoint types from user.ts.
export { UserDurableObject, GatekeeperConnectCallbackImpl };

// Re-export entrypoint types from overseer.ts.
export { OverseerDurableObject, GatekeeperLoopback, GatekeeperHookLoopback,
    CodeModeTailLoopback, AgentSpawnerGatekeeper, GadgetTailLoopback,
    AgentSelfLoopback, TransientStubLoopback };

// Re-export service-binding entrypoint for external channel integrations.
export { ExternalMessageGateway };

// Declare optional environment variables here since they may be omitted from wrangler.jsonc.
type Env = Cloudflare.Env & {
  // Set these if using Cloudflare Access for authentication, otherwise username/password is used.
  CF_ACCESS_AUD?: string,  // audience
  CF_ACCESS_ISS?: string,  // team URL, i.e. https://<team>.cloudflareaccess.com
  DEV?: boolean;
  FLAGS?: Flagship;
}

// =======================================================================================

/** What UserDurableObject.subscribeConnectedAccounts returns, as seen from the Worker. Derived
 * from the method rather than hand-written because the native stub's Unstubify type transform
 * mangles capnweb stubs nested in returns, so the two call sites cast through this (same known
 * type-system gap as the Overseer stub casts). */
type UserSubscription = Awaited<ReturnType<UserDurableObject["subscribeConnectedAccounts"]>>;

/** One browser subscription, held in the session's Worker context (which survives DO resets)
 * so the DO-side registration can be re-created after the object's in-memory state dies.
 *
 * No relay sits between the DO and the browser: re-subscribing triggers the DO's normal
 * catch-up replay (`add` for every current account, then `ready()`), and every browser
 * subscriber reconciles it — repeat `add` for a known id is already the contract (the DO
 * delivers updates through notifyAdd), and every `ready()` handler prunes accounts the replay
 * did not re-send, so disconnects that happened while the registration was dead converge too. */
interface SubscriptionEntry {
  /** Worker-held dup of the browser's subscriber stub; passed to the DO on each (re-)subscribe. */
  subscriber: RpcStub<ConnectedAccountsSubscriber>;
  filter?: ConnectedAccountsFilter;
  /** DO-minted disposer for the current registration; poisoned by a reset, replaced on
   * re-subscribe. Disposal is always best-effort. */
  doDisposer: RpcStub<{}>;
  disposed: boolean;
}

@validateRpc()
class AuthenticatedApiImpl extends RpcTarget implements AuthenticatedApi {
  constructor(private ctx: ExecutionContext, private env: Env,
      userId: DurableObjectId,
      private abortSession: (reason: Error) => void) {
    super();

    this.#userId = userId;
    this.overseers = this.ctx.exports.OverseerDurableObject;
    this.adminSettings = this.ctx.exports.AdminSettings;
    this.users = this.ctx.exports.UserDurableObject;
  }

  private overseers: DurableObjectNamespace<OverseerDurableObject>;
  private adminSettings: DurableObjectNamespace<AdminSettings>;
  private users: DurableObjectNamespace<UserDurableObject>;

  #userId: DurableObjectId;

  // A stub is bound to one incarnation of the DO and is permanently poisoned once that
  // incarnation resets, so re-resolve per call instead of caching one for the session (per the
  // DO error-handling docs: fresh stub per attempt). Stub creation is local and lazy — not a
  // network call. The trade: e-order is guaranteed only per stub, so fresh stubs give up
  // cross-call delivery ordering. No current call site issues a user-DO call before awaiting
  // the one it depends on; an ordering-sensitive site must sequence explicitly (await), never
  // assume delivery order. `#`-private because RpcTarget members are runtime-visible and TS
  // `private` is erased.
  get #user(): DurableObjectStub<UserDurableObject> {
    return this.users.get(this.#userId);
  }

  // Every RPC into the user DO goes through #userRead or #userWrite — a naked `this.#user.x()`
  // is a review defect. This is the single choke point that keeps routine DO resets (storage
  // timeouts, overload aborts) from surfacing in the browser: workerd tags those rejections
  // with structured flags right here in the Worker, one same-colo hop from the object, so
  // recovery is cheap and needs no message matching.

  /** Idempotent read against the user DO: fresh stub per attempt, one retry on reset. */
  async #userRead<T>(operation: string,
               fn: (user: DurableObjectStub<UserDurableObject>) => Promise<T>): Promise<T> {
    let result = await retryOnDoReset(
        () => fn(this.#user),
        info => this.#onUserDoReset(operation, info.error, info.attempt, info.delayMs));
    this.#maybeRevalidateSubscriptions();
    return result;
  }

  /** Non-idempotent call: NO retry — a reset can land after the DO durably committed the write
   * but before the response was delivered, so retrying could double-apply and the client must
   * see the failure. Still observes reset flags (log + subscription recovery) and rethrows
   * unchanged. */
  async #userWrite<T>(operation: string,
                      fn: (user: DurableObjectStub<UserDurableObject>) => Promise<T>): Promise<T> {
    try {
      let result = await fn(this.#user);
      this.#maybeRevalidateSubscriptions();
      return result;
    } catch (e) {
      if (isDoResetError(e)) this.#onUserDoReset(operation, e, 0, 0);
      throw e;
    }
  }

  /** Central reset observation point: keeps the reset volume visible in logs now that users
   * stop seeing it, and (for sessions with live subscriptions) kicks off re-subscription. */
  #onUserDoReset(operation: string, error: unknown, attempt: number, delayMs: number) {
    logger.warn("user DO reset observed", {
      event: "user_do.reset.recovered",
      operation, attempt, durationMs: delayMs,
      durableObjectId: this.#userId.toString(),
      error,
    });
    this.#triggerRecovery();
  }

  // ── subscription recovery ─────────────────────────────────────────────────────────────────
  //
  // The DO holds subscriber registrations in memory only, so a reset destroys them while the
  // browser's WebSocket stays healthy. Three detection paths feed #triggerRecovery:
  //
  //  1. Flags: a call that loses the race with the reset rejects with workerd's structured
  //     flags → #onUserDoReset (production; local aborts reject flagless, see the integration
  //     suite).
  //  2. Incarnation drift: a call issued AFTER the reset simply restarts the object and
  //     succeeds — no error to observe. So while subscriptions exist, successful user-DO calls
  //     opportunistically trigger a recovery pass (throttled); the pass compares the object's
  //     incarnation id against the one the registrations were made under and no-ops when they
  //     match. This also heals registrations lost to plain idle eviction.
  //  3. A new subscribe returning an unexpected incarnation id (see subscribeConnectedAccounts).
  //
  // TODO(stopgap): this entire incarnation-id + drift-polling design exists because workerd's
  // native JSRPC stubs have no brokenness callback — the runtime equivalent of capnweb's
  // `stub.onRpcBroken(cb)` (which exists for browser-side stubs today, but not for
  // DurableObjectStub / native RPC as of 2026-08). When the runtime grows one,
  // `doDisposer.onRpcBroken(() => this.#triggerRecovery())` makes recovery fully event-driven
  // and deletes getIncarnationId(), the throttled check below, and the idle window with it.
  //
  // Residual window (deliberate, closed by the TODO above): a session that is completely idle
  // after the reset learns nothing until its next user-DO call. A standing watchdog RPC would
  // close it, but an awaited in-flight call pins the object in memory (blocks
  // hibernation/eviction) for every open tab — not worth it. The client's socket-level
  // reconnect remains the backstop. The staleness is silent (no error, no toast — the
  // connected-accounts list just stops updating) and biased toward the worst moment: a user
  // watching a page for a change that already happened in the DO (e.g. an OAuth connect
  // completed in a popup).

  #subscriptions = new Set<SubscriptionEntry>();
  /** Incarnation the current registrations were made under; undefined until first subscribe.
   * Only advanced by a fully successful recovery pass, so a failed or torn pass leaves it
   * stale and the next trigger redoes the whole pass. */
  #subscribedIncarnation: string | undefined;
  /** Serializes recovery passes. Each pass re-checks drift first, so queued redundant
   * triggers for the same reset coalesce into no-ops. */
  #recoveryChain: Promise<void> = Promise.resolve();
  #lastIncarnationCheck = 0;
  static readonly #INCARNATION_CHECK_INTERVAL_MS = 30_000;

  /** Cheap, throttled, fire-and-forget drift check — call after any successful user-DO RPC.
   * Sync on purpose so callers never pay latency for it; the recovery pass itself compares
   * incarnations and no-ops when the registrations are still live. */
  #maybeRevalidateSubscriptions() {
    if (this.#subscriptions.size === 0 || this.#subscribedIncarnation === undefined) return;
    let now = Date.now();
    if (now - this.#lastIncarnationCheck < AuthenticatedApiImpl.#INCARNATION_CHECK_INTERVAL_MS) {
      return;
    }
    this.#lastIncarnationCheck = now;
    this.#triggerRecovery();
  }

  /** Queues a recovery pass behind any in-flight one. Best-effort: a failed pass leaves
   * #subscribedIncarnation stale, so the next trigger (flags or throttled check) retries. */
  #triggerRecovery() {
    this.#recoveryChain = this.#recoveryChain.then(() => this.#recoverSubscriptions());
    this.ctx.waitUntil(this.#recoveryChain);
  }

  /** Re-subscribes every live registration against the (restarted) object, if the object's
   * incarnation actually drifted. Runs serialized via #triggerRecovery, never rejects. */
  async #recoverSubscriptions(): Promise<void> {
    if (this.#subscriptions.size === 0 || this.#subscribedIncarnation === undefined) return;
    try {
      let incarnation = await this.#user.getIncarnationId();
      if (incarnation === this.#subscribedIncarnation) return;
      // Iterating the live Set is safe across the awaits below: an entry unsubscribed
      // mid-pass is skipped (Set iterators tolerate deletion, plus the disposed check), and
      // one added mid-pass at worst gets a harmless extra re-subscribe.
      for (let entry of this.#subscriptions) {
        if (entry.disposed) continue;
        // Dispose the old DO-side registration FIRST: a no-op on a dead incarnation, and if
        // this entry was already re-subscribed (e.g. by a pass that failed partway) it cleanly
        // unsubscribes the live registration so the re-subscribe cannot double-deliver.
        try { entry.doDisposer[Symbol.dispose](); } catch {}
        // Plain fresh stub, not a choke-point wrapper: recovery must not recurse into reset
        // handling; a failure here is handled by leaving #subscribedIncarnation stale (see
        // catch). dup() because sending a stub over RPC transfers ownership of the sent handle.
        let subscription = await this.#user.subscribeConnectedAccounts(
            entry.subscriber.dup(), entry.filter) as unknown as UserSubscription;
        if (entry.disposed) {
          // Unsubscribed while the re-subscribe was in flight: nothing owns the registration
          // just created, so tear it down instead of storing a disposer no one will call.
          try { subscription.disposer[Symbol.dispose](); } catch {}
          continue;
        }
        entry.doDisposer = subscription.disposer;
        if (subscription.incarnationId !== incarnation) {
          // The object reset AGAIN mid-pass: entries re-subscribed above are dead already.
          // Fail the pass; the stale #subscribedIncarnation makes the next trigger redo it.
          throw new Error("user DO reset again during subscription recovery");
        }
      }
      this.#subscribedIncarnation = incarnation;
      logger.info("re-subscribed after user DO reset", {
        event: "user_do.subscription.recovered",
        durableObjectId: this.#userId.toString(),
      });
    } catch (e) {
      logger.warn("re-subscribe after user DO reset failed", {
        event: "user_do.subscription.recover_failed",
        durableObjectId: this.#userId.toString(),
        error: e,
      });
    }
  }

  #unsubscribeEntry(entry: SubscriptionEntry) {
    if (entry.disposed) return;
    entry.disposed = true;
    this.#subscriptions.delete(entry);
    try { entry.doDisposer[Symbol.dispose](); } catch {}
    entry.subscriber[Symbol.dispose]();
  }

  #isAdmin(): boolean {
    let name = this.#userId.name;
    let admins = this.env.ADMINS;

    if (!name || !admins) return false;

    if (typeof admins === "string") {
      // Admins should be a JSON binding of array type, but `.env` doesn't actually let you
      // specify JSON bindings, so we also support a string that parses as JSON array.
      admins = JSON.parse(admins);
    }

    if (!Array.isArray(admins)) {
      throw new TypeError("ADMINS must be configured as an array of usernames.");
    }

    return admins.includes(name);
  }

  whoami(): Promise<AiChatAuthorInfo> {
    return this.#userRead("whoami", u => u.whoami());
  }
  setOwnDisplayName(name: string): Promise<void> {
    return this.#userWrite("setOwnDisplayName", u => u.setOwnDisplayName(name));
  }
  changePassword(oldHash: Uint8Array, newHash: Uint8Array): Promise<void> {
    return this.#userWrite("changePassword", u => u.changePassword(oldHash, newHash));
  }
  hasPasswordLogin(): Promise<boolean> {
    return this.#userRead("hasPasswordLogin", u => u.hasPasswordLogin());
  }
  listModels(): Promise<AiChatAuthorInfo[]> {
    return this.#userRead("listModels", u => u.listModels());
  }
  addModel(profile: AiChatAuthorInfo, config: AiModelConfig): Promise<void> {
    return this.#userWrite("addModel", u => u.addModel(profile, config));
  }
  deleteModel(id: string): Promise<void> {
    return this.#userWrite("deleteModel", u => u.deleteModel(id));
  }
  setQuickModel(id: string | null): Promise<void> {
    return this.#userWrite("setQuickModel", u => u.setQuickModel(id));
  }
  getQuickModel(): Promise<null | string> {
    return this.#userRead("getQuickModel", u => u.getQuickModel());
  }

  getPreferredModel(): Promise<string | null> {
    return this.#userRead("getPreferredModel", u => u.getPreferredModel());
  }
  setPreferredModel(id: string | null): Promise<void> {
    return this.#userWrite("setPreferredModel", u => u.setPreferredModel(id));
  }
  isOnboardingCompleted(): Promise<boolean> {
    return this.#userRead("isOnboardingCompleted", u => u.isOnboardingCompleted());
  }
  completeOnboarding(): Promise<void> {
    return this.#userWrite("completeOnboarding", u => u.completeOnboarding());
  }

  getCloudflareUsage(): Promise<CloudflareUsageInfo> {
    return this.#userRead("getCloudflareUsage", u => getUsageInfo(this.env, u));
  }

  listCloudflareAccounts(): Promise<CloudflareAccountOption[]> {
    return this.#userRead("listCloudflareAccounts", u => listConnectedAccounts(this.env, u));
  }

  selectCloudflareAccount(accountId: string): Promise<void> {
    return this.#userWrite("selectCloudflareAccount", u => selectAccount(this.env, u, accountId));
  }

  async setAvatar(data: Uint8Array | null): Promise<void> {
    if (data) {
      if (data.byteLength > 100 * 1024) {
        throw new Error("Avatar too large (max 100 KB)");
      }
      // Verify the data starts with a known image magic-byte header.
      let isJpeg = data[0] === 0xFF && data[1] === 0xD8 && data[2] === 0xFF;
      let isPng = data[0] === 0x89 && data[1] === 0x50 && data[2] === 0x4E && data[3] === 0x47;
      if (!isJpeg && !isPng) {
        throw new Error("Avatar must be a JPEG or PNG image");
      }
    }
    // Avatar data lives in KV (global), not the user's DO storage, so we
    // read/write it directly here to avoid routing through the DO location.
    let userId = this.#userId.name!;
    if (data) {
      await this.env.AVATARS.put(userId, data);
    } else {
      await this.env.AVATARS.delete(userId);
    }
  }
  async getAvatar(userId: string): Promise<Uint8Array | null> {
    let result = await this.env.AVATARS.get(userId, "arrayBuffer");
    if (!result) return null;
    return new Uint8Array(result);
  }

  getAiConfig(): Promise<AiGatewayInfo> {
    let gwConfig = getAiGatewayConfig(this.env);
    if (gwConfig) {
      return Promise.resolve({
        enabled: true,
        enabledProviders: [...gwConfig.providers] as AiModelProvider[],
      });
    } else {
      return Promise.resolve({ enabled: false });
    }
  }

  getUiFeatureFlags(): Promise<UiFeatureFlags> {
    return resolveUiFeatureFlags(this.env, this.#userId.name!);
  }

  async #openGadgetInternal(id: string, shareKey?: string,
                            configureObservers?: RpcStub<ObserverConfigCallback>)
      : Promise<NativeRpcStub<Overseer>> {
    let userId = this.#userId.toString();
    let profileId = this.#userId.name!;
    let overseerId;
    try {
      overseerId = this.overseers.idFromString(id);
    } catch {
      throw createOpenGadgetError(OPEN_GADGET_ERROR_CODES.workspaceNotFound);
    }
    let overseer = this.overseers.get(overseerId);

    // HACK: Detect loss of the connection to the DO by:
    // - Pass a callback to overseer.open() which it should call when the session is disposed.
    // - Detect if the callback itself is disposed before being called, suggesting the connection
    //   was lost.
    // If the connection is lost, we abort this I/O context, which kills the WebSocket from the
    // client, forcing it to engage its reconnect logic, which should recover.
    // TODO: Implement onRpcBroken() in the built-in RPC system, matching Cap'n Web, and use that
    //   instead.
    // TODO: Consider how to reconnect to one DO without resetting the whole WebSocket. Probably
    //   needs new code on the client side. However, typically a client only ever opens one
    //   gadget at a time (since each tab is a separate client), so it's probably fine for now.
    let closed = false;
    let started = false;
    let notifyClosed = () => {
      closed = true;
    };
    (notifyClosed as any)[Symbol.dispose] = () => {
      if (started && !closed) {
        // this.ctx.abort() would be nicer here, but it is still marked experimental in the
        // workers runtime.
        this.abortSession(new Error(`lost connection to workspace DO (gadget ${id})`));
      }
    }

    let result;
    try {
      result = await overseer.open(userId, profileId, notifyClosed, shareKey, configureObservers);
    } catch (err) {
      // A denial proves this user's listing for the workspace is stale: revocation tries to drop it
      // (refreshAffectedCollaboratorListings), but that push is best-effort. Only catches entries
      // they click; others stay frozen at revocation, as a disconnected collaborator gets no pushes.
      if (getOpenGadgetErrorCode(err) === OPEN_GADGET_ERROR_CODES.workspaceAccessDenied) {
        await this.#userWrite("forgetSharedGadget", u => u.forgetSharedGadget(id));
      }
      throw err;
    }
    started = true;
    recordAnalytics(this.ctx, this.env, {
      event_name: "gadget_opened",
      user_id: userId,
      gadget_id: id,
      source: shareKey ? "share_key" : "direct",
    });
    return result;
  }

  async openGadget(id: string, shareKey?: string,
                   configureObservers?: RpcStub<ObserverConfigCallback>)
      : Promise<RpcStub<Overseer>> {
    // @ts-expect-error Cap'n Web RPC stubs and native RPC stubs are compatible but the type
    //     system doesn't know this.
    return this.#openGadgetInternal(id, shareKey, configureObservers);
  }

  async newGadget(): Promise<RpcStub<Overseer>> {
    let id = this.overseers.newUniqueId().toString();
    await this.#userWrite("newGadget", u => u.newGadget(id, "Untitled Workspace"));
    recordAnalytics(this.ctx, this.env, {
      event_name: "gadget_created",
      user_id: this.#userId.toString(),
      gadget_id: id,
      source: "blank",
    });
    let result = await this.openGadget(id);
    if (!result) {
      throw new Error("Open failed despite newly-created workspace?");
    }
    return result;
  }

  async listGadgets(): Promise<GadgetMetadataWithTimestamps[]> {
    return this.#userRead("listGadgets", u => u.listGadgets());
  }

  listOutputs(): Promise<ListOutputsResult> {
    return this.#userRead("listOutputs", u => u.listOutputs());
  }

  async listOutputFormats(): Promise<OutputFormatOffer[]> {
    let offers = await listFormatOffers(this.env, await readAdminConfig(this.env));
    // Neither the agent's hint nor the binding details are part of what a user is offered here.
    return offers.map(({agentHint: _agentHint, bindings: _bindings, ...offer}) => offer);
  }

  listGatekeeperVendors(filter?: GatekeeperVendorFilter): Promise<GatekeeperVendorInfo[]> {
    return this.#userRead("listGatekeeperVendors", u => u.listGatekeeperVendors(filter));
  }

  connectAccount(vendorId: string, resourceUrlPatterns?: string[]): Promise<{url: string}> {
    return this.#userWrite("connectAccount", u => u.connectAccount(vendorId, resourceUrlPatterns));
  }

  ensureAccountResources(accountId: number, resourceUrlPatterns: string[]): Promise<{url?: string}> {
    return this.#userWrite("ensureAccountResources",
        u => u.ensureAccountResources(accountId, resourceUrlPatterns));
  }

  listAddableGatekeepers(): Promise<GatekeeperVendorInfo[]> {
    return this.#userRead("listAddableGatekeepers", u => u.listAddableGatekeepers());
  }

  provisionAmbientAccount(vendorId: string): Promise<void> {
    return this.#userWrite("provisionAmbientAccount", u => u.provisionAmbientAccount(vendorId));
  }

  async subscribeConnectedAccounts(
      subscriber: RpcStub<ConnectedAccountsSubscriber>, filter?: ConnectedAccountsFilter)
      : Promise<RpcStub<{}>> {
    // Dup the browser's subscriber stub: the param handle dies when this call returns, but
    // re-subscribing after a reset needs it for the life of the subscription. Sending a stub
    // over RPC transfers ownership, so every (re-)subscribe sends its own dup() of this master
    // and the master itself is only disposed by #unsubscribeEntry.
    let kept = subscriber.dup();
    let subscription: UserSubscription;
    try {
      // #userWrite, not #userRead: registration is not retry-safe. A `retryable` failure can
      // land after the DO registered but before the response arrived, so a retry would leave a
      // duplicate registration behind — and the includeForcedAutoProvisionedAccounts filter
      // provisions accounts (see listGatekeeperApps). A subscribe raced by a reset surfaces to
      // the component's fallback; the flags observed here still trigger recovery, which — with
      // the next mount — heals it.
      subscription = await this.#userWrite("subscribeConnectedAccounts",
          async u => await u.subscribeConnectedAccounts(
              kept.dup(), filter) as unknown as UserSubscription);
    } catch (e) {
      kept[Symbol.dispose]();
      throw e;
    }
    let entry: SubscriptionEntry = {
      subscriber: kept, filter, doDisposer: subscription.disposer, disposed: false,
    };
    this.#subscriptions.add(entry);

    if (this.#subscribedIncarnation === undefined) {
      this.#subscribedIncarnation = subscription.incarnationId;
    } else if (this.#subscribedIncarnation !== subscription.incarnationId) {
      // The object reset between an earlier subscribe and this one: the older registrations
      // are already dead. Deliberately leave the stale incarnation in place so the recovery
      // pass sees the drift and re-subscribes EVERY entry — redoing this one is harmless (its
      // old registration is disposed first, and browser subscribers are replay-idempotent).
      this.#triggerRecovery();
    }

    // Worker-minted disposer: unlike the DO-minted one it wraps, it survives resets, so the
    // browser's dispose always cleanly tears down whichever registration is current.
    let unsubscribe = () => this.#unsubscribeEntry(entry);
    return new RpcStub<{}>({ [Symbol.dispose]() { unsubscribe(); } });
  }

  disconnectAccount(accountId: number): Promise<void> {
    return this.#userWrite("disconnectAccount", u => u.disconnectAccount(accountId));
  }

  reconnectAccount(accountId: number): Promise<{url: string}> {
    return this.#userWrite("reconnectAccount", u => u.reconnectAccount(accountId));
  }

  startResourceConfigurator(
      accountId: number,
      resourceUrlPattern: string) {
    return this.#userWrite("startResourceConfigurator",
        u => u.startResourceConfigurator(accountId, resourceUrlPattern));
  }

  async dismissSharedGadget(gadgetId: string): Promise<void> {
    return this.#userWrite("dismissSharedGadget", u => u.forgetSharedGadget(gadgetId));
  }

  async listOwnBlueprints(): Promise<BlueprintUserSummary[]> {
    return this.#userRead("listOwnBlueprints", u => u.listBlueprints());
  }

  async getOwnBlueprint(blueprintId: string): Promise<BlueprintUserSummary | null> {
    return this.#userRead("getOwnBlueprint", u => u.getBlueprint(blueprintId));
  }

  async listLibraryBlueprints(): Promise<BlueprintLibrarySummary[]> {
    return this.#userRead("listLibraryBlueprints", u => u.listLibraryBlueprints());
  }

  async setBlueprintPinned(blueprintId: string, pinned: boolean): Promise<void> {
    return this.#userWrite("setBlueprintPinned", u => u.setBlueprintPinned(blueprintId, pinned));
  }

  async isBlueprintPinned(blueprintId: string): Promise<boolean> {
    return this.#userRead("isBlueprintPinned", u => u.isBlueprintPinned(blueprintId));
  }

  async listFeaturedBlueprints(): Promise<BlueprintPublicInfo[]> {
    return (await listFeaturedBlueprintsFromKv(this.env)).map(
        blueprint => publicBlueprintInfo(blueprint.id, blueprint.metadata));
  }

  async addBlueprintToLibrary(blueprintId: string): Promise<void> {
    return this.#userWrite("addBlueprintToLibrary", u => u.addBlueprintToLibrary(blueprintId));
  }

  async removeBlueprintFromLibrary(blueprintId: string): Promise<void> {
    return this.#userWrite("removeBlueprintFromLibrary",
        u => u.removeBlueprintFromLibrary(blueprintId));
  }

  isBlueprintInLibrary(blueprintId: string): Promise<{ uploaded: boolean } | null> {
    return this.#userRead("isBlueprintInLibrary", u => u.isBlueprintInLibrary(blueprintId));
  }

  async importBlueprint(archive: ReadableStream<Uint8Array>): Promise<string> {
    let { metadata, contentLength, content } = await parseBlueprintArchive(archive);
    delete metadata.screenshot;
    let blueprintId = randomBlueprintId();
    let r2Key = `${blueprintId}/${metadata.version}`;

    try {
      let fixedLengthStream = new FixedLengthStream(contentLength);

      await Promise.all([
        content.pipeTo(fixedLengthStream.writable),
        this.env.BLUEPRINT_CONTENT.put(r2Key, fixedLengthStream.readable),
      ]);

      let kvRecord: BlueprintKvRecord = {
        metadata,
        ownerId: this.#userId.toString(),
      };

      await this.env.BLUEPRINTS.put(blueprintId, JSON.stringify(kvRecord));

      await this.#userWrite("importBlueprint", u => u.importBlueprint(blueprintId, metadata));

      recordAnalytics(this.ctx, this.env, {
        event_name: "blueprint_imported",
        user_id: this.#userId.toString(),
        blueprint_id: blueprintId,
      });

      return blueprintId;
    } catch (err) {
      // Try to delete what we uploaded, but don't wait for results becasue there's nothing we
      // can do if they fail, and we already have an error to throw.
      this.env.BLUEPRINTS.delete(blueprintId);
      this.env.BLUEPRINT_CONTENT.delete(r2Key);
      throw err;
    }
  }

  async newGadgetFromBlueprint(
    blueprintId: string,
    bindings: Record<string, BlueprintBindingAssignment>
  ): Promise<RpcStub<Overseer>> {
    // 1. Read blueprint from KV.
    let kvRecord = await readBlueprintKvRecord(this.env, blueprintId);
    if (!kvRecord) throw new Error("Blueprint not found.");

    // 2. Read gzip-compressed Yjs doc from R2 and decompress.
    let codeBytes = await readBlueprintContent(this.env, blueprintId, kvRecord.metadata.version);
    if (!codeBytes) throw new Error("Blueprint content not found in R2.");

    // 3. Create new Overseer DO (same as newGadget()).
    let id = this.overseers.newUniqueId().toString();
    await this.#userWrite("newGadgetFromBlueprint", u => u.newGadget(id, kvRecord.metadata.title));
    let overseerResult = await this.#openGadgetInternal(id);

    // 4. Initialize from blueprint code.
    let overseerDo = this.overseers.get(this.overseers.idFromString(id));
    await overseerDo.initializeFromBlueprint(codeBytes, kvRecord.metadata.title,
        deploymentOutputForBlueprint(await readAdminConfig(this.env), blueprintId,
            sanitizeBlueprintOutput(kvRecord.metadata.output)));

    // 5. Create gatekeepers from assignments and bind them into the workspace's (only) gadget.
    let metadata = await overseerResult.getMetadata();
    using gadget = await overseerResult.getGadget(metadata.defaultGadgetId!);

    // Defensively put blueprint bindings into a map (not a raw object) until we've had a chance to
    // validate the names.
    let blueprintBindings = new Map(Object.entries(kvRecord.metadata.bindings));
    let gadgetId = metadata.defaultGadgetId!;

    // Create gatekeepers in two phases: first every non-spawner binding (binding the
    // non-spawnerOnly ones into the gadget, and recording each created gatekeeper's id by
    // binding name), then the agent spawners, whose configs reference the phase-one results
    // symbolically (see SpawnerEnvTarget).
    let createdIds = new Map<string, WorkpieceId>();
    let gkPromises: Promise<void>[] = [];

    for (let [bindingName, assignment] of Object.entries(bindings)) {
      let blueprintBinding = blueprintBindings.get(bindingName);
      if (!blueprintBinding) {
        throw new Error(`Unknown binding name: ${bindingName}`);
      }

      gkPromises.push((async () => {
        let gk;
        if (assignment.type === "gatekeeper") {
          gk = await overseerResult.newGatekeeper(assignment.accountId, assignment.resourceUrl);
          if (!gk) {
            throw new Error(`Failed to create gatekeeper for binding "${bindingName}".`);
          }
        } else if (assignment.type === "aiModel") {
          gk = await overseerResult.newAiModelGatekeeper(assignment.modelId);
        } else {
          return;  // agent spawners are created in phase two
        }
        try {
          let id = await gk.getId();
          createdIds.set(bindingName, id);
          // A spawnerOnly binding exists purely to feed some spawner's env; it is not bound
          // into the gadget itself.
          if (!blueprintBinding.spawnerOnly) {
            await gadget.bind(bindingName, id);
          }
        } finally {
          gk[Symbol.dispose]();
        }
      })());
    }

    await Promise.all(gkPromises);

    // Phase two: agent spawners, with the full AgentSpawnerConfig reconstructed -- displayName
    // from the binding's title, modelId from the assignment, and env resolved against the
    // phase-one gatekeepers and the new gadget.
    for (let [bindingName, assignment] of Object.entries(bindings)) {
      if (assignment.type !== "agentSpawner") continue;
      let blueprintBinding = blueprintBindings.get(bindingName);
      if (blueprintBinding?.type !== "agentSpawner") {
        throw new Error(`Binding "${bindingName}" type mismatch.`);
      }

      let env: Record<string, WorkpieceId> = {};
      for (let [envName, target] of Object.entries(blueprintBinding.env)) {
        if (target.type === "gadget") {
          env[envName] = gadgetId;
        } else {
          let id = createdIds.get(target.name);
          if (id === undefined) {
            throw new Error(`Agent spawner binding "${bindingName}" references binding ` +
                `"${target.name}", which was not assigned.`);
          }
          env[envName] = id;
        }
      }

      let config: AgentSpawnerConfig = {
        displayName: blueprintBinding.title,
        modelId: assignment.modelId,
        env,
      };
      using gk = await overseerResult.newAgentSpawnerGatekeeper(config);
      await gadget.bind(bindingName, await gk.getId());
    }

    recordAnalytics(this.ctx, this.env, {
      event_name: "gadget_created",
      user_id: this.#userId.toString(),
      gadget_id: id,
      blueprint_id: blueprintId,
      source: "blueprint",
    });

    // @ts-expect-error Cap'n Web RPC stubs and native RPC stubs are compatible but the type
    //     system doesn't know this.
    return overseerResult;
  }

  async deleteOrphanedBlueprint(blueprintId: string): Promise<void> {
    return this.#userWrite("deleteOrphanedBlueprint", u => u.deleteOwnedBlueprint(blueprintId));
  }

  // --- Gatekeeper management apps ---

  // The management apps available to the current user: their connected accounts that declare a
  // top-level UI (AccountDescription.providesUi). The app id is the gatekeeper's routing id (its
  // vendor id, e.g. "context"), so each app is hosted at /gatekeepers/<vendorId>. UI-providing
  // accounts are auto-provisioned singletons (one per vendor), so the vendor id identifies them.
  async listGatekeeperApps(): Promise<GatekeeperAppInfo[]> {
    // listProvidedAccounts provisions auto-provisioned accounts first, so their apps appear in
    // the nav even before the user opens a gadget — in a single round trip. #userWrite, not
    // #userRead: provisioning calls the vendor's createAccount BEFORE the record persists, so a
    // reset in that window followed by a retry would create a duplicate vendor-side account
    // (the DO's dedup promise is in-memory and dies with the reset). Same in getGatekeeperApp.
    let accounts = await this.#userWrite("listGatekeeperApps", u => u.listProvidedAccounts());
    return accounts
        .filter(account => account.description.providesUi)
        .map(account => ({
          id: account.vendorId,
          title: account.description.providesUi!.title,
          icon: account.description.providesUi!.icon,
        }));
  }

  async getGatekeeperApp(id: string): Promise<GatekeeperUiFrame | null> {
    // Self-sufficient: listProvidedAccounts provisions auto-provisioned accounts first, so a
    // direct URL load of /gatekeepers/$id works without racing the Header's listGatekeeperApps.
    // #userWrite because of that provisioning side effect (see listGatekeeperApps); both DO
    // calls share one closure so they reach the same stub/incarnation.
    return this.#userWrite("getGatekeeperApp", async u => {
      let accounts = await u.listProvidedAccounts();
      let app = accounts.find(account => account.vendorId === id && account.description.providesUi);
      if (!app) return null;
      // isAdmin is supplied fresh per open so admin-gated features reflect the user's current status.
      return u.startAccountAppUi(app.accountId, { isAdmin: this.#isAdmin() });
    });
  }

  // --- Deployment admin ---

  async amIAdmin(): Promise<boolean> {
    return this.#isAdmin();
  }

  async getAdminApi(): Promise<RpcStub<AdminApi> | null> {
    if (!this.#isAdmin()) return null;
    // #isAdmin() guarantees a non-empty user id name. Forwarded to gatekeepers when listing the
    // resource catalog so RBAC-gated ones still surface for this admin.
    let adminUserId = this.#userId.name!;
    // @ts-expect-error Cap'n Web RPC stubs and native RPC targets are compatible but the type
    //     system doesn't know this.
    return new AdminApiImpl(this.adminSettings.getByName(""), adminUserId);
  }
}

async function serveBlueprintScreenshot(env: Env, blueprintId: string): Promise<Response> {
  let object = await env.BLUEPRINT_CONTENT.get(`${BLUEPRINT_SCREENSHOT_R2_PREFIX}${blueprintId}`);
  if (!object) return new Response("Not Found", {status: 404});

  let contentType = object.httpMetadata?.contentType;
  if (contentType !== "image/jpeg" && contentType !== "image/png") {
    contentType = "image/jpeg";
  }

  return new Response(object.body, {
    headers: {
      "Content-Type": contentType,
      "Cache-Control": "public, max-age=31536000, immutable",
    },
  });
}

// Returned by startGatekeeperLogin(). Wraps the PendingLogin DO so the client awaits the login
// result through a capability (this stub) rather than a guessable id — no login id is ever exposed
// to the client. Disposing the stub (e.g. when the pop-up closes or the component unmounts) cancels
// the in-flight wait and lets the DO be evicted.
@validateRpc()
class LoginAttemptImpl extends RpcTarget implements LoginAttempt {
  constructor(private pending: DurableObjectStub<PendingLogin>) {
    super();
  }

  async wait(): Promise<string> {
    return await this.pending.awaitResult();
  }
}

@validateRpc()
class PublicApiImpl extends RpcTarget implements PublicApi {
  users: DurableObjectNamespace<UserDurableObject>;

  constructor(private ctx: ExecutionContext, private env: Env,
      private abortSession: (reason: Error) => void,
      private accessPayload?: JWTPayload) {
    super();
    this.users = this.ctx.exports.UserDurableObject;
  }

  async getServerConfig(): Promise<ServerConfig> {
    return getServerConfig(this.env);
  }

  async startGatekeeperLogin(vendorId: string): Promise<{ url: string; attempt: RpcStub<LoginAttempt> }> {
    if (!getAuthGatekeeperAllowlist(this.env).includes(vendorId)) {
      throw new Error(`Sign-in via "${vendorId}" is not enabled on this deployment.`);
    }
    const vendor = getAuthVendorBinding(this.env, vendorId);
    if (!vendor) throw new Error(`No such auth gatekeeper: ${vendorId}`);
    const desc = await vendor.describe();
    if (!desc.providesAuth) throw new Error(`"${vendorId}" does not provide authentication.`);

    // The PendingLogin DO is the rendezvous between this request and the (separate) OAuth-callback
    // invocation. The client never sees its id — we hand back an `attempt` stub instead.
    const pendingId = this.ctx.exports.PendingLogin.newUniqueId();
    const pending = this.ctx.exports.PendingLogin.get(pendingId);
    const callback = this.ctx.exports.LoginConnectCallbackImpl(
        { props: { pendingId: pendingId.toString(), vendorId } });
    // For most providers, sign-in needs only minimal scopes to verify the user's email (the grant is
    // transient); capability scopes are requested later via an explicit connectAccount. Cloudflare is
    // the exception: signing in with Cloudflare also links AI Gateway billing, so it requests the
    // full (persistent) scope set up front and LoginConnectCallbackImpl persists the connection.
    const scopes = vendorId === CLOUDFLARE_VENDOR_ID ? "full" : "auth";
    const { url } = await vendor.connectAccount(callback, { scopes });
    // @ts-expect-error Cap'n Web RPC stubs and native RPC targets are compatible but the type
    //     system doesn't know this.
    return { url, attempt: new LoginAttemptImpl(pending) };
  }

  async authenticate(token: string): Promise<AuthenticatedApi> {
    let split = token.split(':');
    if (split.length !== 2) {
      throw createAuthError(AUTH_ERROR_CODES.invalidSessionToken);
    }

    let userId = this.users.idFromName(split[0]);
    // Token verification is an idempotent read; fresh stub per attempt.
    await retryOnDoReset(() => this.users.get(userId).authenticate(split[1]));
    recordAnalytics(this.ctx, this.env, {
      event_name: "user_authenticated",
      user_id: userId.toString(),
      source: "session_token",
    });
    return new AuthenticatedApiImpl(this.ctx, this.env, userId, this.abortSession);
  }

  async authenticateFromCfAccess(): Promise<AuthenticatedApi> {
    if (!this.accessPayload) {
      throw createAuthError(AUTH_ERROR_CODES.notAuthenticatedWithAccess);
    }

    let email = this.accessPayload.email as string;
    let userId = this.users.idFromName(email);
    let signupsEnabled = (await readAdminConfig(this.env)).signupsEnabled;
    let accountCreated =
        await this.users.get(userId).authenticateFromCfAccess(email, signupsEnabled);
    if (accountCreated) {
      recordAnalytics(this.ctx, this.env, {
        event_name: "account_created",
        user_id: userId.toString(),
        source: "cf_access",
      });
    }
    recordAnalytics(this.ctx, this.env, {
      event_name: "user_authenticated",
      user_id: userId.toString(),
      source: "cf_access",
    });
    return new AuthenticatedApiImpl(this.ctx, this.env, userId, this.abortSession);
  }

  async login(username: string, passwordHash: Uint8Array): Promise<string | null> {
    if (this.env.CF_ACCESS_AUD) {
      throw new Error("This deployment requires Cloudflare Access authentication.");
    }
    if (!isPasswordAuthEnabled(this.env)) {
      throw new Error("Password login is disabled on this deployment. Use a sign-in option.");
    }

    username = normalizeUsername(username);

    let id = this.users.idFromName(username);
    let user = this.users.get(id);

    let token = await user.login(passwordHash);
    if (!token) return null;

    recordAnalytics(this.ctx, this.env, {
      event_name: "user_authenticated",
      user_id: id.toString(),
      source: "password",
    });

    return `${username}:${token}`;
  }

  async createAccount(username: string, displayName: string, passwordHash: Uint8Array)
      : Promise<string | null> {
    if (this.env.CF_ACCESS_AUD) {
      throw new Error("This deployment requires Cloudflare Access authentication.");
    }
    if (!isPasswordAuthEnabled(this.env)) {
      throw new Error("Password signup is disabled on this deployment. Use a sign-in option.");
    }
    if (!(await readAdminConfig(this.env)).signupsEnabled) {
      throw new Error("New signups are currently disabled on this deployment.");
    }

    username = normalizeUsername(username);

    let id = this.users.idFromName(username);
    let user = this.users.get(id);

    let token = await user.createAccount(username, displayName, passwordHash);
    if (!token) return null;

    recordAnalytics(this.ctx, this.env, {
      event_name: "account_created",
      user_id: id.toString(),
      source: "password",
    });

    return `${username}:${token}`;
  }

  async getBlueprint(id: string): Promise<BlueprintPublicInfo | null> {
    let kvRecord = await readBlueprintKvRecord(this.env, id);
    if (!kvRecord) return null;

    return publicBlueprintInfo(id, kvRecord.metadata);
  }

  async downloadBlueprint(id: string): Promise<ReadableStream<Uint8Array>> {
    let kvRecord = await readBlueprintKvRecord(this.env, id);
    if (!kvRecord) throw new Error("Blueprint not found.");

    let r2Object = await this.env.BLUEPRINT_CONTENT.get(`${id}/${kvRecord.metadata.version}`);
    if (!r2Object) throw new Error("Blueprint content not found in R2.");

    let metadata = { ...kvRecord.metadata };
    delete metadata.screenshot;

    return buildBlueprintArchiveStream(metadata, r2Object.body, r2Object.size);
  }
}

export default {
  async fetch(req: Request, env: Env, ctx: ExecutionContext) {
    let url = new URL(req.url);

    if (url.pathname === SITE_LOGO_PATH) {
      return serveSiteLogo(req, env.BLUEPRINT_CONTENT);
    }

    if (url.pathname.startsWith(BLUEPRINT_SCREENSHOT_PATH_PREFIX)) {
      let blueprintId = url.pathname.slice(BLUEPRINT_SCREENSHOT_PATH_PREFIX.length);
      return serveBlueprintScreenshot(env, blueprintId);
    }

    // Sign-in via authentication gatekeepers happens entirely within each gatekeeper Worker (the
    // OAuth redirect lands on `/gatekeeper/<name>/oauth`); the result is bridged back to the waiting
    // browser via the `attempt` stub from PublicApi.startGatekeeperLogin(). So the backend no longer
    // hosts /auth/* callbacks.

    if (url.pathname === "/api/client-errors") {
      return handleClientErrorRequest(req, env, ctx);
    }

    if (url.pathname === "/api") {
      // Make sure the bundled format blueprints are installed. The AdminSettings DO doesn't wake
      // merely because someone deployed, so the install needs a trigger; hanging it off API
      // traffic means a fresh deployment is provisioned by its first visitor. Fire-and-forget,
      // and the DO is idempotent.
      if (!formatBlueprintInstallStarted) {
        formatBlueprintInstallStarted = true;
        ctx.waitUntil(ctx.exports.AdminSettings.getByName("").ensureFormatBlueprintsInstalled()
            .then((complete: boolean) => {
              // A partial install resolves rather than throwing, and nothing else will call the DO
              // from here, so clearing this is the whole retry: one bad archive would otherwise
              // leave the deployment half-provisioned for as long as the isolate lives.
              if (!complete) formatBlueprintInstallStarted = false;
            })
            .catch((err: unknown) => {
              // Likewise let the next request try again. The DO coalesces concurrent callers, so a
              // retry costs one comparison once it succeeds.
              formatBlueprintInstallStarted = false;
              logger.warn("failed to install bundled format blueprints", {
                event: "formats.install.trigger.failed", error: err,
              });
            }));
      }

      let accessPayload: JWTPayload | undefined;

      if (env.CF_ACCESS_AUD) {
        if (req.headers.get("Origin") !== url.origin) {
          return new Response("Cross-origin API access not allowed.", { status: 403 });
        }

        const payload = await verifyCfAccessJwt(req, env);
        if (!payload) return new Response("Invalid CF access JWT.", { status: 403 });

        if (!payload.email) {
          return new Response("Access JWT didn't specify email address.", { status: 403 });
        }

        accessPayload = payload;
      }

      // HACK: Implement `abortSession` callback by closing the websocket.
      // TODO: When ctx.abort() becomes non-experimental, consider using that instead.
      let resp: Response | undefined;
      let aborted = false;
      let abortSession = (reason: Error) => {
        // Closing the socket fails no invocation, so nothing else logs this.
        logger.warn("aborting api session", { event: "session.abort", error: reason });
        aborted = true;
        resp?.webSocket?.close();
      };

      resp = await newWorkersRpcResponse(req,
          new PublicApiImpl(ctx, env, abortSession, accessPayload));

      if (aborted) {
        // Oops, we missed the abortSession() call while awaiting, apply now.
        resp?.webSocket?.close();
      }
      return resp;
    }

    return new Response("Not Found", {status: 404});
  }
} satisfies ExportedHandler<Env>;
