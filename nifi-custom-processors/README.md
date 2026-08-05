# NiFi Custom Python Processors

Local playground for custom NiFi 2.x Python processors (`nifiapi`). Not built/packaged automatically — drop a `.py` file into a mounted `python/extensions` directory (PVC or `minikube mount`, see `DesktopShare/completed/nifi-minikube-custom-processor.md`) and NiFi hot-loads it.

## Processors

| File | Base class | What it does |
|---|---|---|
| `TransactionGenerator.py` | `nifiapi.flowfilesource.FlowFileSource` | First synthetic data generator. Creates realistic credit-card transaction FlowFiles (JSON) for testing downstream fraud-detection pipelines, in "normal" or "fraudulent" mode. |
| `NewTransactionGenerator.py` | `nifiapi.flowfilesource.FlowFileSource` | Refactored, cleaned-up successor to `TransactionGenerator.py` — same synthetic fraud-demo transaction generator, developed after initial testing. Also packaged as a NAR (see `../my-custom-nifi-bundle/`) to demonstrate the Java-bundle deployment path alongside the pure-Python one. |
| `GenericTransform.py` | `nifiapi.flowfiletransform.FlowFileTransform` | Bare-minimum proven skeleton (the "framework" from the AI guide below). Validates that a custom transform processor loads in the NiFi UI and exposes `success`/`failure` relationships before any business logic is added — the template to start from for any new transform processor. |
| `FraudModel.py` | `nifiapi.flowfiletransform.FlowFileTransform` | Native in-NiFi implementation of the CML fraud-detection heuristic (high amount + suspicious geography). Enriches the payload with `cml_response` (fraud_score, risk_level, decision, explanations), built on the `GenericTransform` skeleton. |
| `PandasJSONTransformer.py` | `nifiapi.flowfiletransform.FlowFileTransform` | Native Pandas-based geospatial math inside NiFi. Normalizes JSON transaction FlowFiles with lat/lon into a DataFrame and computes vectorized Euclidean distance from a home reference point, adding `dist_from_home` while preserving original transaction metadata. |
| `XLivePostProcessor.py` | `nifiapi.flowfiletransform.FlowFileTransform` | Posts (or replies) to X via OAuth1-signed `POST /2/tweets`. `Dry Run` property for safe testing, `Reply To Tweet ID` so one processor instance handles both a top-level post and a reply. Built for `cso-operator-app`'s live-streamer-alert flow — the reference example for a `FlowFileTransform`-style processor that actually takes a flowfile in (the generators above are source-style). |
| `TwitchChatListenerProcessor.py` | `nifiapi.flowfilesource.FlowFileSource` | Holds a persistent Twitch IRC connection and emits one FlowFile per detected `!load <streamer> [screen]` (`!l` alias) or `!matrix <screenN>` (`!m` alias) chat command. Requests the `twitch.tv/tags` IRCv3 capability to read badges/mod status per message; mod-only short forms (`!m`, `k:` in place of `kick:`, `s1`-`s4` in place of `screen1`-`4`) are each gated independently on broadcaster/moderator status, with the full-text forms still open to everyone. Checks the streamer's live status via cso-operator-app before dispatching a load (fails open on a lookup error rather than silently blocking a real load), applies a shared cooldown across both commands, mints a fresh access token from the refresh token before every (re)connect, and reconnects with backoff on disconnect. Backend for the array's `!load`/`!matrix` Twitch chat bot. |
| `TwitchChatReplyProcessor.py` | `nifiapi.flowfiletransform.FlowFileTransform` | Posts a one-off Twitch chat confirmation via the Helix "Send Chat Message" API once a dispatch to an edge device has actually succeeded (wired downstream of `InvokeHTTP`'s `Original` relationship, not at parse time). Mints a stateless App Access Token via the client_credentials grant instead of reusing `TwitchChatListenerProcessor`'s rotating user refresh token, so the two can't race each other. `Dry Run` property for safe testing. |
| `WatchlistChatJoinerProcessor.py` | `nifiapi.flowfiletransform.FlowFileTransform` | Holds one persistent IRC connection, opened once in `onScheduled`, and executes JOIN + a one-time greeting for whichever streamer the incoming FlowFile names. Does no polling or timers of its own — the upstream flow (watchlist fetch → live-check → dedup cache) decides when a FlowFile reaches it. Fully separate connection and refresh token from `TwitchChatListenerProcessor`. `Dry Run` (default true) skips the real IRC connection and logs what would be sent. |

## Pattern for building a new one

All of the processors above were built following the exact methodology in [How to Build and Test Custom NiFi Processors with AI](https://cldr-steven-matison.github.io/blog/How-to-AI-with-NiFi-and-Python/): prove a bare `GenericTransform`/`FlowFileSource` skeleton loads and routes on the NiFi canvas first, inject real logic only after, keep error handling defensive (route to `failure`, never crash the processor).

## Related: the streamer backend these processors serve

`XLivePostProcessor.py`, `TwitchChatListenerProcessor.py`, `TwitchChatReplyProcessor.py`, and `WatchlistChatJoinerProcessor.py` are the NiFi-side half of `cso-operator-app`'s Streamers module — the actual app logic, HTTP endpoints, and NiFi flow they plug into live in that repo, not here:

- `cso-operator-app/backend/services/streamers.py` — service logic
- `cso-operator-app/backend/routers/streamers.py` — HTTP routes (watchlist, live-check, etc. that these processors call)
- `cso-operator-app/streamers/StreamersApp.json` — the exported NiFi flow definition that wires these processors together
- `cso-operator-app/nifi-processors/` — that repo's own mirror of these same four processors

Repo: [github.com/cldr-steven-matison/cso-operator-app](https://github.com/cldr-steven-matison/cso-operator-app)
