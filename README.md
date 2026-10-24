# paraemt-worker

Dedicated HydraFlowSim worker for running cases with the ParaEMT power system electromagnetic
transient simulator. It consumes campaign requests from a Kafka share group, prepares or retrieves
a shared steady state snapshot in S3 compatible object storage, runs each case in an isolated
directory, and publishes measurement results and execution status to Kafka.

## Processing flow

```text
ParaEMT request topic -> ShareConsumer -> ParaEMT -> result topic
                              |                  |
                              +---- status topic-+
                              |
                     S3 compatible storage
                     (shared snapshots, failure logs)
```

The worker accepts messages whose payload has a unique `id` and `plugin: "paraemt"` (the plugin
field defaults to `paraemt`). Supported systems are `2gen`, `ieee9`, `ieee39`, `wecc179`,
`wecc240`, and `2area`. Parameters include `gen_index`, `t_trip`, `t_end`, `ds_rate`, `dsp`,
`t_step`, and `gen_step`. A case must include a generator trip (`t_trip`) or a reference step
(`dsp`). See [`examples/`](examples/) and the upstream ParaEMT model documentation for model
details.

The first worker for a system creates a shared steady state snapshot and uploads it to the
resources bucket. Subsequent tasks reuse the local snapshot or download it from object storage.
Each simulation gets a fresh working directory. On completion, the worker publishes one result
record per scalar or per bus measurement. On failure, it attempts to save the solver output log
in the results bucket.

## Message contracts

Request values are Kafka Connect JSON envelopes, as produced by `paraemt-http-server`:

```json
{"payload":{"id":"uuid","fecha":1761300000000,"plugin":"paraemt","system":"ieee9",
 "gen_index":0,"t_trip":0.2,"t_end":1.0,"ds_rate":10}}
```

Result messages use `simulation_id`, `fecha`, `t`, `measurement`, and `value`. Status messages use
`id`, `fecha`, `status`, `started_at`, `finished_at`, `duration_s`, `error`, and `metrics`. The
`fecha` field records message creation time in epoch milliseconds using the Kafka Connect
Timestamp schema. Configure the relational sink connector to persist status messages in the
`simulation-status` table when that sink is enabled in the deployment.

The worker acknowledges successful processing explicitly. A simulation reported as failed is
acknowledged after the failure status is published; errors that escape message processing release
the message to the share group for redelivery. Review broker share record lock settings,
`MAX_POLL_INTERVAL_MS`, and Kubernetes termination grace periods together. Set them using measured
average simulation durations and include extreme durations plus cleanup and publication time.

## Configuration

Copy `.env.example` and set these environment variables for the deployment:

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | yes | | Broker addresses |
| `CONSUMER_GROUP_ID` | yes | `paraemt-workers` | Share group distributing tasks among replicas |
| `REQUEST_TOPIC` | yes | | ParaEMT work topic |
| `RESULTS_TOPIC` | yes | | Simulation result topic |
| `SIMULATION_STATUS_TOPIC` | yes | | Execution status topic |
| `MAX_POLL_INTERVAL_MS` | no | `3600000` | Consumer processing interval |
| `PRODUCER_FLUSH_TIMEOUT` | no | `60` | Result delivery timeout in seconds |
| `S3_ENDPOINT_URL` | no | AWS endpoint | S3 compatible service endpoint |
| `S3_ACCESS_KEY_ID` | no | Default credential chain | Object storage access key |
| `S3_SECRET_ACCESS_KEY` | no | Default credential chain | Object storage secret |
| `S3_RESOURCES_BUCKET` | no | `hydraflowsim-resources` | Shared snapshots |
| `S3_RESULTS_BUCKET` | no | `hydraflowsim-results` | Failure logs |
| `PARAEMT_HOME` | no | `/opt/paraemt/ParaEMT_public` | ParaEMT checkout directory |
| `PARAEMT_COMMIT` | no | Image build argument | Pinned upstream revision for snapshot keys and provenance |
| `PARAEMT_TIMEOUT_S` | no | `3600` | Maximum duration of one solver invocation |
| `LOG_LEVEL` | no | `INFO` | Logging verbosity |

## Build and run

Build and publish the image to a registry accessible from the Kubernetes cluster:

```sh
docker build -t <registry>/paraemt-worker:<tag> .
docker push <registry>/paraemt-worker:<tag>
```

For local development, install the requirements and set the variables from `.env.example` before
running `python src/main.py`. The worker requires a Kafka broker that supports share groups and
explicit acknowledgements. The deployment must create the request, result, and status topics and
grant the consumer permission to configure its share group, or preconfigure
`share.auto.offset.reset=earliest` administratively.

## Upstream simulator

The container build checks out the public [ParaEMT repository](https://github.com/NatLabRockies/ParaEMT_public)
at a pinned revision. Its upstream license and notices remain with that checkout. This repository
contains the HydraFlowSim integration and does not replace or relicense the simulator itself.
