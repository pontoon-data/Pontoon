By default, Pontoon collects anonymized usage data through [PostHog](https://posthog.com/) to help us improve the performance and reliability of our tool. The data we collect includes general usage statistics and metadata such as transfer performance (e.g. error rates) to monitor the application’s health and functionality.

If you’d like to disable all telemetry, you can do so by setting the environment variable `PONTOON_TELEMETRY_DISABLED` to `true`:

```bash
docker run \
  -e PONTOON_TELEMETRY_DISABLED=true \
  /* additional args */ \
  ghcr.io/pontoon-data/pontoon/pontoon-unified:latest
```

If you're running Pontoon with Docker Compose, set `PONTOON_TELEMETRY_DISABLED=true` in your `.env` file.

If you disabled telemetry correctly, you'll see the following log when starting Pontoon: `Telemetry is disabled`
