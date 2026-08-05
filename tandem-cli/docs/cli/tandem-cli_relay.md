## tandem-cli relay

Inspect and control the relay

### Options

```
  -h, --help   help for relay
```

### Options inherited from parent commands

```
      --api-key string       API key (or set TANDEM_ADMIN_API_KEY)
      --base-url string      Admin API base URL (required; or set TANDEM_ADMIN_URL)
      --ca-cert string       PEM file to verify the server against (or set TANDEM_ADMIN_CA_CERT)
      --header stringArray   extra request header 'Name: value' (repeatable)
      --insecure             skip TLS certificate verification
      --output string        output format: human or json (default "human")
      --timeout duration     per-request timeout; 0 disables it (default 1m0s)
      --token string         bearer token (or set TANDEM_ADMIN_TOKEN)
      --yes                  confirm destructive actions without prompting
```

### SEE ALSO

* [tandem-cli](tandem-cli.md)	 - Inspect and act on a Tandem outbox and relay
* [tandem-cli relay buckets](tandem-cli_relay_buckets.md)	 - Per-bucket ownership and lag - all buckets, or one
* [tandem-cli relay pause](tandem-cli_relay_pause.md)	 - Pause the relay (optionally a single bucket)
* [tandem-cli relay release-bucket](tandem-cli_relay_release-bucket.md)	 - Force-release a bucket for reassignment (zombie owner recovery)
* [tandem-cli relay resume](tandem-cli_relay_resume.md)	 - Resume the relay (optionally a single bucket)
* [tandem-cli relay status](tandem-cli_relay_status.md)	 - Relay running state, bucket coverage, and worker count
* [tandem-cli relay workers](tandem-cli_relay_workers.md)	 - Active relay workers

