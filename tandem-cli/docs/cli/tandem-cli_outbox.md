## tandem-cli outbox

Inspect and act on outbox messages

### Options

```
  -h, --help   help for outbox
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
* [tandem-cli outbox discard](tandem-cli_outbox_discard.md)	 - Discard a FAILED message (irreversible; unblocks the aggregate)
* [tandem-cli outbox get](tandem-cli_outbox_get.md)	 - Get one outbox message, with payload and headers
* [tandem-cli outbox replay](tandem-cli_outbox_replay.md)	 - Replay a single message (reset DONE or FAILED to PENDING)
* [tandem-cli outbox replay-bulk](tandem-cli_outbox_replay-bulk.md)	 - Replay all DONE/FAILED messages matching criteria
* [tandem-cli outbox search](tandem-cli_outbox_search.md)	 - Search outbox messages
* [tandem-cli outbox summary](tandem-cli_outbox_summary.md)	 - Outbox health summary: counts per status, plus lag

