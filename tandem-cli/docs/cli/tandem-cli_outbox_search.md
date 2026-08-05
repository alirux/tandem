## tandem-cli outbox search

Search outbox messages

```
tandem-cli outbox search [flags]
```

### Options

```
      --aggregate-id string     filter by aggregate id
      --aggregate-type string   filter by aggregate type
      --created-from string     filter: created at or after (RFC3339)
      --created-to string       filter: created at or before (RFC3339)
      --cursor string           opaque cursor from a previous page
  -h, --help                    help for search
      --limit int               page size, 1-500 (default 50 server-side)
      --status string           filter by status (PENDING, IN_FLIGHT, DONE, FAILED, DISCARDED)
      --type string             filter by CloudEvents event type
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

* [tandem-cli outbox](tandem-cli_outbox.md)	 - Inspect and act on outbox messages

