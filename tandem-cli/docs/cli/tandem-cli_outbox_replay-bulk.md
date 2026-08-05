## tandem-cli outbox replay-bulk

Replay all DONE/FAILED messages matching criteria

```
tandem-cli outbox replay-bulk [flags]
```

### Options

```
      --aggregate-id string     selector: aggregate id
      --aggregate-type string   selector: aggregate type
      --dry-run                 preview the matched count without changing anything
      --from-id int             selector: outbox id range start
  -h, --help                    help for replay-bulk
      --status stringArray      selector: eligible status (DONE or FAILED), repeatable
      --to-id int               selector: outbox id range end
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

