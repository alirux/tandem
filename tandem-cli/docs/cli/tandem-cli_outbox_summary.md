## tandem-cli outbox summary

Outbox health summary: counts per status, plus lag

```
tandem-cli outbox summary [flags]
```

### Options

```
  -h, --help                help for summary
      --interval duration   refresh period with --watch (default 2s)
      --watch               refresh continuously instead of taking one reading
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

