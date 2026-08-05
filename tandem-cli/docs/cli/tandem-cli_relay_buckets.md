## tandem-cli relay buckets

Per-bucket ownership and lag - all buckets, or one

```
tandem-cli relay buckets [bucket] [flags]
```

### Options

```
  -h, --help             help for buckets
      --uncovered-only   list only buckets with no live owner (list form only)
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

* [tandem-cli relay](tandem-cli_relay.md)	 - Inspect and control the relay

