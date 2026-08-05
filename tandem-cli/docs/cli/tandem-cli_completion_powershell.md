## tandem-cli completion powershell

Generate the autocompletion script for powershell

### Synopsis

Generate the autocompletion script for powershell.

To load completions in your current shell session:

	tandem-cli completion powershell | Out-String | Invoke-Expression

To load completions for every new session, add the output of the above command
to your powershell profile.


```
tandem-cli completion powershell [flags]
```

### Options

```
  -h, --help              help for powershell
      --no-descriptions   disable completion descriptions
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

* [tandem-cli completion](tandem-cli_completion.md)	 - Generate a shell autocompletion script

