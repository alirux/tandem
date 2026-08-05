## tandem-cli completion

Generate a shell autocompletion script

### Synopsis

Generates a script that adds tab-completion for tandem-cli's commands, subcommands, and flags to your shell. Run the subcommand for your shell (bash, zsh, fish, or powershell) and see its own --help for how to load the script. Editor/shell convenience only - it never talks to the Admin API.

### Options

```
  -h, --help   help for completion
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
* [tandem-cli completion bash](tandem-cli_completion_bash.md)	 - Generate the autocompletion script for bash
* [tandem-cli completion fish](tandem-cli_completion_fish.md)	 - Generate the autocompletion script for fish
* [tandem-cli completion powershell](tandem-cli_completion_powershell.md)	 - Generate the autocompletion script for powershell
* [tandem-cli completion zsh](tandem-cli_completion_zsh.md)	 - Generate the autocompletion script for zsh

