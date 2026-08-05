## tandem-cli

Inspect and act on a Tandem outbox and relay

### Synopsis

Inspect and act on a Tandem outbox and relay

Copyright 2026 Alberto Lirussi
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
software except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, this software is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations.

### Options

```
      --api-key string       API key (or set TANDEM_ADMIN_API_KEY)
      --base-url string      Admin API base URL (required; or set TANDEM_ADMIN_URL)
      --ca-cert string       PEM file to verify the server against (or set TANDEM_ADMIN_CA_CERT)
      --header stringArray   extra request header 'Name: value' (repeatable)
  -h, --help                 help for tandem-cli
      --insecure             skip TLS certificate verification
      --output string        output format: human or json (default "human")
      --timeout duration     per-request timeout; 0 disables it (default 1m0s)
      --token string         bearer token (or set TANDEM_ADMIN_TOKEN)
      --yes                  confirm destructive actions without prompting
```

### SEE ALSO

* [tandem-cli completion](tandem-cli_completion.md)	 - Generate a shell autocompletion script
* [tandem-cli outbox](tandem-cli_outbox.md)	 - Inspect and act on outbox messages
* [tandem-cli relay](tandem-cli_relay.md)	 - Inspect and control the relay

