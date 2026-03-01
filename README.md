# octodns-etcd

OctoDNS provider for **etcd** in SkyDNS format (compatible with the CoreDNS etcd plugin). I made this for my own use.

## Install

### uv

```bash
uv add 'octodns-etcd @ git+https://github.com/eudika/octodns-etcd.git'
# specific ref:
# uv add 'octodns-etcd @ git+https://github.com/eudika/octodns-etcd.git@master'
```

### pip

```bash
pip install 'octodns-etcd @ git+https://github.com/eudika/octodns-etcd.git'
```

### pyproject.toml

For a project managed by uv or pip:

```toml
dependencies = [
    "octodns-etcd @ git+https://github.com/eudika/octodns-etcd.git",
]
```

## OctoDNS config

Follow the [OctoDNS provider layout](https://github.com/octodns/octodns): define `providers` (with `class`, etc.) and `zones` (with `sources` and `targets`).

```yaml
providers:
  config:
    class: octodns.provider.yaml.YamlProvider
    directory: ./config
  etcd:
    class: octodns_etcd.EtcdProvider
    host: localhost
    port: 2379
    prefix: /skydns
    # default_ttl: 3600   # optional; used when etcd value has no ttl (populate)

zones:
  example.com.:
    sources:
      - config
    targets:
      - etcd
```

For PTR records, enable AutoARPA and add reverse zones to `zones`:

```yaml
manager:
  auto_arpa: true

providers:
  ...

zones:
  example.com.:
    sources:
      - config
    targets:
      - etcd
  168.192.in-addr.arpa.:
    sources:
      - auto-arpa
    targets:
      - etcd
  0.8.e.f.ip6.arpa.:
    sources:
      - auto-arpa
    targets:
      - etcd
```

## Supported record types

**A**, **AAAA**, **TXT**, **CNAME**, **MX**, **SRV**, **PTR**.

Layout matches [CoreDNS etcd](https://coredns.io/plugins/etcd/) and [SkyDNS](https://github.com/skynetservices/skydns).

### Key

`prefix` + reversed FQDN labels. Example: `www.example.com.` → `/skydns/com/example/www`.

- One record per name → single key (no suffix).
- Multiple records per name (e.g. several A values or A + AAAA) → `base/0`, `base/1`, …  
  If a bare key already exists, it is moved to `base/0` when a second record is added.

### Value (JSON per type)

Each etcd value is a JSON object. Optional fields (e.g. `ttl`) may be omitted. Common field: `ttl` (integer, seconds).

| Type | Fields | Example |
|------|--------|---------|
| A | `host` (IPv4) | `{"host":"192.168.1.10","ttl":300}` |
| AAAA | `host` (IPv6) | `{"host":"2001:db8::1","ttl":300}` |
| TXT | `text` | `{"text":"v=spf1 -all","ttl":300}` |
| CNAME | `host` (target FQDN) | `{"host":"www.example.com.","ttl":300}` |
| MX | `host` (exchange), `priority` (preference); optional `mail`: true for CoreDNS | `{"host":"mail.example.com.","priority":10,"ttl":300}` |
| SRV | `host` (target), `port`, `priority`, `weight` | `{"host":"srv.example.com.","port":8080,"priority":0,"weight":5,"ttl":300}` |
| PTR | `host` (target FQDN) | `{"host":"ptr.example.com.","ttl":300}` |

## Limitations

Both come from the CoreDNS etcd plugin, not this provider.

### Apex A/AAAA

For an A/AAAA query at the zone apex (e.g. `example.com`), the CoreDNS etcd plugin returns *all* A/AAAA records in the zone in that single response. So the answer for the apex is mixed with IPs that actually belong to other names (e.g. `www.example.com`, `ns.example.com`). Avoid apex A/AAAA records, or serve them from a higher-priority plugin (e.g. hosts).

### Multiple PTR per address

DNS allows multiple PTRs per address; the CoreDNS etcd plugin does not.

## Develop

```bash
uv sync --extra dev
uv run ruff check src tests && uv run ruff format src tests
uv run pytest tests -q
```
