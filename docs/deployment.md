# Production Deployment

The bundled `docker-compose.yml` is a minimal standalone setup. For
production, put reverse-proxy config, TLS, and auth in a local
`docker-compose.override.yml` (already gitignored), for example:

```yaml
services:
  app:
    ports: !reset []
    labels:
      - traefik.enable=true
      - traefik.http.routers.energy.rule=Host(`energy.example.com`)
      - traefik.http.routers.energy.tls.certresolver=letsencrypt
    networks:
      - reverse-proxy
    environment:
      CORS_ORIGINS: https://energy.example.com

networks:
  reverse-proxy:
    external: true
```

The app has no built-in authentication — put it behind something (OIDC proxy,
basic auth, Tailscale, etc.) before exposing it to the internet.
