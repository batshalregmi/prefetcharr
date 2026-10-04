# Season-pack fork

This fork preserves the current season's Sonarr episode monitoring states.
Replace your existing Compose image with:

```yaml
image: ghcr.io/batshalregmi/prefetcharr:latest
```

The publishing workflow also creates a seven-character commit tag and supports
`linux/amd64`. Configuration, entrypoint and runtime layout
are unchanged.

## Behavior

`Actor::run_prefetch` previously included current-season episodes in searches.
`Client::search_season` monitors every episode of a searched season, either
explicitly or through Sonarr's season-toggle propagation. The insufficient
episode fallback, `monitor_unannounced_episodes`, also toggled the last season
and then restored episode states, leaving a window where watched episodes
could be monitored.

The fork keeps the original contiguous episode window and configured count,
then excludes episodes in the current or earlier seasons from acquisition.
Future completed seasons retain season searches; future airing seasons retain
episode searches. If the window is shorter than the configured count and the
next season exists in Sonarr, `monitor_future_season` enables announcements
only in that future season and restores its existing episode states.
Current-season monitoring is never toggled or restored.

With a count of two and a twelve-episode season, E11 reaches the first episode
of the next season. A count of one reaches it at E12. Missing files in the
current season are deliberately left for Sonarr or manual handling. Specials
remain skipped. The queue and existing playback-key deduplication are unchanged;
different playback episodes can still trigger another future-season search,
as they could upstream. Seasons absent from Sonarr are left untouched; this
fork no longer globally enables monitoring of all newly added seasons.

Existing future-season episodes may still be monitored by the normal future
acquisition mechanism, including when a whole-season search is requested.
The future announcement toggle/restoration has the same non-atomic limitation
as upstream, but never targets the current season.

## Publishing and access

Pushes to the fork's default branch, `latest`, validate formatting, Clippy,
build and tests before publishing using `GITHUB_TOKEN` with `packages: write`.
No registry password secret is needed. Clippy retains the existing upstream
`unused_async_trait_impl` warnings while treating other warnings as errors.

If the package is private, open your GitHub profile's Packages page, select
`prefetcharr`, open Package settings, and change visibility to Public. Public
images need no Docker login. For a private package, use a classic GitHub token
with `read:packages` on the Docker host:

```sh
printf '%s' "$GHCR_TOKEN" | docker login ghcr.io -u batshalregmi --password-stdin
```

The `upstream` remote points to `https://github.com/p-hueber/prefetcharr.git`.
