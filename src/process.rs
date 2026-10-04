use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::anyhow;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{
    Message,
    media_server::{EpisodeRef, NowPlaying, PrefetchKey, Queue, Series},
    sonarr,
    util::once::Seen,
};

struct Pending {
    series: Series,
    // BTreeSet so iteration is always (season, episode) ascending — the
    // backend Queue::append impls must receive episodes in playback order.
    owed: BTreeSet<EpisodeRef>,
    last_seen: Instant,
}

pub struct Actor {
    rx: mpsc::Receiver<Message>,
    sonarr_client: sonarr::Client,
    seen: Seen<PrefetchKey>,
    prefetch_num: usize,
    request_seasons: bool,
    exclude_tag: Option<sonarr::Tag>,
    queue: Option<Arc<dyn Queue + Send + Sync>>,
    pending: HashMap<String, Pending>,
    has_pending: Arc<AtomicBool>,
    pending_ttl: Duration,
}

impl Actor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rx: mpsc::Receiver<Message>,
        sonarr_client: sonarr::Client,
        seen: Seen<PrefetchKey>,
        prefetch_num: usize,
        request_seasons: bool,
        exclude_tag: Option<String>,
        queue: Option<Arc<dyn Queue + Send + Sync>>,
        has_pending: Arc<AtomicBool>,
        pending_ttl: Duration,
    ) -> Self {
        let exclude_tag = exclude_tag.map(sonarr::Tag::from);
        Self {
            rx,
            sonarr_client,
            seen,
            prefetch_num,
            request_seasons,
            exclude_tag,
            queue,
            pending: HashMap::new(),
            has_pending,
            pending_ttl,
        }
    }
}

impl Actor {
    pub async fn process(&mut self) {
        let mut gc = tokio::time::interval(self.pending_ttl / 4);
        gc.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(Message::NowPlaying(np)) => {
                            if let Err(e) = self.prefetch(np).await {
                                error!(err = ?e, "Failed to process");
                            }
                        }
                        None => break,
                    }
                }
                _ = gc.tick() => {
                    self.gc_pending();
                    self.publish_has_pending();
                }
            }
        }
    }

    pub async fn prefetch(&mut self, np: NowPlaying) -> anyhow::Result<()> {
        self.refresh_pending(&np);

        let result = self.run_prefetch(&np).await;

        if let Ok(Some(pairs)) = &result
            && let Some(sid) = np.session_id.clone()
            && !pairs.is_empty()
        {
            let entry = self.pending.entry(sid).or_insert_with(|| Pending {
                series: np.series.clone(),
                owed: BTreeSet::new(),
                last_seen: Instant::now(),
            });
            entry.owed.extend(pairs.iter().copied());
        }

        // Always run flush + GC, even if Sonarr work failed — pending state
        // is independent of Sonarr success.
        self.flush_queue(&np).await;
        self.gc_pending();
        self.publish_has_pending();

        result.map(|_| ())
    }

    async fn run_prefetch(&mut self, np: &NowPlaying) -> anyhow::Result<Option<Vec<EpisodeRef>>> {
        if !self.seen.once(PrefetchKey::from(np)) {
            debug!(now_playing = ?np, "skip previously processed item");
            return Ok(None);
        }

        // find series
        let mut series = self.find_series(np).await?;

        info!(title = series.title.clone().unwrap_or_else(|| "?".to_string()), now_playing = ?np);

        // Resolve and match exclusion tag
        if let Some(exclude_tag) = &mut self.exclude_tag {
            self.sonarr_client.update_tag(exclude_tag).await;
            if let Some(true) = series.is_tagged_with(exclude_tag) {
                info!("excluded via tag");
                return Ok(None);
            }
        }

        // Season 0 contains specials without any particular order.
        if np.season == 0 {
            return Ok(None);
        }

        // fetch n next episodes
        let episodes = self
            .sonarr_client
            .episode_range(&series, np.season, np.episode, self.prefetch_num)
            .await?;

        if episodes.len() < self.prefetch_num {
            info!(
                season = np.season,
                "Insufficient announced episodes; skipping current-season monitoring and search to preserve Sonarr monitor state"
            );
            if series.season(np.season + 1).is_some() {
                self.sonarr_client
                    .monitor_future_season(&mut series, np.season + 1)
                    .await?;
            }
        }

        let missing_episodes: Vec<_> = episodes.into_iter().filter(|e| !e.has_file).collect();
        let pairs: Vec<EpisodeRef> = missing_episodes
            .iter()
            .map(|e| EpisodeRef::new(e.season_number, e.episode_number))
            .collect();
        // Keep the original window (and queue behavior) but only acquire future
        // seasons. Filtering before taking the window would prefetch too early.
        let missing_episodes: Vec<_> = missing_episodes
            .into_iter()
            .filter(|e| e.season_number > np.season)
            .collect();
        if missing_episodes.is_empty() {
            return Ok(Some(pairs));
        }
        info!(season = np.season, "Evaluating future season prefetch");
        if !series.monitored {
            series.monitored = true;
            self.sonarr_client.put_series(&series).await?;
        }
        let mut episodes_to_search = Vec::new();

        if self.request_seasons {
            let mut seasons_to_search: HashSet<i32> = HashSet::new();

            for e in missing_episodes {
                if series
                    .season(e.season_number)
                    .is_some_and(sonarr::SeasonResource::is_fully_aired)
                {
                    seasons_to_search.insert(e.season_number);
                } else {
                    episodes_to_search.push(e);
                }
            }

            let mut season_numbers: Vec<_> = seasons_to_search.into_iter().collect();
            season_numbers.sort_unstable();

            let mut error = false;
            for season_num in season_numbers {
                if let Err(err) = self
                    .sonarr_client
                    .search_season(&mut series, season_num)
                    .await
                {
                    error!("skip searching for season {season_num}: {err:#}");
                    error = true;
                }
            }
            if error {
                return Err(anyhow!("failed searching one or more seasons"));
            }
        } else {
            episodes_to_search = missing_episodes;
        }

        if !episodes_to_search.is_empty() {
            let episodes_to_search: Vec<_> = episodes_to_search
                .into_iter()
                .map(|mut e| {
                    e.monitored = true;
                    e
                })
                .collect();
            self.sonarr_client
                .update_episode_monitoring(&episodes_to_search)
                .await?;
            self.sonarr_client
                .search_episodes(&episodes_to_search)
                .await?;
        }

        Ok(Some(pairs))
    }

    fn refresh_pending(&mut self, np: &NowPlaying) {
        let Some(sid) = np.session_id.as_deref() else {
            return;
        };
        if let Some(p) = self.pending.get_mut(sid) {
            // If the user switched to a different series within the same
            // session, drop stale owed pairs — they belong to the previous
            // show and would never be resolvable in the new context.
            if p.series != np.series {
                p.series = np.series.clone();
                p.owed.clear();
            }
            p.last_seen = Instant::now();
        }
    }

    async fn flush_queue(&mut self, np: &NowPlaying) {
        let Some(queue) = self.queue.clone() else {
            return;
        };
        let Some(sid) = np.session_id.as_deref() else {
            return;
        };
        let Some(pending) = self.pending.get(sid) else {
            return;
        };
        if pending.owed.is_empty() {
            return;
        }
        let owed: Vec<EpisodeRef> = pending.owed.iter().copied().collect();
        match queue.append(np, &owed).await {
            Ok(success) => {
                if let Some(entry) = self.pending.get_mut(sid) {
                    entry.owed.retain(|p| !success.contains(p));
                    if entry.owed.is_empty() {
                        self.pending.remove(sid);
                    }
                }
            }
            Err(e) => {
                warn!(err = ?e, session_id = sid, "queue append failed");
            }
        }
    }

    fn gc_pending(&mut self) {
        let now = Instant::now();
        let ttl = self.pending_ttl;
        self.pending
            .retain(|_, p| now.saturating_duration_since(p.last_seen) <= ttl);
    }

    fn publish_has_pending(&self) {
        self.has_pending
            .store(!self.pending.is_empty(), Ordering::Relaxed);
    }

    async fn find_series(
        &mut self,
        np: &NowPlaying,
    ) -> Result<sonarr::SeriesResource, anyhow::Error> {
        let series = self.sonarr_client.series().await?;
        let series = series
            .into_iter()
            .find(|s| match &np.series {
                Series::Title(t) => s.title.as_ref() == Some(t),
                Series::Tvdb(i) => &s.tvdb_id == i,
            })
            .ok_or_else(|| anyhow!("series not found in Sonarr"))?;
        Ok(series)
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashSet,
        sync::{Arc, Mutex, atomic::AtomicBool, atomic::Ordering},
        time::Duration,
    };

    use futures::{FutureExt, future::BoxFuture};
    use serde_json::json;
    use tokio::sync::mpsc;

    use crate::{
        fake_sonarr::{FakeSonarr, make_episode, make_season, make_series},
        media_server::{EpisodeRef, NowPlaying, Queue, Series, test::np_default},
        util::once,
    };

    type AppendCall = (String, Vec<EpisodeRef>);

    #[derive(Default)]
    struct FakeQueue {
        calls: Mutex<Vec<AppendCall>>,
        next_success: Mutex<Option<HashSet<EpisodeRef>>>,
    }

    impl FakeQueue {
        fn calls(&self) -> Vec<AppendCall> {
            self.calls.lock().unwrap().clone()
        }

        fn override_next(&self, success: HashSet<EpisodeRef>) {
            *self.next_success.lock().unwrap() = Some(success);
        }
    }

    impl Queue for FakeQueue {
        fn append(
            &self,
            np: &NowPlaying,
            episodes: &[EpisodeRef],
        ) -> BoxFuture<'_, anyhow::Result<HashSet<EpisodeRef>>> {
            let sid = np.session_id.clone().unwrap_or_default();
            let pairs: Vec<EpisodeRef> = episodes.to_vec();
            self.calls.lock().unwrap().push((sid, pairs.clone()));
            let success = self
                .next_success
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| pairs.into_iter().collect());
            async move { Ok(success) }.boxed()
        }
    }

    fn actor_with_queue(
        fake: &FakeSonarr,
        prefetch_num: usize,
        queue: Arc<FakeQueue>,
        has_pending: Arc<AtomicBool>,
        pending_ttl: Duration,
    ) -> super::Actor {
        let (_tx, rx) = mpsc::channel(1);
        let sonarr = crate::sonarr::Client::new(fake.url(), "secret").unwrap();
        super::Actor::new(
            rx,
            sonarr,
            once::Seen::default(),
            prefetch_num,
            false,
            None,
            Some(queue as Arc<dyn Queue + Send + Sync>),
            has_pending,
            pending_ttl,
        )
    }

    fn default_series() -> serde_json::Value {
        make_series(
            1234,
            "TestShow",
            5678,
            &[
                make_season(0, false, true),
                make_season(1, false, true),
                make_season(2, false, true),
            ],
        )
    }

    fn default_episodes() -> Vec<serde_json::Value> {
        let mut eps = Vec::new();
        for s in 1..=2 {
            for e in 1..=8 {
                eps.push(make_episode(s * 10 + e, 1234, s, e, false));
            }
        }
        eps
    }

    fn actor(fake: &FakeSonarr, prefetch_num: usize, request_seasons: bool) -> super::Actor {
        actor_with_tag(fake, prefetch_num, request_seasons, None)
    }

    #[tokio::test]
    async fn preserve_current_season_monitor_state() -> anyhow::Result<()> {
        for request_seasons in [false, true] {
            for playback in [5, 11, 12] {
                for has_file in [false, true] {
                    for future_airing in [false, true] {
                        let fake = FakeSonarr::start().await;
                        let mut series = make_series(
                            1234,
                            "TestShow",
                            5678,
                            &[
                                make_season(3, false, true),
                                make_season(4, false, !future_airing),
                            ],
                        );
                        series["monitorNewItems"] = json!("none");
                        fake.add_series(series);
                        let current: Vec<_> = (1..=12)
                            .map(|e| {
                                let mut ep = make_episode(300 + e, 1234, 3, e, has_file);
                                ep["monitored"] = json!(e >= 11);
                                ep
                            })
                            .collect();
                        fake.add_episodes(current.clone());
                        fake.add_episodes(vec![
                            make_episode(401, 1234, 4, 1, false),
                            make_episode(402, 1234, 4, 2, false),
                        ]);
                        let mut actor = actor(&fake, 2, request_seasons);
                        let np = NowPlaying {
                            series: Series::Tvdb(5678),
                            season: 3,
                            episode: playback,
                            ..np_default()
                        };
                        actor.prefetch(np.clone()).await?;
                        let commands = fake.commands();
                        actor.prefetch(np).await?;
                        assert_eq!(
                            commands,
                            fake.commands(),
                            "duplicate playback must not search again"
                        );
                        for ep in current {
                            assert_eq!(
                                fake.episode(i32::try_from(ep["id"].as_i64().unwrap()).unwrap()),
                                ep
                            );
                        }
                        assert_eq!(
                            fake.series_state(1234)["seasons"][0]["monitored"],
                            json!(false)
                        );
                        assert_eq!(fake.series_state(1234)["monitorNewItems"], json!("none"));
                        if playback == 5 {
                            assert!(commands.is_empty());
                        } else if request_seasons && !future_airing {
                            assert_eq!(
                                commands,
                                vec![
                                    json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 4})
                                ]
                            );
                        } else {
                            let ids = if playback == 11 {
                                vec![401]
                            } else {
                                vec![401, 402]
                            };
                            assert_eq!(
                                commands,
                                vec![json!({"name": "EpisodeSearch", "episodeIds": ids})]
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn future_season_without_announced_episodes() -> anyhow::Result<()> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        let episodes: Vec<_> = default_episodes()
            .into_iter()
            .filter(|e| e["seasonNumber"] == 1)
            .collect();
        fake.add_episodes(episodes.clone());
        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Tvdb(5678),
                season: 1,
                episode: 7,
                ..np_default()
            })
            .await?;
        assert!(fake.commands().is_empty());
        assert!(
            !fake.series_state(1234)["seasons"][1]["monitored"]
                .as_bool()
                .unwrap()
        );
        assert!(
            fake.series_state(1234)["seasons"][2]["monitored"]
                .as_bool()
                .unwrap()
        );
        for ep in episodes {
            assert_eq!(
                fake.episode(i32::try_from(ep["id"].as_i64().unwrap()).unwrap()),
                ep
            );
        }
        Ok(())
    }

    fn actor_with_tag(
        fake: &FakeSonarr,
        prefetch_num: usize,
        request_seasons: bool,
        exclude_tag: Option<String>,
    ) -> super::Actor {
        let (_tx, rx) = mpsc::channel(1);
        let sonarr = crate::sonarr::Client::new(fake.url(), "secret").unwrap();
        super::Actor::new(
            rx,
            sonarr,
            once::Seen::default(),
            prefetch_num,
            request_seasons,
            exclude_tag,
            None,
            Arc::new(AtomicBool::new(false)),
            Duration::from_secs(3600),
        )
    }

    // The original window crosses into the next season; only that season is searched
    #[tokio::test]
    #[test_log::test]
    async fn search_next() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(
            !fake.series_state(1234)["seasons"][1]["monitored"]
                .as_bool()
                .unwrap()
        );
        for id in 11..=18 {
            assert!(!fake.episode(id)["monitored"].as_bool().unwrap());
        }
        assert!(
            fake.series_state(1234)["seasons"][2]["monitored"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 2})]
        );
        Ok(())
    }

    // With request_seasons disabled, prefetches individual episodes across season boundaries
    #[tokio::test]
    #[test_log::test]
    async fn search_episodes() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 3, false)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.episode(18)["monitored"].as_bool().unwrap());
        assert!(fake.episode(21)["monitored"].as_bool().unwrap());
        assert!(fake.episode(22)["monitored"].as_bool().unwrap());
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [21, 22]})]
        );
        Ok(())
    }

    // Final season shortage leaves monitoring unchanged and issues no search
    #[tokio::test]
    #[test_log::test]
    async fn search_episodes_exceeding() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 3, false)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 2,
                ..np_default()
            })
            .await?;

        // No final-season fallback
        assert_eq!(fake.series_state(1234), default_series());
        for id in 21..=28 {
            assert!(!fake.episode(id)["monitored"].as_bool().unwrap());
        }
        assert!(fake.commands().is_empty());
        Ok(())
    }

    // Mid-season playback does not acquire current-season episodes
    #[tokio::test]
    #[test_log::test]
    async fn pilot() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 1,
                season: 1,
                ..np_default()
            })
            .await?;

        // Current-season monitor state stays authoritative
        assert_eq!(fake.series_state(1234), default_series());
        assert!(fake.commands().is_empty());
        Ok(())
    }

    // Season 0 (specials) is skipped without triggering any searches or monitoring changes
    #[tokio::test]
    #[test_log::test]
    async fn special_episode() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 1,
                season: 0,
                ..np_default()
            })
            .await?;

        assert!(!fake.series_state(1234)["monitored"].as_bool().unwrap());
        assert!(fake.commands().is_empty());
        Ok(())
    }

    // An already-monitored current season still preserves unmonitored episodes
    #[tokio::test]
    #[test_log::test]
    async fn monitor_episodes_of_monitored_season() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        let mut series = default_series();
        series["seasons"][1]["monitored"] = true.into();
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 1,
                season: 1,
                ..np_default()
            })
            .await?;

        // Current-season episodes must remain unmonitored
        for id in 11..=18 {
            assert!(!fake.episode(id)["monitored"].as_bool().unwrap());
        }
        assert!(fake.commands().is_empty());
        Ok(())
    }

    // Current-season airing episodes must not be searched
    #[tokio::test]
    #[test_log::test]
    async fn search_season_not_fully_aired() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        let mut series = default_series();
        series["monitored"] = true.into();
        series["seasons"][1] = make_season(1, false, false);
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor(&fake, 1, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.episode(18)["monitored"].as_bool().unwrap());
        assert!(fake.commands().is_empty());
        Ok(())
    }

    // Fully aired seasons use season search while still-airing seasons fall back to episode search
    #[tokio::test]
    #[test_log::test]
    async fn search_season_mixed() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        let mut series = default_series();
        series["monitored"] = true.into();
        series["seasons"][1] = make_season(1, false, false);
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor(&fake, 2, true)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        // Season 2 fully aired → SeasonSearch
        assert!(!fake.episode(18)["monitored"].as_bool().unwrap());
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 2})]
        );
        Ok(())
    }

    // Series with a matching exclusion tag is skipped without any searches
    #[tokio::test]
    #[test_log::test]
    async fn exclude_tag_skips_series() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_tag(1, "no-prefetch");
        let mut series = default_series();
        series["tags"] = json!([1]);
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor_with_tag(&fake, 2, true, Some("no-prefetch".to_string()))
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.series_state(1234)["monitored"].as_bool().unwrap());
        assert!(fake.commands().is_empty());
        Ok(())
    }

    // Series without the exclusion tag is processed normally
    #[tokio::test]
    #[test_log::test]
    async fn exclude_tag_allows_untagged_series() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_tag(1, "no-prefetch");
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor_with_tag(&fake, 1, true, Some("no-prefetch".to_string()))
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 8,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.commands().is_empty());
        Ok(())
    }

    // Unresolved exclusion tag label does not exclude any series
    #[tokio::test]
    #[test_log::test]
    async fn exclude_tag_unresolved() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        let mut series = default_series();
        series["tags"] = json!([1]);
        fake.add_series(series);
        fake.add_episodes(default_episodes());

        actor_with_tag(&fake, 1, true, Some("no-prefetch".to_string()))
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 8,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.commands().is_empty());
        Ok(())
    }

    // Duplicate NowPlaying for s01e07 is skipped on second prefetch call
    #[tokio::test]
    #[test_log::test]
    async fn deduplicate() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        let np = NowPlaying {
            series: Series::Title("TestShow".to_string()),
            episode: 8,
            season: 1,
            ..np_default()
        };

        let mut actor = actor(&fake, 1, true);
        actor.prefetch(np.clone()).await?;
        actor.prefetch(np).await?;

        assert_eq!(
            fake.commands(),
            vec![json!({"name": "SeasonSearch", "seriesId": 1234, "seasonNumber": 2})],
        );
        Ok(())
    }

    // Series can be found by TVDB ID instead of title
    #[tokio::test]
    #[test_log::test]
    async fn find_series_by_tvdb_id() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        actor(&fake, 1, true)
            .prefetch(NowPlaying {
                series: Series::Tvdb(5678),
                episode: 8,
                season: 1,
                ..np_default()
            })
            .await?;

        assert!(!fake.commands().is_empty());
        Ok(())
    }

    // Prefetching a series not in Sonarr returns an error
    #[tokio::test]
    #[test_log::test]
    async fn series_not_found() {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());

        let result = actor(&fake, 1, true)
            .prefetch(NowPlaying {
                series: Series::Title("Unknown".to_string()),
                episode: 1,
                season: 1,
                ..np_default()
            })
            .await;

        assert!(result.is_err());
    }

    // s01e08 already has a file so only s02e01 is searched
    #[tokio::test]
    #[test_log::test]
    async fn skip_downloaded_episodes() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        let mut eps = default_episodes();
        // Mark s01e08 as already downloaded
        eps[7]["hasFile"] = true.into();
        fake.add_episodes(eps);

        actor(&fake, 2, false)
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                ..np_default()
            })
            .await?;

        // s01e08 has_file=true so only s02e01 is searched
        assert_eq!(
            fake.commands(),
            vec![json!({"name": "EpisodeSearch", "episodeIds": [21]})]
        );
        Ok(())
    }

    // Queue::append is invoked with the prefetch range when a session_id is
    // present and the actor has a queue handle
    #[tokio::test]
    #[test_log::test]
    async fn queue_append_called() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        let queue = Arc::new(FakeQueue::default());
        let has_pending = Arc::new(AtomicBool::new(false));
        let mut actor = actor_with_queue(
            &fake,
            2,
            queue.clone(),
            has_pending.clone(),
            Duration::from_secs(3600),
        );

        actor
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                session_id: Some("s1".into()),
                ..np_default()
            })
            .await?;

        let calls = queue.calls();
        assert_eq!(calls.len(), 1);
        let (sid, pairs) = &calls[0];
        assert_eq!(sid, "s1");
        // Episodes must reach the backend in playback order, even across the
        // season boundary — the player would otherwise watch them out of
        // sequence.
        assert_eq!(pairs, &vec![EpisodeRef::new(1, 8), EpisodeRef::new(2, 1)]);
        // Default fake returns full success → has_pending falls back to false
        assert!(!has_pending.load(Ordering::Relaxed));
        Ok(())
    }

    // Episodes already on disk must not be appended to the play queue —
    // some clients (e.g. Emby) play a single item and resolve "next up"
    // locally, so re-enqueueing existing files fights with the client.
    #[tokio::test]
    #[test_log::test]
    async fn queue_skips_episodes_with_file() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        let mut eps = default_episodes();
        // s01e08 is already in the library
        eps[7]["hasFile"] = true.into();
        fake.add_episodes(eps);

        let queue = Arc::new(FakeQueue::default());
        let has_pending = Arc::new(AtomicBool::new(false));
        let mut actor = actor_with_queue(
            &fake,
            2,
            queue.clone(),
            has_pending.clone(),
            Duration::from_secs(3600),
        );

        actor
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                session_id: Some("s1".into()),
                ..np_default()
            })
            .await?;

        let calls = queue.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "s1");
        assert_eq!(calls[0].1, vec![EpisodeRef::new(2, 1)]);
        Ok(())
    }

    // Partial success keeps remaining pairs owed; subsequent prefetch calls
    // for the same session retry only the still-owed subset.
    #[tokio::test]
    #[test_log::test]
    async fn queue_partial_success_retries() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        let queue = Arc::new(FakeQueue::default());
        let has_pending = Arc::new(AtomicBool::new(false));
        let mut actor = actor_with_queue(
            &fake,
            2,
            queue.clone(),
            has_pending.clone(),
            Duration::from_secs(3600),
        );

        // First call: fake returns only s01e08 as success.
        queue.override_next([EpisodeRef::new(1, 8)].into_iter().collect());
        let np = NowPlaying {
            series: Series::Title("TestShow".to_string()),
            episode: 7,
            season: 1,
            session_id: Some("s1".into()),
            ..np_default()
        };
        actor.prefetch(np.clone()).await?;
        assert!(has_pending.load(Ordering::Relaxed));

        // Second call: fake returns full success — only (2, 1) is retried.
        actor.prefetch(np).await?;
        let calls = queue.calls();
        assert_eq!(calls.len(), 2);
        let second_pairs: HashSet<EpisodeRef> = calls[1].1.iter().copied().collect();
        assert_eq!(second_pairs, [EpisodeRef::new(2, 1)].into_iter().collect());
        assert!(!has_pending.load(Ordering::Relaxed));
        Ok(())
    }

    // Stale pending entries are evicted by GC after the TTL elapses, and
    // has_pending flips back to false.
    #[tokio::test]
    #[test_log::test]
    async fn queue_pending_gc() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        let queue = Arc::new(FakeQueue::default());
        let has_pending = Arc::new(AtomicBool::new(false));
        let mut actor = actor_with_queue(
            &fake,
            2,
            queue.clone(),
            has_pending.clone(),
            Duration::from_millis(50),
        );

        queue.override_next(HashSet::new());
        actor
            .prefetch(NowPlaying {
                series: Series::Title("TestShow".to_string()),
                episode: 7,
                season: 1,
                session_id: Some("s1".into()),
                ..np_default()
            })
            .await?;
        assert!(has_pending.load(Ordering::Relaxed));

        // Wait past TTL, then drive GC by processing a different unrelated event.
        tokio::time::sleep(Duration::from_millis(80)).await;
        // process something that doesn't add to pending — just any np without
        // session_id so the gc still runs.
        actor
            .prefetch(NowPlaying {
                series: Series::Tvdb(99999),
                episode: 1,
                season: 1,
                session_id: None,
                ..np_default()
            })
            .await
            .ok();

        assert!(!has_pending.load(Ordering::Relaxed));
        Ok(())
    }

    // No queue handle → no append calls; pending stays empty
    #[tokio::test]
    #[test_log::test]
    async fn queue_no_op_without_handle() -> Result<(), Box<dyn std::error::Error>> {
        let fake = FakeSonarr::start().await;
        fake.add_series(default_series());
        fake.add_episodes(default_episodes());

        // Default actor has no queue handle
        let mut a = actor(&fake, 2, false);
        a.prefetch(NowPlaying {
            series: Series::Title("TestShow".to_string()),
            episode: 7,
            season: 1,
            session_id: Some("s1".into()),
            ..np_default()
        })
        .await?;

        // No has_pending observable here, but no panic either.
        // The Sonarr search still happened, and that's covered by other tests.
        Ok(())
    }
}
