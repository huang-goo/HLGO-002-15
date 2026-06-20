package com.apptasticsoftware.rssreader.module.youtube;

import com.apptasticsoftware.rssreader.internal.DaemonThreadFactory;
import com.apptasticsoftware.rssreader.util.ItemComparator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class YoutubeFeedAggregator implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger("com.apptasticsoftware.rssreader");
    private final YoutubeFeedReader feedReader;
    private final ExecutorService executorService;
    private final boolean ownsExecutor;

    public YoutubeFeedAggregator() {
        this(new YoutubeFeedReader());
    }

    public YoutubeFeedAggregator(YoutubeFeedReader feedReader) {
        Objects.requireNonNull(feedReader, "YoutubeFeedReader must not be null");
        this.feedReader = feedReader;
        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new DaemonThreadFactory("YoutubeFeedAggregator")
        );
        this.ownsExecutor = true;
    }

    public YoutubeFeedAggregator(YoutubeFeedReader feedReader, ExecutorService executorService) {
        Objects.requireNonNull(feedReader, "YoutubeFeedReader must not be null");
        Objects.requireNonNull(executorService, "ExecutorService must not be null");
        this.feedReader = feedReader;
        this.executorService = executorService;
        this.ownsExecutor = false;
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public List<YoutubeItem> aggregate(Collection<String> feedUrls) throws IOException {
        return aggregate(feedUrls, Integer.MAX_VALUE);
    }

    public List<YoutubeItem> aggregate(Collection<String> feedUrls, int maxItems) throws IOException {
        Objects.requireNonNull(feedUrls, "Feed URLs must not be null");
        if (maxItems <= 0) {
            throw new IllegalArgumentException("maxItems must be greater than 0");
        }
        feedUrls.forEach(url -> Objects.requireNonNull(url, "Feed URL must not be null"));

        List<CompletableFuture<List<YoutubeItem>>> futures = feedUrls.stream()
                .map(url -> CompletableFuture.supplyAsync(() -> readFeed(url), executorService))
                .collect(Collectors.toList());

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        return futures.stream()
                .map(future -> {
                    try {
                        return future.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return Collections.<YoutubeItem>emptyList();
                    } catch (ExecutionException e) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.log(Level.WARNING, "Failed to aggregate feed", e);
                        }
                        return Collections.<YoutubeItem>emptyList();
                    }
                })
                .flatMap(Collection::stream)
                .sorted(ItemComparator.newestPublishedItemFirst())
                .limit(maxItems)
                .collect(Collectors.toList());
    }

    private List<YoutubeItem> readFeed(String url) {
        try (Stream<YoutubeItem> stream = feedReader.read(url)) {
            return stream.collect(Collectors.toList());
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.log(Level.WARNING, () -> String.format("Failed read URL %s. Message: %s", url, e.getMessage()));
            }
            return Collections.emptyList();
        }
    }
}
