package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final List<Pattern> ignoredUrls;
  private final int maxDepth;
  private final PageParserFactory parserFactory;

  @Inject
  ParallelWebCrawler(
    Clock clock,
    @Timeout Duration timeout,
    @PopularWordCount int popularWordCount,
    @TargetParallelism int threadCount,
    @IgnoredUrls List<Pattern> ignoredUrls,
    @MaxDepth int maxDepth,
    PageParserFactory parserFactory) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.ignoredUrls = ignoredUrls;
    this.maxDepth = maxDepth;
    this.parserFactory = parserFactory;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    CrawlerRecursiveAction crawlerRecursiveAction = new CrawlerRecursiveAction.Builder()
      .setDeadline(deadline)
      .setClock(clock)
      .setTimeout(timeout)
      .setStartingUrls(startingUrls)
      .setIgnoredUrls(ignoredUrls)
      .setMaxDepth(maxDepth)
      .setParserFactory(parserFactory)
      .build();

    pool.invoke(crawlerRecursiveAction);
    Map<String, Integer> counts = crawlerRecursiveAction.getCounts();
    Set<String> visitedUrls = crawlerRecursiveAction.getVisitedUrls();

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
        .setWordCounts(counts)
        .setUrlsVisited(visitedUrls.size())
        .build();
    } else {
      return new CrawlResult.Builder()
        .setWordCounts(WordCounts.sort(counts, popularWordCount))
        .setUrlsVisited(visitedUrls.size())
        .build();
    }
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
