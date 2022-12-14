package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.regex.Pattern;

public class CrawlerRecursiveAction extends RecursiveAction {
  private static final Map<String, Integer> counts = Collections.synchronizedMap(new HashMap<>());
  private static final Set<String> visitedUrls = Collections.synchronizedSet(new HashSet<>());
  private final List<Pattern> ignoredUrls;
  private final Instant deadline;
  private final Duration timeout;
  private final Clock clock;
  private final PageParserFactory parserFactory;
  private final List<String> startingUrls;
  private List<String> toCrawlUrls;
  private int maxDepth;

  private CrawlerRecursiveAction(Instant deadline, Duration timeout, List<String> startingUrls, int maxDepth,
                                 Clock clock, List<Pattern> ignoredUrls, PageParserFactory parserFactory) {
    this.deadline = deadline;
    this.timeout = timeout;
    this.startingUrls = startingUrls;
    this.toCrawlUrls = startingUrls;
    this.maxDepth = maxDepth;
    this.clock = clock;
    this.ignoredUrls = ignoredUrls;
    this.parserFactory = parserFactory;
  }

  public Instant getDeadline() {
    return deadline;
  }

  public Duration getTimeout() {
    return timeout;
  }

  public List<String> getStartingUrls() {
    return startingUrls;
  }

  public int getMaxDepth() {
    return maxDepth;
  }

  public Clock getClock() {
    return clock;
  }

  public List<Pattern> getIgnoredUrls() {
    return ignoredUrls;
  }

  public PageParserFactory getParserFactory() {
    return parserFactory;
  }

  public Set<String> getVisitedUrls() {
    return visitedUrls;
  }

  public Map<String, Integer> getCounts() {
    return counts;
  }

  @Override
  protected void compute() {
    while (true) {
      if (isTimeOut()) {
        return;
      }
      if (toCrawlUrls.size() > 1) {
        ForkJoinTask.invokeAll(createSubtasks());
      } else {
        if (isMaxDepthReached() || isStartEmpty()) {
          return;
        }
        String url = startingUrls.get(0);
        if (isUrlIgnored(url) || visitedUrls.add(url)) return;
        PageParser.Result result = parserFactory.get(url).parse();
        synchronized (counts) {
          for (Map.Entry<String, Integer> entry : result.getWordCounts().entrySet()) {
            if (counts.containsKey(entry.getKey())) {
              counts.put(entry.getKey(),counts.get(entry.getKey()) + entry.getValue());
            } else {
              counts.put(entry.getKey(),counts.get(entry.getKey()));
            }
          }
        }
        toCrawlUrls = result.getLinks();
        maxDepth--;
      }
    }
  }

  private boolean isUrlIgnored(String url) {
    for (Pattern pat : ignoredUrls) {
      if (pat.matcher(url).matches()) {
        return true;
      }
    }
    return false;
  }

  private boolean isVisited(String url) {
    return visitedUrls.contains(url);
  }

  private boolean isStartEmpty() {
    return startingUrls.isEmpty();
  }

  private boolean isTimeOut() {
    return clock.instant().isAfter(deadline);
  }

  private boolean isMaxDepthReached() {
    return maxDepth == 0;
  }

  private List<CrawlerRecursiveAction> createSubtasks() {
    List<CrawlerRecursiveAction> subtasks = new ArrayList<>();
    int size = toCrawlUrls.size();
    for (int i = 1; i < size; i++) {
      subtasks.add(new CrawlerRecursiveAction
        .Builder()
        .setDeadline(deadline)
        .setStartingUrls(toCrawlUrls.subList(i, i + 1))
        .setMaxDepth(maxDepth - 1)
        .setClock(clock)
        .setTimeout(timeout)
        .setIgnoredUrls(ignoredUrls)
        .setParserFactory(parserFactory)
        .build());
    }
    toCrawlUrls = toCrawlUrls.subList(0, 1);
    return subtasks;
  }

  public static final class Builder {
    private List<Pattern> ignoredUrls;
    private Instant deadline;
    private Duration timeout;
    private List<String> startingUrls;
    private int maxDepth;
    private Clock clock;
    private PageParserFactory parserFactory;

    public Builder setIgnoredUrls(List<Pattern> ignoredUrls) {
      this.ignoredUrls = ignoredUrls;
      return this;
    }

    public Builder setDeadline(Instant deadline) {
      this.deadline = deadline;
      return this;
    }

    public Builder setTimeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder setStartingUrls(List<String> startingUrls) {
      this.startingUrls = startingUrls;
      return this;
    }

    public Builder setClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public Builder setMaxDepth(int maxDepth) {
      this.maxDepth = maxDepth;
      return this;
    }

    public Builder setParserFactory(PageParserFactory parserFactory) {
      this.parserFactory = parserFactory;
      return this;
    }

    public CrawlerRecursiveAction build() {
      return new CrawlerRecursiveAction(deadline, timeout, startingUrls, maxDepth, clock, ignoredUrls, parserFactory);
    }
  }
}
