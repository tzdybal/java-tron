package org.tron.program;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.Manager;
import org.tron.core.exception.HeaderNotFound;
import org.tron.core.exception.ItemNotFoundException;

import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Benchmark {
    private final Manager dbManager;

    private Benchmark(Manager dbManager) {
        this.dbManager = dbManager;
    }

    public void run(long n) throws HeaderNotFound {
        long head = dbManager.getHead().getNum();
        long genesis = dbManager.getGenesisBlock().getNum();

        for (int i = 0; i < 10; ++i) {
            Random random = new Random(i * 1337);

            // gather some random block ids
            System.out.println("Preparing test data...");
            Stream<BlockCapsule.BlockId> blockIds = random.longs(n, genesis, head).mapToObj(r -> {
                try {
                    return dbManager.getBlockIdByNum(r);
                } catch (ItemNotFoundException e) {
                    return new BlockCapsule.BlockId();
                }
            });

            System.out.println("Executing tests...");
            List<Long> stats = blockIds.map(blockId -> {
                long start = System.nanoTime();
                try {
                    dbManager.getBlockStore().get(blockId.getBytes());
                } catch (Exception e) {
                    System.err.println("can't get block:" + e.getMessage());
                }
                long end = System.nanoTime();
                return end - start;
            }).collect(Collectors.toList());

            LongSummaryStatistics summary = stats.stream().mapToLong(l -> l).summaryStatistics();

            System.out.println("\n=== Results ===");
            System.out.println("Number of blocks to read: " + n);
            System.out.println("Database size:            " + dbManager.getHead().getNum() + "[blocks]");
            System.out.println("Average block get time:   " + summary.getAverage() / 1000.0 + " [μs]");
            System.out.println("Minimum block get time:   " + summary.getMin() / 1000.0 + " [μs]");
            System.out.println("Maximum block get time:   " + summary.getMax() / 1000.0 + " [μs]");
            System.out.println("Histogram:");

            final long grouping = 10;
            stats.stream().reduce(new TreeMap<Long, Long>(), (acc, value) -> {
                long bucket = value / 1000 / grouping;
                acc.put(bucket * grouping, acc.getOrDefault(bucket * grouping, 0L) + 1);
                return acc;
            }, (a, b) -> {
                a.forEach((k, v) -> b.put(k, b.getOrDefault(k, 0L) + v));
                return b;
            }).forEach((b, c) -> System.out.println(b + ": " + c));
        }
    }

    public static void main(String[] args) {
        Args.setParam(args, Constant.TESTNET_CONF);
        Args.getInstance();

        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.setAllowCircularReferences(false);
        TronApplicationContext context =
                new TronApplicationContext(beanFactory);
        context.register(DefaultConfig.class);

        context.refresh();


        Application appT = ApplicationFactory.create(context);
        shutdown(appT);

        Benchmark benchmark = new Benchmark(appT.getDbManager());
        try {
            benchmark.run(100000);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        appT.shutdown();
        context.destroy();
    }

    public static void shutdown(final Application app) {
        logger.info("********register application shutdown hook********");
        Runtime.getRuntime().addShutdownHook(new Thread(app::shutdown));
    }
}
