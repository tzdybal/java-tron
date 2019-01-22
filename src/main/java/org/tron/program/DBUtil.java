package org.tron.program;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.core.Constant;
import org.tron.core.capsule.ColumnManager;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.Manager;
import org.tron.core.exception.BadItemException;
import org.tron.core.exception.HeaderNotFound;
import org.tron.core.exception.ItemNotFoundException;
import org.tron.core.services.RpcApiService;
import org.tron.core.services.WitnessService;
import org.tron.core.services.http.FullNodeHttpApiService;
import org.tron.core.services.interfaceOnSolidity.RpcApiServiceOnSolidity;
import org.tron.core.services.interfaceOnSolidity.http.solidity.HttpApiOnSolidityService;

@Slf4j(topic = "app")
public class DBUtil {

  /**
   * Start the FullNode.
   */
  public static void main(String[] args) {
    logger.info("Full node running.");
    Args.setParam(args, Constant.TESTNET_CONF);
    Args cfgArgs = Args.getInstance();

    DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
    beanFactory.setAllowCircularReferences(false);
    TronApplicationContext context =
            new TronApplicationContext(beanFactory);
    context.register(DefaultConfig.class);

    context.refresh();
    Application appT = ApplicationFactory.create(context);
    shutdown(appT);

    Manager dbManager = appT.getDbManager();
    String prefix = "exploded";
    try {
      explodeBlocks(dbManager, prefix);
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    appT.initServices(cfgArgs);
    appT.startServices();
    appT.startup();


  }

  private static void explodeBlocks(Manager dbManager, String prefix) throws HeaderNotFound, IOException {
    ColumnManager columnManager = new ColumnManager(prefix);

    BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(prefix + File.separator + "dump.protobuf"), 100*1024*1024);
    for (long num = dbManager.getGenesisBlock().getNum(); num < dbManager.getHead().getNum(); ++num) {
      try {
        if (num % 10000 == 0) {
          System.out.print("\nProcessing block " + num);
        } else if (num % 1000 == 0) {
          System.out.print('.');
        }

        dbManager.getBlockByNum(num).saveTo(columnManager);
        stream.write(dbManager.getBlockByNum(num).getData());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    columnManager.close();
  }

  public static void shutdown(final Application app) {
    logger.info("********register application shutdown hook********");
    Runtime.getRuntime().addShutdownHook(new Thread(app::shutdown));
  }
}
