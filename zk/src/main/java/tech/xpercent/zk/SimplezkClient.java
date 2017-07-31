package tech.xpercent.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by macxie on 7/31/17.
 * Nothing special, just some crud operation api of zookeeper
 */
public class SimplezkClient {

    private static final String connectionString = "arc1.server:2181,arc2.server:2181,arc3.server:2181";
    private static final int session = 2000;

    private static final Logger logger = LoggerFactory.getLogger(SimplezkClient.class);

    private ZooKeeper zkClient = null;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectionString, session, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    zkClient.getChildren("/",true);
                } catch (Exception e) {
                }
                System.out.println(watchedEvent.getType()+"-----"+watchedEvent.getState()+"-----"+watchedEvent.getPath());
            }
        });

    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {
        nodeExists();
        try {
            String nodeString = zkClient.create( "/fourthnode", "everything".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("create node is {} ", nodeString);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void nodeExists() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/fourthnode", false);
        if(exists == null){
            logger.warn("\\{} node does not exists","/fourthnode");
        }else{
            logger.warn("\\{} node alread exists","/fourthnode");
        }
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children =  zkClient.getChildren("/", true);
        for(String child: children){
            logger.info("children are {},",child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void getZdata() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        byte[] data = zkClient.getData("/fourthnode", false, null);
        System.out.println(new String(data,"utf-8"));
    }

    @Test
    public void deleteNode() throws KeeperException, InterruptedException {
        //-1,表示删除所这个节点的所有版本
        zkClient.delete("/fourthnode",-1);
    }

    @Test
    public void setData() throws KeeperException, InterruptedException {
        zkClient.setData("/fourthnode","new content".getBytes(),-1);
    }

}
