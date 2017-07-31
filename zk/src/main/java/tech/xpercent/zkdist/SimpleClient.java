package tech.xpercent.zkdist;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by macxie on 7/31/17.
 * Usage:
 * 1. package this class as a runnable jar, e.g. zkclient.jar
 * 2. java -jar zkclient.jar
 * 3. run this program will print all of alive server in zookeeper cluster
 */
public class SimpleClient {

    private static final String connectionString = "arc1.server:2181,arc2.server:2181,arc3.server:2181";
    private static final int session = 2000;
    private static final String crmServers = "/crm";
    private static final Logger logger = LoggerFactory.getLogger(SimpleServer.class);

    // 加volatile的意义何在?
    private volatile ArrayList<String> serverList = null;

    private ZooKeeper zk = null;

    // 连接到zookeeper集群上(相当于zookeeper集群的客户端)
    public void connectZK() throws Exception {
        zk = new ZooKeeper(connectionString, session, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServerList();
                } catch (Exception e) {
                }
            }
        });
    }

    // 获取系统中所有crm服务器,并监听其上下线动态
    public void getServerList() throws Exception {
        List<String> children = zk.getChildren(crmServers, true);
        ArrayList<String> servers = new ArrayList<String>();
        for (String child : children) {
            byte[] data = zk.getData(crmServers + "/" + child, false, null);
            servers.add(new String(data, "utf-8"));
        }
        serverList = servers;
        System.out.println(serverList);
    }

    public static void main(String[] args) throws Exception {
        SimpleClient client = new SimpleClient();
        client.connectZK();
        client.getServerList();
        client.handleCrmBusiness();
    }

    // 模拟业务服务器的工作
    public void handleCrmBusiness() throws Exception {
        System.out.println("client start working....");
        Thread.sleep(Long.MAX_VALUE);
    }
}
