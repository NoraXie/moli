package tech.xpercent.zkdist;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by macxie on 7/31/17.
 * This is a client of zookeeper cluster which accept bussiness server register.
 * Each bussiness server send their meta data to zookeeper through this client when they startup.
 *
 * Usage:
 * 1. package this class as a runnable jar, e.g zkserver.jar
 * 2. java -jar zkserver.jar [argument]
 * 3. all of your server need to be listener shoulb run this jar, the argument should be easy to known by your team.
 */
public class SimpleServer {

    private static final String connectionString = "arc1.server:2181,arc2.server:2181,arc3.server:2181";
    private static final int session = 2000;
    private static final String crmServers = "/crm/";
    private static final Logger logger = LoggerFactory.getLogger(SimpleServer.class);

    private ZooKeeper zk = null;

    // 连接zookeeper集群上(相当于zookeeper集群的客户端)
    public void connectZK() throws Exception {
        zk = new ZooKeeper(connectionString, session, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "-----" + watchedEvent.getState() + "-----" + watchedEvent.getPath());
                try {
                    zk.getChildren("/", true);
                } catch (Exception e) {
                }
            }
        });
    }

    // 将业务服务器注册到zookeeper集群上
    public void regist(String hostname) throws Exception {
        String s = zk.create(crmServers + "server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(s);
    }


    // 模拟业务服务器的工作
    public void handleCrmBusiness(String hostname) throws Exception {
        System.out.println(hostname + "start working....");
        Thread.sleep(Long.MAX_VALUE);
    }

    // zookeeper集群客户端启动,如果这个客户端停止服务,在zookeeper上注册的临时服务器节点数据会消失
    public static void main(String[] args) throws Exception {

        SimpleServer simpleServer = new SimpleServer();
        //连接zookeeper集群
        simpleServer.connectZK();
        //注册业务服务器
        simpleServer.regist(args[0]);
        //启动业务服务器
        simpleServer.handleCrmBusiness(args[0]);

    }

}