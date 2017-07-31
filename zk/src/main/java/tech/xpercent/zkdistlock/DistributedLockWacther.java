package tech.xpercent.zkdistlock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.xpercent.zkdist.SimpleServer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Created by macxie on 7/31/17.
 * 新需求:
 * 公司内部多个服务都要访问一张表.导致数据不一致问题.
 * 考虑通过zookeeper对这张表进行共享锁管理.
 * 工作机制:
 * 所有请求同一份数据的服务,每次请求数据时,在zookeeper上注册自己的信息
 * 开发基于zookeeper的客户端程序对每个请求服务的id进行比较,比较策略不一而足,视不同需求实现.
 * 比如id最小时,允许其对数据的访问.同时修改自己在zookeeper中注册的id号.
 * <p>
 * 这里只是模拟一下表数据请求的服务
 */
public class DistributedLockWacther {

    private static final String connectionString = "arc1.server:2181,arc2.server:2181,arc3.server:2181";
    private static final int session = 2000;
    private static final String lockServers = "/lock";
    private static final Logger logger = LoggerFactory.getLogger(DistributedLockWacther.class);
    private volatile ArrayList<String> serverList;

    private String mypath = null;

    private ZooKeeper zk = null;

    // 连接zookeeper集群上(相当于zookeeper集群的客户端)
    public void connectZK() throws Exception {
        zk = new ZooKeeper(connectionString, session, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged && lockServers.equals(watchedEvent.getPath())) {
                        requestLocker();
                    }
                } catch (Exception e) {
                }
            }
        });

        //程序一启动,先在zookeeper中注册
        regist();

        // 获取父节点下所有子节点,并监听父节点,一旦有变化,回调process
        List<String> allServer = zk.getChildren(lockServers,true);
        if(allServer.size() == 1){
            doStuff();
            regist();
        }
    }

    // 请求共享锁
    public void requestLocker() throws Exception {
        List<String> serverList = zk.getChildren(lockServers,false);
        if(serverList == null || serverList.size() == 1){
            return ;
        }
        Collections.sort(serverList);
        if(serverList.indexOf(mypath) == 0){
            doStuff();
            regist();
        }
    }

    // 开始操作表,操作完成之后要删除自己的节点
    public void doStuff() throws Exception{
        try {
            System.out.println("开始访问数据");
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            //对表的操作完成后,删除节点的所有版本
            zk.delete(this.mypath,-1);
        }
    }

    // 将业务服务器注册到zookeeper集群上 或者 更新自己的节点序列号
    public void regist() throws Exception {
        mypath = zk.create(lockServers + "/server",null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(mypath);
    }

    public static void main(String[] args) throws Exception {
        DistributedLockWacther wacther = new DistributedLockWacther();
        wacther.connectZK();
        wacther.regist();
        wacther.requestLocker();
    }
}
