package com.learn.zookeeper.zk;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** 
 * 功能描述
 * @author : 
 * @date 创建时间：2017年3月28日 下午3:41:23 
 * @version 1.0  
 */
public class ZkClient {

	private final static Logger LOGGER = LoggerFactory.getLogger(ZkClient.class);
    private String hosts;//zookeeper  地址列表
    private int sessionTimeout = 3000;//会话超时时间
    private int connTimeout;//连接超时
    private volatile boolean isConnection = false;//是否连接成功
    
    public boolean isConnection() {
		return isConnection;
	}

	public void setConnection(boolean isConnection) {
		this.isConnection = isConnection;
	}

	private ZooKeeper zk;
    
    private CountDownLatch connectLatch = new CountDownLatch(1);
    
    /**
     * 创建zookeeper客户端
     *
     * @param hosts zookeeper服务地址 10.0.1.121:2181,10.0.1.131:2181
     * @throws InterruptedException 
     */
    public ZkClient(String hosts) throws ZkClientException, InterruptedException {
        this(hosts, 3000, 3000);
    }

    /**
     * 创建zookeeper客户端
     *
     * @param hosts          zookeeper服务地址 10.0.1.121:2181,10.0.1.131:2181
     * @param sessionTimeout 会话超时时间
     * @param connTimeout    连接超时时间
     * @throws InterruptedException 
     */
    public ZkClient(String hosts, int sessionTimeout, int connTimeout) throws ZkClientException, InterruptedException {
        this(hosts, sessionTimeout, connTimeout, 1);
    }

    /**
     * @param hosts             zookeeper服务地址 10.0.1.121:2181,10.0.1.131:2181
     * @param sessionTimeout    会话超时时间
     * @param connTimeout       连接超时时间
     * @param watcherThreadSize 处理watcher的线程数
     * @throws ZkClientException
     * @throws InterruptedException 
     */
    public ZkClient(String hosts, int sessionTimeout, int connTimeout, int watcherThreadSize) throws ZkClientException, InterruptedException {
        this.hosts = hosts;
        this.sessionTimeout = sessionTimeout;
        this.connTimeout = connTimeout;
        this.connection();
    }
    
    /**
     * 同步阻塞连接zookeeper
     *
     * @throws ZkClientException
     * @throws InterruptedException 
     */
    private synchronized void connection() throws ZkClientException, InterruptedException {
        if (this.checkConnection()) {
            throw new ZkClientException("Has been connected to the server, please do not repeat connection. host:" + hosts);
        }
        try {
            zk = new ZooKeeper(hosts, sessionTimeout,new Watcher(){
				public void process(WatchedEvent event) {
					if(event.getState().equals(KeeperState.SyncConnected)){
						setConnection(true);
						LOGGER.info("服务器已连接！");
						connectLatch.countDown();
					}
				}
            	
            });
            connectLatch.await();
        } catch (IOException e) {
            throw new ZkClientException("Connect zookeeper fail, hosts=" + hosts, e);
        }
    }
    
    /**
     * 重连zookeeper
     *
     * @throws ZkClientException
     * @throws InterruptedException 
     */
    public void reconnection() throws ZkClientException, InterruptedException {
        this.connection();
    }
    
    /**
     * 获取节点下的数据
     *
     * @param path 节点路径
     * @return
     */
    public byte[] getData(String path) throws ZkClientException {
        return getData(path, false);
    }

    /**
     * 获取节点下的数据
     *
     * @param path    节点路径
     * @param watcher 是否对该节点进行数据变动监听（只能收到一次变动消息）
     * @return
     * @throws ZkClientException
     */
    public byte[] getData(String path, boolean watcher) throws ZkClientException {
        this.checkStatus();
        try {
            return this.zk.getData(path, watcher, null);
        } catch (Exception e) {
            throw new ZkClientException("getData node " + path, e);
        }
    }

    /**
     * 插入数据
     *
     * @param path
     * @param data
     * @throws ZkClientException
     */
    public void setData(String path, byte[] data) throws ZkClientException {
        this.checkStatus();
        try {
            this.zk.setData(path, data, -1);
        } catch (Exception e) {
            throw new ZkClientException("setData node " + path, e);
        }
    }

    /**
     * 获取child节点信息
     *
     * @param path
     * @throws ZkClientException
     */
    protected List<String> getChild(String path) throws ZkClientException {
        return this.getChild(path, false);
    }

    /**
     * 获取child节点信息
     *
     * @param path
     * @throws ZkClientException
     */
    public List<String> getChild(String path, boolean watcher) throws ZkClientException {
        this.checkStatus();
        try {
            return this.zk.getChildren(path, watcher);
        } catch (Exception e) {
            throw new ZkClientException("getChildren node " + path, e);
        }
    }

    /**
     * 创建节点
     * 不支持多层节点创建
     *
     * @param path 节点路径
     * @param data 节点数据
     * @param mode 节点类型 CreateMode.PERSISTENT 永久节点，CreateMode.EPHEMERAL临时节点
     */
    public String create(String path, byte[] data, CreateMode mode) throws ZkClientException {
        this.checkStatus();
        String createNode;
        try {
            createNode = this.zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        } catch (Exception e) {
            throw new ZkClientException("create node " + path + ", mode=" + mode.name(), e);
        }
        return createNode;
    }

    /**
     * 创建一个临时节点
     * 不支持多层节点创建
     *
     * @param path     节点路径
     * @param data     节点数据
     */
    public void createEphemeral(String path, byte[] data) throws ZkClientException {
        this.create(path, data, CreateMode.EPHEMERAL);
        
    }

    /**
     * 创建一个永久节点
     * 不支持多层节点创建
     *
     * @param path 路径
     * @param data 数据
     * @throws ZkClientException
     */
    public void createPersistent(String path, byte[] data) throws ZkClientException {
        this.create(path, data, CreateMode.PERSISTENT);
    }

    /**
     * 支持多层节点创建
     *
     * @param path
     * @param mode
     * @throws ZkClientException
     */
    public String create(String path, CreateMode mode) throws ZkClientException {
        if (path != null && !path.trim().equals("")) {
            if (exists(path)) {
                throw new ZkClientException("Node path: " + path + " already exists.");
            }
            String[] paths = path.trim().split("/");
            String p = "";
            for (int i = 0; i < paths.length; i++) {
                String s = paths[i];
                if (s != null && !s.equals("")) {
                    p += "/" + s;
                    if (!exists(p)) {
                        if (i < paths.length - 1) {
                            this.create(p, new byte[1], CreateMode.PERSISTENT);
                        } else {
                            this.create(p, new byte[1], mode);
                        }
                    }
                }
            }
        }
        return path;
    }

    /**
     * 删除节点
     *
     * @param path 路径
     * @throws ZkClientException
     */
    public void delete(String path) throws ZkClientException {
        this.checkStatus();
        try {
            this.zk.delete(path, -1);
        } catch (Exception e) {
            throw new ZkClientException("delete node " + path, e);
        }
    }

    /**
     * 关闭客户端
     *
     * @throws ZkClientException
     */
    public void close() throws ZkClientException {
        try {
            if (zk != null && zk.getState().isConnected()) {
                zk.close();
            }
        } catch (InterruptedException e) {
            throw new ZkClientException("close zookeeper client error.", e);
        }
    }

    /**
     * 判断节点是否存在
     *
     * @param path
     * @return
     * @throws ZkClientException
     */
    public boolean exists(String path) throws ZkClientException {
        this.checkStatus();
        try {
            return this.zk.exists(path, false) != null;
        } catch (Exception e) {
            throw new ZkClientException("exists node " + path, e);
        }
    }

    /**
     * 检查zookeeper是否处于连接状态
     *
     * @return
     */
    public boolean checkConnection() {
        boolean conn = false;
        if (zk != null) {
            conn = zk.getState().isConnected();
        }
        return conn && this.isConnection();
    }

    /**
     * 检查zookeeper连接状态
     *
     * @return
     * @throws ZkClientException
     */
    public boolean checkStatus() throws ZkClientException {
        if (zk == null) {
            throw new ZkClientException("Not connected to the zookeeper server,host=" + hosts + ",invoking this.connect().");
        }
        if (zk.getState().isAlive() && this.isConnection()) {
            return true;
        }
        throw new ZkClientException("Not connected to the zookeeper server,host=" + hosts + ",state: " + zk.getState());
    }
    
	public void testWatch()throws Exception{
		//String conn = "10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181";
		/*ZooKeeper zk = new ZooKeeper(conn,5000,new Watcher() {
			public void process(WatchedEvent arg0) {
			}
		});*/
		
		//创建观察者对象
		Watcher watch = new Watcher(){
			public void process(WatchedEvent event) {
				System.out.println("有事件发生了：" + event);
				if(event.getType().equals(EventType.NodeDataChanged)){
					try {
						System.out.println("节点数据变动了:" + StringUtils.toString(zk.getData("/zkUtil", null, null),"utf-8"));
					} catch (Exception e) {
						e.printStackTrace();
					} 
				}
			}
		};
		//ZooKeeper zk = new ZooKeeper(conn,5000,watch);
		
		byte[] data = zk.getData("/zkUtil", watch, new Stat());
		System.out.println(new String(data));
		while(true){
			//5秒监听一次
			Thread.sleep(5000);
		}
	}
}
