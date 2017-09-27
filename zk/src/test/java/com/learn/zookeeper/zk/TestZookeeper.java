package com.learn.zookeeper.zk;

import java.util.List;

import junit.framework.TestCase;

/** 
 * 功能描述:
 * @author : 
 * @date 创建时间：2017年3月28日 下午3:19:56 
 * @version 1.0  
 */
public class TestZookeeper extends TestCase{
	
	/**
	 * 创建znode
	 * 功能描述:
	 * @author wwj
	 * @date 2017年3月28日 下午3:40:19
	 * @parameter 参数
	 * @return 返回值
	 * @throws 异常
	 */
	public void createPersistent() throws Exception{
		ZkClient ZkClient = new ZkClient("10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181");
		ZkClient.createPersistent("/zkUtil", "zkUtil".getBytes());
	}
	
	public void testSetData() throws Exception{
		ZkClient ZkClient = new ZkClient("10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181");
		ZkClient.setData("/zkUtil", "zkUtil2".getBytes());
	}
	
	
	public void testWatch() throws Exception{
		ZkClient ZkClient = new ZkClient("10.17.1.234:2181,10.17.1.235:2181,10.17.1.236:2181");
		ZkClient.testWatch();
	}
}
