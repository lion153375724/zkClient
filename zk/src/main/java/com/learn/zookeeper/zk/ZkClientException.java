package com.learn.zookeeper.zk;


/**
 * 
 * @author Administrator
 *
 */
public class ZkClientException extends RuntimeException {
    public ZkClientException(String msg) {
        super(msg);
    }

    public ZkClientException(String message, Exception e) {
        super(message, e);
    }

    public ZkClientException(Exception e) {
        super(e);
    }
}
