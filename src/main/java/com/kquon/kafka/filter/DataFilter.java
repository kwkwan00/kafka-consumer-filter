package com.kquon.kafka.filter;

/**
 * Created by kquon on 6/27/15.
 */
public interface DataFilter {

    void execute(byte[] message);

}
