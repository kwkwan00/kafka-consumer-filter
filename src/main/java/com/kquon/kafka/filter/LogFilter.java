package com.kquon.kafka.filter;

/**
 * Created by kquon on 6/27/15.
 */
public class LogFilter implements DataFilter {

    public void execute(byte[] message) {
        if (message != null) {
            String messageString = message.toString();

            // Filtering Logic Here
            if (messageString.contains("DEBUG")) {
                // DEBUG log handling
            } else if (messageString.contains("INFO")) {
                // INFO log handling
            } else if (messageString.contains("WARN")) {
                // WARN log handling
            } else if (messageString.contains("ERROR")) {
                // ERROR log handling
            } else if (messageString.contains("FATAL")) {
                // FATAL log handling
            } else {
                // Other log handling
            }
            
        }
    }

}