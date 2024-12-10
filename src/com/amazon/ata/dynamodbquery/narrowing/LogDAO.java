package com.amazon.ata.dynamodbquery.narrowing;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.HashMap;
import java.util.List;

import static javax.swing.UIManager.put;

public class LogDAO {

    private DynamoDBMapper mapper;

    /**
     * Allows access to and manipulation of Log objects from the data store.
     * @param mapper Access to DynamoDB
     */
    public LogDAO(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Uses the query() method to retrieve all the items from the LogEntries table that have a given
     * partition key value
     *

     *      * this method will return the list of all the items from the LogEntries table that have a given
     *      * partition key value
     * and the sort key value is between two given times.
     * @param logLevel the given partition key
     * @param startTime the given start time
     * @param endTime the given end time
     * @return the PaginatedQueryList that is returned from the query
     */
    public List<Log> getLogsBetweenTimes(String logLevel, String startTime, String endTime) {
        // TODO: implement
        /**
         * Uses the query() method to retrieve all the items from the LogEntries table that have a given
         * partition key value
         *
         *      * this method will return the list of all the items from the LogEntries table that have a given
         *      * partition key value
         * and the sort key value is between two given times.
         * @param logLevel the given partition key
         * @param startTime the given start time
         * @param endTime the given end time
         * @return the PaginatedQueryList that is returned from the query
         */


        // TODO: implement

        // Example implementation:
        // Create a DynamoDB query expression
        DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>()
                // Set the partition key
                .withKeyConditionExpression("logLevel = :logLevel");
        queryExpression.withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
            put(":logLevel", new AttributeValue(logLevel));
        }});
        // Set the sort key range
        queryExpression.withFilterExpression("startTime > :startTime AND endTime < :endTime")
                .withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
                    put(":startTime", new AttributeValue(startTime));
                    put(":endTime", new AttributeValue(endTime));
                }});

        return mapper.query(Log.class, queryExpression);

    }

        /*DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>()
                // Set the partition key
               .withKeyConditionExpression("logLevel = :logLevel");
        queryExpression.withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
            put(":logLevel", new AttributeValue(logLevel));
        }});
        // Set the sort key range
        queryExpression.withFilterExpression("endTime < :endTime")
               .withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
                    put(":endTime", new AttributeValue(endTime));
                }});

        return mapper.query(Log.class, queryExpression);
        */

        /*DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>()
                // Set the partition key
                .withKeyConditionExpression("logLevel = :logLevel");
                //Set the sort key range

        queryExpression.withFilterExpression("endTime");
        queryExpression.getExpressionAttributeValues(new HashMap<String, AttributeValue>() {{

            put(":logLevel", new AttributeValue(logLevel));

            put(":endTime", new AttributeValue(endTime));

            put(":startTime", new AttributeValue(startTime));

            put(":exclusiveStartAsin", new AttributeValue("")); // Start with the first item

            // Pagination
            queryExpression.withExclusiveStartKey(new HashMap<String, AttributeValue>() {{
                put("logLevel", new AttributeValue(logLevel));
                put("endTime", new AttributeValue(endTime));
            }});

            queryExpression.withScanIndexForward(false); // Retrieve in descending order (from end to start)

            return mapper.query(Log.class, queryExpression,
                                                     new DynamoDBMapper.QueryAsyncHandler<Log>() {

                                                         @Override
                                                         public List<Log> onFailure(Exception e,
                                                                                   DynamoDBQueryExpression<Log> queryExpression,
                                                                                   int remainingRetryAttempts) {
                                                             throw new RuntimeException(e);
                                                         }

                                                         @Override
                                                         public List<Log> onSuccess(DynamoDBQueryExpression<Log> queryExpression,
                                                                                   List<Log> logs) {
                                                             return logs;
                                                         }

                                                     }

                                                     }

                                                     }
        )
    }*/




    /**
     * Uses the query() method to retrieve all the items from the LogEntries table that have a given partition key value
     * and the sort key value that is before a given time.
     * @param logLevel the given partition key
     * @param endTime the given end time
     * @return the PaginatedQueryList that is returned from the query
     */
    public List<Log> getLogsBeforeTime(String logLevel, String endTime) {
        //TODO: implement
        

        // Example implementation:

            DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>()
                    // Set the partition key
               .withKeyConditionExpression("logLevel = :logLevel");
            queryExpression.withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
                put(":logLevel", new AttributeValue(logLevel));
            }});
            // Set the sort key range
            queryExpression.withFilterExpression("endTime < :endTime")
               .withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
                    put(":endTime", new AttributeValue(endTime));
                }});

            return mapper.query(Log.class, queryExpression);
    }

            /*DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>()
                    // Set the partition key
                   .withKeyConditionExpression("logLevel = :logLevel");
                    // Set the sort key range
                    queryExpression.withFilterExpression("endTime");
                    queryExpression.getExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
                    put(":logLevel", new AttributeValue(logLevel));
                    put(":endTime", new AttributeValue(endTime));
                    put(":exclusiveStartAsin", new AttributeValue("")); // Start with the first item
                    // Pagination
                    queryExpression.withExclusiveStartKey(new HashMap<String, AttributeValue>() {{
                        put("logLevel", new AttributeValue(logLevel));
                        put("endTime", new AttributeValue(endTime));
                    }});

                    queryExpression.withScanIndexForward(false); // Retrieve in descending order (from end to start)

                    return mapper.query(Log.class, queryExpression,
                                                     new DynamoDBMapper.QueryAsyncHandler<Log>() {

                                                         @Override
                                                         public List<Log> onFailure(Exception e,
                                                                                   DynamoDBQueryExpression<Log> queryExpression,
                                                                                   int remainingRetryAttempts) {
                                                             throw new RuntimeException(e);
                                                         }

                                                         @Override
                                                         public List<Log> onSuccess(DynamoDBQueryExpression<Log> queryExpression,
                                                                                   List<Log> logs) {
                                                             return logs;
                                                         }

                                                      }

                                                     );
            */






    /**
     * Uses the query() method to retrieve all the items from the LogEntries table that have
     * a given partition key value
     * and the sort key value that is after a given time.
     * @param logLevel the given partition key
     * @param startTime the given start time
     * @return the PaginatedQueryList that is returned from the query
     */
    public List getLogsAfterTime(String logLevel, String startTime) {
        //TODO: implement

        // Example implementation:
        DynamoDBQueryExpression<Log> queryExpression = new DynamoDBQueryExpression<Log>();
        //Set the partition key
        queryExpression.withKeyConditionExpression("logLevel = :logLevel");
        queryExpression.withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
            put(":logLevel", new AttributeValue(logLevel));

            put(":startTime", new AttributeValue(startTime));

        }});
        //set the sort key range
        queryExpression.withFilterExpression("startTime > :startTime");
        queryExpression.withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
            put(":startTime", new AttributeValue(startTime));

            put(":endTime", new AttributeValue("")); // Start with the first item
            // Pagination * @return the PaginatedQueryList that is returned from the query
            queryExpression.withExclusiveStartKey(new HashMap<String, AttributeValue>() {{
                put("logLevel", new AttributeValue(logLevel));
                put("startTime", new AttributeValue(startTime));
            }});
        }});
        return mapper.query(Log.class, queryExpression);
        
    }
}
