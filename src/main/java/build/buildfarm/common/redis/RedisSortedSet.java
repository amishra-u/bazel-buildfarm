package build.buildfarm.common.redis;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterPipeline;
import redis.clients.jedis.Response;

/**
 * A redis sorted set is an implementation of a concurrent skip list set data structure which
 * internally redis to store and distribute the data. It's important to know that the lifetime of
 * the set persists before and after this data structure is created (since it exists in redis).
 * Therefore, two redis sets with the same name, would in fact be the same underlying redis sets.
 */
public class RedisSortedSet {

  /**
   * The name is used by the redis cluster client to access the set data. If two set had the same
   * name, they would be instances of the same underlying redis set.
   */
  private final String name;

  /**
   * Construct a named redis set with an established redis cluster.
   *
   * @param name The global name of the set.
   */
  public RedisSortedSet(String name) {
    this.name = name;
  }

  /**
   * Increments scores for members in a sorted set using a JedisCluster client.
   *
   * <p>This method increments scores for the specified members in the sorted set. If a member does
   * not exist, it is added with the given score. The operation is atomic, ensuring consistency in
   * the sorted set.
   *
   * @param jedis JedisCluster client to interact with Redis cluster.
   * @param memberAndScore A map where keys are member names and values are the increment scores.
   * @return A map containing updated scores for each member after the increment operation.
   */
  public Map<String, Integer> incrementMembersScore(
      JedisCluster jedis, Stream<Map.Entry<String, Integer>> memberAndScore) {
    JedisClusterPipeline pipeline = jedis.pipelined();
    Stream<AbstractMap.SimpleEntry<String, Response<Double>>> updatedScoreResponse =
        memberAndScore.map(
            entry ->
                new AbstractMap.SimpleEntry<>(
                    entry.getKey(), pipeline.zincrby(this.name, entry.getValue(), entry.getKey())));
    pipeline.sync();
    // keep the last score for a key.
    return updatedScoreResponse.collect(
        Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get().intValue()));
  }

  /**
   * Removes the specified members from the sorted set and returns the count of removed members.
   * Removal is performed in batches for improved performance.
   *
   * @param jedis JedisCluster client to interact with Redis cluster.
   * @param members A stream of members to be removed from the set.
   * @return total count of members removed.
   */
  public int removeMembers(JedisCluster jedis, Stream<String> members) {
    Iterator<String> iterator = members.iterator();
    int batchSize = 128;
    int membersRemoved = 0;
    while (true) {
      List<String> batch = new ArrayList<>(batchSize);
      for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
        batch.add(iterator.next());
      }
      if (batch.isEmpty()) break;
      membersRemoved += jedis.zrem(this.name, batch.toArray(new String[0]));
    }
    return membersRemoved;
  }
}
