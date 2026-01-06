const redis = require('../config/redis');

async function getWatchHistory(request, reply) {
  const { user_id } = request.params;
  const limit = Number(request.query.limit || 50);

  const cacheKeyOrder = `wh:order:${user_id}`;
  const cacheKeyProgress = `wh:progress:${user_id}`;

  try {
    const contentIds = await redis.zrevrange(
      cacheKeyOrder,
      0,
      limit - 1
    );

    if (contentIds.length === 0) {
      return [];
    }

    const playbacks = await redis.hmget(
      cacheKeyProgress,
      ...contentIds
    );

    const response = contentIds.map((content_id, index) => ({
      content_id,
      playback_time: playbacks[index]
    }));

    return response;
  } catch (err) {
    request.log.error(err);
    reply.code(500).send({ error: 'Failed to fetch watch history' });
  }
}

module.exports = {
  getWatchHistory,
};
