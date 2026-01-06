const { masterPool } = require('../config/db');
const redis = require('../config/redis');
const { SCHEMA } = require('../lib/db-ops');

const watchHistorySchema = {
  body: {
    type: 'object',
    required: ['listitem'],
    properties: {
      auth_token: { type: 'string' },
      region: { type: 'string' },
      listitem: {
        type: 'object',
        required: ['content_id', 'catalog_id', 'play_back_time'],
        properties: {
          content_id: { type: 'string' },
          catalog_id: { type: 'string' },
          play_back_time: { type: 'string' }
        }
      }
    }
  }
};

async function updateWatchHistory(request, reply) {
  const { session_id } = request.params;
  const { listitem } = request.body;
  const { content_id, catalog_id, play_back_time } = listitem;

  try {
    const sessionRes = await masterPool.query(
      `SELECT user_id FROM ${SCHEMA}.sessions WHERE session_id = $1 LIMIT 1`,
      [session_id]
    );

    if (sessionRes.rows.length === 0) {
      reply.code(401).send({ error: 'Invalid Session' });
      return;
    }

    const user_id = sessionRes.rows[0].user_id;
    const cacheKeyOrder = `wh:order:${user_id}`;
    const cacheKeyProgress = `wh:progress:${user_id}`;
    const timestamp = Math.floor(Date.now() / 1000);

    const pipeline = redis.pipeline();

    //  Update Redis cache (HOT PATH)
    pipeline.zadd(
      cacheKeyOrder,
      timestamp,
      content_id
    );

    pipeline.hset(
      cacheKeyProgress,
      content_id,
      play_back_time
    );
    
    pipeline.xadd('stream:watch-history', '*', 
      'user_id', user_id, 
      'content_id', content_id, 
      'catalog_id', catalog_id,
      'play_back_time', play_back_time,
      'timestamp', Math.floor(Date.now() / 1000)
    );

    await pipeline.exec();

    return { status: 'ok', message: 'Watch history updated' };
  } catch (err) {
    request.log.error(err);
    reply.code(500).send({ error: 'Internal Server Error' });
  }
}

module.exports = {
  updateWatchHistory,
  watchHistorySchema,
};
