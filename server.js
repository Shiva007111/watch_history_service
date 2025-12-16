require('dotenv').config();
const fastify = require('fastify')({ logger: true });
const redis = require('./redis-client');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://localhost:5432/saranyu_ott_development',
  max: 10, 
  idleTimeoutMillis: 30000,
});

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

fastify.post('/users/:session_id/playlists/watchhistory', { schema: watchHistorySchema }, async (request, reply) => {
  const { session_id } = request.params;
  const { listitem } = request.body;
  const { content_id, catalog_id, play_back_time } = listitem;

  try {
    const sessionRes = await pool.query(
      `SELECT user_id FROM dangal_dangal_schema.sessions WHERE session_id = $1 LIMIT 1`,
      [session_id]
    );

    if (sessionRes.rows.length === 0) {
      reply.code(401).send({ error: 'Invalid Session' });
      return;
    }

    const user_id = sessionRes.rows[0].user_id;

    const pipeline = redis.pipeline();
    // add to redis stream like event
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
});

// Health check
fastify.get('/health', async () => {
  return { status: 'ok', uptime: process.uptime() };
});

const start = async () => {
  try {
    await fastify.listen({ port: 3000, host: '0.0.0.0' });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
